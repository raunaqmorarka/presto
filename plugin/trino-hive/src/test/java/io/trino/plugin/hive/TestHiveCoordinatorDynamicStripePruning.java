/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.operator.OperatorStats;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.airlift.testing.Assertions.assertBetweenInclusive;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static io.trino.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static io.trino.tpch.TpchTable.getTables;

public class TestHiveCoordinatorDynamicStripePruning
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestHiveCoordinatorDynamicStripePruning.class);
    private static final String ORC_LINEITEM = "orc_lineitem";
    private static final long LINEITEM_COUNT = 6001215;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setInitialTables(getTables())
                .setHiveProperties(ImmutableMap.of("hive.dynamic-filtering-probe-blocking-timeout", "1h"))
                .build();
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        super.init();
        // setup big fact table because some initial splits may get scheduled quickly without waiting for DF to be sent from coordinator
        @Language("SQL") String sql = String.format("CREATE TABLE %s WITH (format='orc') AS " +
                "SELECT orderkey, partkey, suppkey FROM %s", ORC_LINEITEM, "tpch.sf1.lineitem");
        long start = System.nanoTime();
        long rows = (Long) getQueryRunner().execute(sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows for %s in %s", rows, ORC_LINEITEM, nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name()) // Avoid node local DF
                .build();
    }

    @Test(timeOut = 30_000)
    public void testJoinWithSelectiveBuildSide()
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(
                getSession(),
                "SELECT * FROM orc_lineitem JOIN supplier ON orc_lineitem.suppkey = supplier.suppkey " +
                        "AND supplier.name = 'Supplier#000000001'");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), "tpch:" + ORC_LINEITEM);
        // Probe-side is partially scanned
        assertBetweenInclusive(probeStats.getInputPositions(), 625L, LINEITEM_COUNT - 1);
        assertBetweenInclusive(probeStats.getDynamicFilterSplitsProcessed(), 1L, probeStats.getTotalDrivers());
    }

    @Test(timeOut = 30_000)
    public void testJoinWithMultipleDynamicFiltersOnProbe()
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        // supplier names Supplier#000000001 and Supplier#000000002 match suppkey 1 and 2
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(
                getSession(),
                "SELECT * FROM (" +
                        "SELECT supplier.suppkey FROM " +
                        "orc_lineitem JOIN tpch.tiny.supplier ON orc_lineitem.suppkey = supplier.suppkey AND supplier.name IN ('Supplier#000000001', 'Supplier#000000002')" +
                        ") t JOIN supplier ON t.suppkey = supplier.suppkey AND supplier.suppkey IN (2, 3)");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), "tpch:" + ORC_LINEITEM);
        // Probe-side is partially scanned
        assertBetweenInclusive(probeStats.getInputPositions(), 557L, LINEITEM_COUNT - 1);
        assertBetweenInclusive(probeStats.getDynamicFilterSplitsProcessed(), 1L, probeStats.getTotalDrivers());
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinWithSelectiveBuildSide()
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(
                getSession(),
                "SELECT * FROM orc_lineitem WHERE suppkey IN (SELECT suppkey FROM supplier WHERE name = 'Supplier#000000001')");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), "tpch:" + ORC_LINEITEM);
        // Probe-side is partially scanned
        assertBetweenInclusive(probeStats.getInputPositions(), 625L, LINEITEM_COUNT - 1);
        assertBetweenInclusive(probeStats.getDynamicFilterSplitsProcessed(), 1L, probeStats.getTotalDrivers());
    }
}
