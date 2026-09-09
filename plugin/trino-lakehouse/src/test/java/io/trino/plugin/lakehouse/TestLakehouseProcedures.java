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
package io.trino.plugin.lakehouse;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static java.nio.file.Files.createTempDirectory;

final class TestLakehouseProcedures
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = LakehouseQueryRunner.builder()
                .addLakehouseProperty("hive.metastore", "file")
                .addLakehouseProperty("hive.metastore.catalog.dir", createTempDirectory("lakehouse_procedures").toUri().toString())
                .addLakehouseProperty("hive.metastore-cache-ttl", "1m")
                .addLakehouseProperty("fs.hadoop.enabled", "true")
                .build();
        queryRunner.execute("CREATE SCHEMA lakehouse.tpch");
        return queryRunner;
    }

    @Test
    void testOptimizeIcebergTable()
    {
        try (TestTable table = newTrinoTable("test_optimize_iceberg", "(id integer)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize");
        }
    }

    @Test
    void testOptimizeHiveTable()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("lakehouse", "non_transactional_optimize_enabled", "true")
                .build();
        try (TestTable table = newTrinoTable("test_optimize_hive", "WITH (type = 'HIVE', format = 'PARQUET') AS SELECT 1 id")) {
            assertUpdate(session, "ALTER TABLE " + table.getName() + " EXECUTE optimize");
        }
    }

    @Test
    void testSyncPartitionMetadata()
    {
        try (TestTable table = newTrinoTable("test_sync_partitions", "WITH (type = 'HIVE', format = 'PARQUET', partitioned_by = ARRAY['part']) AS SELECT 1 id, 'a' part")) {
            assertUpdate("CALL system.sync_partition_metadata(schema_name => CURRENT_SCHEMA, table_name => '%s', mode => 'FULL')".formatted(table.getName()));
        }
    }

    @Test
    void testFlushMetadataCache()
    {
        assertUpdate("CALL system.flush_metadata_cache()");
    }
}
