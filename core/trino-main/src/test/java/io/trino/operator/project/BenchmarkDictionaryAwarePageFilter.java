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
package io.trino.operator.project;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.metadata.MetadataManager;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.planner.CompilerConfig;
import io.trino.sql.relational.RowExpression;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 15, time = 1)
public class BenchmarkDictionaryAwarePageFilter
{
    private static final Random random = new Random(5376453765L);
    private static final int MAX_ROWS = 10_000_000;
    private static final Session SESSION = testSessionBuilder().build();
    private static final ConnectorSession CONNECTOR_SESSION = SESSION.toConnectorSession();
    private static final PageFunctionCompiler PAGE_FUNCTION_COMPILER = new PageFunctionCompiler(createTestingFunctionManager(), new CompilerConfig());
    private static final MetadataManager METADATA_MANAGER = createTestMetadataManager();

    private List<Page> inputPages;
    @Param({
            "INT_BLOCK_WITH_NULLS",
            "INT_BLOCK_WITHOUT_NULLS",
            "INT_BLOCK_WITH_NULL_RLE",
    })
    public DataSet inputDataSet;

    @Param({
            "CURRENT",
            "BOOLEAN_POSITIONS_MAY_HAVE_NULL",
            "BOOLEAN_POSITIONS_SEPARATE_NULLS_LOOP",
            "ARRAY_POSITIONS_MAY_HAVE_NULL",
            "ARRAY_POSITIONS_SEPARATE_NULLS_LOOP",
    })
    public PageFilterProvider pageFilterProvider;

    public enum DataSet
    {
        INT_BLOCK_WITH_NULL_RLE {
            private int numBlocksCreated;

            @Override
            Block createBlock(int positionsCount)
            {
                numBlocksCreated++;
                if (numBlocksCreated % 1001 == 0) {
                    return RunLengthEncodedBlock.create(INTEGER, null, positionsCount);
                }
                return createIntsBlock(positionsCount, false);
            }
        },
        INT_BLOCK_WITHOUT_NULLS {
            private int numBlocksCreated;

            @Override
            Block createBlock(int positionsCount)
            {
                numBlocksCreated++;
                if (numBlocksCreated % 1001 == 0) {
                    return RunLengthEncodedBlock.create(INTEGER, 64992484L, positionsCount);
                }
                return createIntsBlock(positionsCount, false);
            }
        },
        INT_BLOCK_WITH_NULLS {
            @Override
            Block createBlock(int positionsCount)
            {
                return createIntsBlock(positionsCount, true);
            }
        };

        abstract Block createBlock(int positionsCount);

        private static Block createIntsBlock(int positionsCount, boolean nullable)
        {
            BlockBuilder blockBuilder = INTEGER.createBlockBuilder(null, positionsCount);
            for (int i = 0; i < positionsCount; i++) {
                if (nullable && i % 101 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    INTEGER.writeLong(blockBuilder, random.nextInt(64992484 - 10, 64992484 + 10));
                }
            }
            return blockBuilder.build();
        }
    }

    public enum PageFilterProvider
    {
        CURRENT(() -> {
            RowExpression filter = call(
                    METADATA_MANAGER.resolveOperator(SESSION, LESS_THAN, ImmutableList.of(INTEGER, INTEGER)),
                    constant(64992484L, INTEGER),
                    field(0, INTEGER));
            return PAGE_FUNCTION_COMPILER.compileFilter(filter, Optional.empty()).get();
        }),
        BOOLEAN_POSITIONS_MAY_HAVE_NULL(BooleanSelectedPositionsV1PageFilter::new),
        BOOLEAN_POSITIONS_SEPARATE_NULLS_LOOP(BooleanSelectedPositionsV2PageFilter::new),
        ARRAY_POSITIONS_MAY_HAVE_NULL(ArraySelectedPositionsV1PageFilter::new),
        ARRAY_POSITIONS_SEPARATE_NULLS_LOOP(ArraySelectedPositionsV2PageFilter::new),
        ;

        private final PageFilter pageFilter;

        PageFilterProvider(Supplier<PageFilter> pageFilterSupplier)
        {
            this.pageFilter = pageFilterSupplier.get();
        }

        public PageFilter getPageFilter()
        {
            return pageFilter;
        }
    }

    @Setup
    public void setup()
    {
        setup(MAX_ROWS);
    }

    private void setup(int inputRows)
    {
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        while (inputRows > 0) {
            Block block = inputDataSet.createBlock(4096);
            pages.add(new Page(block.getPositionCount(), block));
            inputRows -= 4096;
        }
        inputPages = pages.build();
    }

    @Benchmark
    public double filterPages()
    {
        long rowsProcessed = 0;
        long rowsFiltered = 0;
        DictionaryAwarePageFilter filter = new DictionaryAwarePageFilter(pageFilterProvider.getPageFilter());
        for (int i = 0; i < inputPages.size(); i++) {
            Page page = inputPages.get(i);
            SelectedPositions selectedPositions = filter.filter(CONNECTOR_SESSION, page);
            int selectedPositionCount = selectedPositions.size();
            rowsProcessed += page.getPositionCount();
            rowsFiltered += page.getPositionCount() - selectedPositionCount;
        }
        return ((double) rowsFiltered / rowsProcessed) * 100;
    }

    public static void main(String[] args)
            throws Throwable
    {
        benchmark(BenchmarkDictionaryAwarePageFilter.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                // .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g", "-XX:+UnlockDiagnosticVMOptions", "-XX:+LogCompilation", "-XX:LogFile=~/Downloads/hotspot_current.log", "-XX:+PrintInlining"))
                .run();
    }
}
