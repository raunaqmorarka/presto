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
package io.trino.plugin.iceberg;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import com.google.inject.Key;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.filesystem.TrackingFileSystemFactory;
import io.trino.filesystem.TrackingFileSystemFactory.OperationType;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.alluxio.AlluxioFileSystemCacheConfig;
import io.trino.filesystem.alluxio.AlluxioFileSystemCacheModule;
import io.trino.filesystem.alluxio.TestingAlluxioFileSystemCache;
import io.trino.filesystem.cache.CacheFileSystemFactory;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.filesystem.cache.NoneCachingHostAddressProvider;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.iceberg.cache.IcebergCacheKeyProvider;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_GET_LENGTH;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_NEW_STREAM;
import static io.trino.filesystem.alluxio.TestingAlluxioFileSystemCache.OperationType.CACHE_READ;
import static io.trino.filesystem.alluxio.TestingAlluxioFileSystemCache.OperationType.EXTERNAL_READ;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.TestIcebergAlluxioCacheFileOperations.FileType.DATA;
import static io.trino.plugin.iceberg.TestIcebergAlluxioCacheFileOperations.FileType.MANIFEST;
import static io.trino.plugin.iceberg.TestIcebergAlluxioCacheFileOperations.FileType.METADATA_JSON;
import static io.trino.plugin.iceberg.TestIcebergAlluxioCacheFileOperations.FileType.METASTORE;
import static io.trino.plugin.iceberg.TestIcebergAlluxioCacheFileOperations.FileType.SNAPSHOT;
import static io.trino.plugin.iceberg.TestIcebergAlluxioCacheFileOperations.FileType.fromFilePath;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

// single-threaded AccessTrackingFileSystemFactory is shared mutable state
@Execution(ExecutionMode.SAME_THREAD)
public class TestIcebergAlluxioCacheFileOperations
        extends AbstractTestQueryFramework
{
    private TrackingFileSystemFactory trackingFileSystemFactory;
    private TestingAlluxioFileSystemCache alluxioFileSystemCache;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("iceberg")
                .setSchema("test_schema")
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .build();
        try {
            File metastoreDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").toFile().getAbsoluteFile();
            Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
            dataDirectory.toFile().mkdirs();
            trackingFileSystemFactory = new TrackingFileSystemFactory(new LocalFileSystemFactory(dataDirectory));
            AlluxioFileSystemCacheConfig alluxioFileSystemCacheConfiguration = new AlluxioFileSystemCacheConfig()
                    .setCacheDirectories(metastoreDirectory.getAbsolutePath() + "/cache")
                    .setMaxCacheSizes("100MB")
                    .setCachePageSize(DataSize.of(5, DataSize.Unit.MEGABYTE));
            alluxioFileSystemCache = new TestingAlluxioFileSystemCache(AlluxioFileSystemCacheModule.getAlluxioConfiguration(alluxioFileSystemCacheConfiguration), new IcebergCacheKeyProvider());
            TrinoFileSystemFactory fileSystemFactory = new CacheFileSystemFactory(trackingFileSystemFactory, alluxioFileSystemCache, alluxioFileSystemCache.getCacheKeyProvider());
            queryRunner.installPlugin(new TestingIcebergPlugin(
                    dataDirectory,
                    Optional.empty(),
                    Optional.of(fileSystemFactory),
                    binder -> {
                        newOptionalBinder(binder, Key.get(boolean.class, AsyncIcebergSplitProducer.class))
                                .setBinding().toInstance(false);
                        binder.bind(CachingHostAddressProvider.class).to(NoneCachingHostAddressProvider.class).in(SINGLETON);
                    }));
            queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg");

            queryRunner.execute("CREATE SCHEMA " + session.getSchema().orElseThrow());
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    public void testCacheFileOperations()
    {
        alluxioFileSystemCache.clear();
        assertUpdate("DROP TABLE IF EXISTS test_cache_file_operations");
        assertUpdate("CREATE TABLE test_cache_file_operations(key varchar, data varchar) with (partitioning=ARRAY['key'])");
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p1', '1-abc')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p2', '2-xyz')", 1);
        alluxioFileSystemCache.clear();
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 2)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(DATA, INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(METASTORE, INPUT_FILE_NEW_STREAM), 1)
                        .build(),
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation(EXTERNAL_READ), 9)
                        .addCopies(new CacheOperation(CACHE_READ), 3)
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 2)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(METASTORE, INPUT_FILE_NEW_STREAM), 1)
                        .build(),
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation(EXTERNAL_READ), 4)
                        .addCopies(new CacheOperation(CACHE_READ), 6)
                        .build());
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p3', '3-xyz')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p4', '4-xyz')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p5', '5-xyz')", 1);
        alluxioFileSystemCache.clear();
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 2)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 5)
                        .addCopies(new FileOperation(DATA, INPUT_FILE_NEW_STREAM), 5)
                        .addCopies(new FileOperation(METASTORE, INPUT_FILE_NEW_STREAM), 1)
                        .build(),
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation(EXTERNAL_READ), 19)
                        .addCopies(new CacheOperation(CACHE_READ), 7)
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 2)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 5)
                        .addCopies(new FileOperation(METASTORE, INPUT_FILE_NEW_STREAM), 1)
                        .build(),
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation(EXTERNAL_READ), 7)
                        .addCopies(new CacheOperation(CACHE_READ), 12)
                        .build());
    }

    private record CacheOperation(TestingAlluxioFileSystemCache.OperationType type)
    {
        public static CacheOperation create(TestingAlluxioFileSystemCache.OperationType operationType)
        {
            return new CacheOperation(operationType);
        }
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, TestIcebergAlluxioCacheFileOperations.Scope scope, Multiset<TestIcebergAlluxioCacheFileOperations.FileOperation> expectedAccesses)
    {
        assertFileSystemAccesses(getSession(), query, scope, expectedAccesses);
    }

    private void assertFileSystemAccesses(Session session, @Language("SQL") String query, TestIcebergAlluxioCacheFileOperations.Scope scope, Multiset<TestIcebergAlluxioCacheFileOperations.FileOperation> expectedAccesses)
    {
        resetCounts();
        getDistributedQueryRunner().execute(session, query);
        assertMultisetsEqual(
                getOperations().stream()
                        .filter(scope)
                        .collect(toImmutableMultiset()),
                expectedAccesses);
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<FileOperation> expectedAccesses, Multiset<CacheOperation> expectedCacheAccesses)
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        trackingFileSystemFactory.reset();
        alluxioFileSystemCache.reset();
        queryRunner.executeWithPlan(queryRunner.getDefaultSession(), query);
        assertMultisetsEqual(getOperations(), expectedAccesses);
        assertMultisetsEqual(getCacheOperations(), expectedCacheAccesses);
    }

    private Multiset<CacheOperation> getCacheOperations()
    {
        return alluxioFileSystemCache.getOperationCounts()
                .entrySet().stream()
                .filter(entry -> {
                    String path = entry.getKey().location().path();
                    return !path.endsWith(".trinoSchema") && !path.contains(".trinoPermissions");
                })
                .flatMap(entry -> nCopies((int) entry.getValue().stream().filter(l -> l > 0).count(), CacheOperation.create(
                        entry.getKey().type())).stream())
                .collect(toCollection(HashMultiset::create));
    }

    private void resetCounts()
    {
        trackingFileSystemFactory.reset();
    }

    private Multiset<TestIcebergAlluxioCacheFileOperations.FileOperation> getOperations()
    {
        return trackingFileSystemFactory.getOperationCounts()
                .entrySet().stream()
                .flatMap(entry -> nCopies(entry.getValue(), new TestIcebergAlluxioCacheFileOperations.FileOperation(
                        fromFilePath(entry.getKey().location().toString()),
                        entry.getKey().operationType())).stream())
                .collect(toCollection(HashMultiset::create));
    }

    private record FileOperation(TestIcebergAlluxioCacheFileOperations.FileType fileType, OperationType operationType)
    {
        public FileOperation
        {
            requireNonNull(fileType, "fileType is null");
            requireNonNull(operationType, "operationType is null");
        }
    }

    enum Scope
            implements Predicate<TestIcebergAlluxioCacheFileOperations.FileOperation>
    {
        METADATA_FILES {
            @Override
            public boolean test(TestIcebergAlluxioCacheFileOperations.FileOperation fileOperation)
            {
                return fileOperation.fileType() != TestIcebergAlluxioCacheFileOperations.FileType.DATA;
            }
        },
        ALL_FILES {
            @Override
            public boolean test(TestIcebergAlluxioCacheFileOperations.FileOperation fileOperation)
            {
                return true;
            }
        },
    }

    enum FileType
    {
        METADATA_JSON,
        SNAPSHOT,
        MANIFEST,
        STATS,
        DATA,
        METASTORE
        /**/;

        public static TestIcebergAlluxioCacheFileOperations.FileType fromFilePath(String path)
        {
            if (path.endsWith("metadata.json")) {
                return METADATA_JSON;
            }
            if (path.contains("/snap-")) {
                return SNAPSHOT;
            }
            if (path.endsWith("-m0.avro")) {
                return MANIFEST;
            }
            if (path.endsWith(".stats")) {
                return STATS;
            }
            if (path.contains("/data/") && (path.endsWith(".orc") || path.endsWith(".parquet"))) {
                return DATA;
            }
            if (path.endsWith(".trinoSchema") || path.contains("/.trinoPermissions/")) {
                return METASTORE;
            }
            throw new IllegalArgumentException("File not recognized: " + path);
        }
    }
}
