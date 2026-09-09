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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.hive.metastore.glue.SchemaMappingDelegates;
import io.trino.plugin.hive.metastore.glue.SchemaMappingDelegates.SchemaMappingRule;
import io.trino.plugin.hive.security.UsingSystemSecurity;
import io.trino.plugin.iceberg.ForIcebergMetadata;
import io.trino.plugin.iceberg.ForIcebergSplitManager;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.encryption.EncryptionManagerFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.NodeVersion;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class TrinoGlueCatalogFactory
        implements TrinoCatalogFactory
{
    private final CatalogName catalogName;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ForwardingFileIoFactory fileIoFactory;
    private final TypeManager typeManager;
    private final boolean cacheTableMetadata;
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final String trinoVersion;
    private final Optional<String> defaultSchemaLocation;
    private final StatsRecordingGlueClient glueClient;
    private final boolean isUniqueTableLocation;
    private final boolean hideMaterializedViewStorageTable;
    private final GlueMetastoreStats stats;
    private final boolean isUsingSystemSecurity;
    private final Executor metadataFetchingExecutor;
    private final ExecutorService icebergScanExecutor;
    private final List<SchemaMappingDelegate> schemaMappingDelegates;

    // A rule without catalog id reuses the default catalog.
    private record SchemaMappingDelegate(String prefix, Optional<StatsRecordingGlueClient> glueClient, Optional<IcebergTableOperationsProvider> tableOperationsProvider) {}

    @Inject
    public TrinoGlueCatalogFactory(
            CatalogName catalogName,
            TrinoFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            NodeVersion nodeVersion,
            GlueHiveMetastoreConfig glueConfig,
            IcebergConfig icebergConfig,
            IcebergGlueCatalogConfig catalogConfig,
            @UsingSystemSecurity boolean usingSystemSecurity,
            GlueMetastoreStats stats,
            GlueClient glueClient,
            @ForIcebergMetadata ExecutorService metadataExecutorService,
            @ForIcebergSplitManager ExecutorService icebergScanExecutor,
            EncryptionManagerFactory encryptionManagerFactory)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.cacheTableMetadata = catalogConfig.isCacheTableMetadata();
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.trinoVersion = nodeVersion.toString();
        this.defaultSchemaLocation = glueConfig.getDefaultWarehouseDir();
        this.glueClient = new StatsRecordingGlueClient(glueClient, stats);
        this.isUniqueTableLocation = icebergConfig.isUniqueTableLocation();
        this.hideMaterializedViewStorageTable = icebergConfig.isHideMaterializedViewStorageTable();
        this.stats = requireNonNull(stats, "stats is null");
        this.isUsingSystemSecurity = usingSystemSecurity;
        if (icebergConfig.getMetadataParallelism() == 1) {
            this.metadataFetchingExecutor = directExecutor();
        }
        else {
            this.metadataFetchingExecutor = new BoundedExecutor(metadataExecutorService, icebergConfig.getMetadataParallelism());
        }
        this.icebergScanExecutor = requireNonNull(icebergScanExecutor, "icebergScanExecutor is null");
        this.schemaMappingDelegates = glueConfig.getSchemaMappingRules()
                .map(SchemaMappingDelegates::parseRules)
                .orElse(ImmutableList.of())
                .stream()
                .map(rule -> createSchemaMappingDelegate(rule, glueConfig, catalogConfig, encryptionManagerFactory))
                .collect(toImmutableList());
    }

    private SchemaMappingDelegate createSchemaMappingDelegate(
            SchemaMappingRule rule,
            GlueHiveMetastoreConfig glueConfig,
            IcebergGlueCatalogConfig catalogConfig,
            EncryptionManagerFactory encryptionManagerFactory)
    {
        if (rule.catalogId().isEmpty()) {
            return new SchemaMappingDelegate(rule.prefix(), Optional.empty(), Optional.empty());
        }
        GlueClient ruleGlueClient = SchemaMappingDelegates.createGlueClient(glueConfig, rule.catalogId());
        GlueMetastoreStats ruleStats = new GlueMetastoreStats();
        return new SchemaMappingDelegate(
                rule.prefix(),
                Optional.of(new StatsRecordingGlueClient(ruleGlueClient, ruleStats)),
                Optional.of(new GlueIcebergTableOperationsProvider(
                        fileSystemFactory,
                        fileIoFactory,
                        typeManager,
                        catalogConfig,
                        ruleStats,
                        ruleGlueClient,
                        encryptionManagerFactory)));
    }

    @Managed
    @Flatten
    public GlueMetastoreStats getStats()
    {
        return stats;
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        TrinoGlueCatalog defaultCatalog = createCatalog(glueClient, tableOperationsProvider);
        if (schemaMappingDelegates.isEmpty()) {
            return defaultCatalog;
        }
        Map<String, TrinoCatalog> delegatesByPrefix = schemaMappingDelegates.stream()
                .collect(toImmutableMap(
                        SchemaMappingDelegate::prefix,
                        delegate -> {
                            if (delegate.glueClient().isEmpty()) {
                                return defaultCatalog;
                            }
                            return createCatalog(delegate.glueClient().get(), delegate.tableOperationsProvider().orElseThrow());
                        }));
        return new SchemaMappingTrinoCatalog(defaultCatalog, delegatesByPrefix);
    }

    private TrinoGlueCatalog createCatalog(StatsRecordingGlueClient glueClient, IcebergTableOperationsProvider tableOperationsProvider)
    {
        return new TrinoGlueCatalog(
                catalogName,
                fileSystemFactory,
                fileIoFactory,
                typeManager,
                cacheTableMetadata,
                tableOperationsProvider,
                trinoVersion,
                glueClient,
                isUsingSystemSecurity,
                defaultSchemaLocation,
                isUniqueTableLocation,
                hideMaterializedViewStorageTable,
                metadataFetchingExecutor,
                icebergScanExecutor);
    }
}
