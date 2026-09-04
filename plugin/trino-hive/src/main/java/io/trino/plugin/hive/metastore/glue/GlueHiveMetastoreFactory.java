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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.tracing.TracingHiveMetastore;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class GlueHiveMetastoreFactory
        implements HiveMetastoreFactory
{
    private final HiveMetastore metastore;
    private final boolean builtInCaching;

    // Glue metastore does not support impersonation, so just use single shared instance
    @Inject
    public GlueHiveMetastoreFactory(
            GlueHiveMetastore metastore,
            GlueHiveMetastoreConfig config,
            TrinoFileSystemFactory fileSystemFactory,
            CatalogName catalogName,
            Set<GlueHiveMetastore.TableKind> visibleTableKinds,
            Tracer tracer)
    {
        HiveMetastore delegate = metastore;
        if (config.getSchemaMappingRules().isPresent()) {
            Map<String, HiveMetastore> delegatesByPrefix = buildDelegates(
                    config.getSchemaMappingRules().get(),
                    metastore,
                    config,
                    fileSystemFactory,
                    catalogName,
                    visibleTableKinds);
            delegate = new SchemaMappingHiveMetastore(delegate, delegatesByPrefix);
        }
        this.metastore = new TracingHiveMetastore(tracer, delegate);
        // Schema mapping is cached by the generic metastore cache, keyed on the prefixed names
        this.builtInCaching = config.getSchemaMappingRules().isEmpty();
    }

    private static Map<String, HiveMetastore> buildDelegates(
            String rules,
            GlueHiveMetastore defaultMetastore,
            GlueHiveMetastoreConfig config,
            TrinoFileSystemFactory fileSystemFactory,
            CatalogName catalogName,
            Set<GlueHiveMetastore.TableKind> visibleTableKinds)
    {
        ImmutableMap.Builder<String, HiveMetastore> delegates = ImmutableMap.builder();
        for (String rule : Splitter.on(',').trimResults().omitEmptyStrings().split(rules)) {
            int separator = rule.indexOf(':');
            String prefix = separator == -1 ? rule : rule.substring(0, separator);
            Optional<String> catalogId = separator == -1 || separator == rule.length() - 1
                    ? Optional.empty()
                    : Optional.of(rule.substring(separator + 1));
            checkArgument(!prefix.isEmpty(), "Empty prefix in schema mapping rule: %s", rule);
            if (catalogId.isEmpty()) {
                // same account, reuse the default metastore
                delegates.put(prefix, defaultMetastore);
                continue;
            }
            GlueHiveMetastoreConfig catalogIdConfig = new GlueHiveMetastoreConfig().setCatalogId(catalogId.get());
            delegates.put(prefix, new GlueHiveMetastore(
                    GlueMetastoreModule.createGlueClient(
                            config,
                            ImmutableSet.of(
                                    new GlueHiveExecutionInterceptor(config),
                                    new GlueCatalogIdInterceptor(catalogIdConfig))),
                    GlueCache.NOOP,
                    new GlueMetastoreStats(),
                    fileSystemFactory,
                    config,
                    new CatalogName(catalogName + "-" + prefix),
                    visibleTableKinds));
        }
        return delegates.buildOrThrow();
    }

    @Override
    public boolean hasBuiltInCaching()
    {
        return builtInCaching;
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return false;
    }

    @Override
    public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        return metastore;
    }
}
