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
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.spi.catalog.CatalogName;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Builds per-prefix Glue metastore delegates from schema mapping rules of the form {@code prefix[:catalogId],...}.
 */
public final class SchemaMappingDelegates
{
    private SchemaMappingDelegates() {}

    /**
     * A rule without a catalog id reuses the default Glue metastore when one exists,
     * otherwise it gets its own same-account Glue metastore.
     */
    public static Map<String, HiveMetastore> createDelegates(
            String rules,
            Optional<HiveMetastore> defaultGlueMetastore,
            GlueHiveMetastoreConfig config,
            TrinoFileSystemFactory fileSystemFactory,
            CatalogName catalogName,
            Set<GlueHiveMetastore.TableKind> visibleTableKinds)
    {
        ImmutableMap.Builder<String, HiveMetastore> delegates = ImmutableMap.builder();
        for (String rule : Splitter.on(',').trimResults().omitEmptyStrings().split(rules)) {
            int separator = rule.indexOf(':');
            String prefix;
            Optional<String> catalogId;
            if (separator == -1) {
                prefix = rule;
                catalogId = Optional.empty();
            }
            else {
                prefix = rule.substring(0, separator);
                if (separator == rule.length() - 1) {
                    catalogId = Optional.empty();
                }
                else {
                    catalogId = Optional.of(rule.substring(separator + 1));
                }
            }
            checkArgument(!prefix.isEmpty(), "Empty prefix in schema mapping rule: %s", rule);
            if (catalogId.isEmpty() && defaultGlueMetastore.isPresent()) {
                delegates.put(prefix, defaultGlueMetastore.get());
                continue;
            }
            ImmutableSet.Builder<ExecutionInterceptor> interceptors = ImmutableSet.builder();
            interceptors.add(new GlueHiveExecutionInterceptor(config));
            catalogId.ifPresent(id -> interceptors.add(new GlueCatalogIdInterceptor(new GlueHiveMetastoreConfig().setCatalogId(id))));
            delegates.put(prefix, new GlueHiveMetastore(
                    GlueMetastoreModule.createGlueClient(config, interceptors.build()),
                    GlueCache.NOOP,
                    new GlueMetastoreStats(),
                    fileSystemFactory,
                    config,
                    new CatalogName(catalogName + "-" + prefix),
                    visibleTableKinds));
        }
        return delegates.buildOrThrow();
    }
}
