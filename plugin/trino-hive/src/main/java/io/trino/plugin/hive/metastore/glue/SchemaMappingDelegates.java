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
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Builds per-prefix Glue metastore delegates from schema mapping rules of the form {@code prefix[:catalogId],...}.
 */
public final class SchemaMappingDelegates
{
    private SchemaMappingDelegates() {}

    /**
     * A rule without catalog id targets the same AWS account as the default Glue client.
     */
    public record SchemaMappingRule(String prefix, Optional<String> catalogId)
    {
        public SchemaMappingRule
        {
            requireNonNull(prefix, "prefix is null");
            requireNonNull(catalogId, "catalogId is null");
            checkArgument(!prefix.isEmpty(), "prefix is empty");
        }
    }

    public static List<SchemaMappingRule> parseRules(String rules)
    {
        return Splitter.on(',').trimResults().omitEmptyStrings().splitToStream(rules)
                .map(SchemaMappingDelegates::parseRule)
                .collect(toImmutableList());
    }

    private static SchemaMappingRule parseRule(String rule)
    {
        int separator = rule.indexOf(':');
        if (separator == -1) {
            return new SchemaMappingRule(rule, Optional.empty());
        }
        String prefix = rule.substring(0, separator);
        checkArgument(!prefix.isEmpty(), "Empty prefix in schema mapping rule: %s", rule);
        if (separator == rule.length() - 1) {
            return new SchemaMappingRule(prefix, Optional.empty());
        }
        return new SchemaMappingRule(prefix, Optional.of(rule.substring(separator + 1)));
    }

    public static GlueClient createGlueClient(GlueHiveMetastoreConfig config, Optional<String> catalogId)
    {
        ImmutableSet.Builder<ExecutionInterceptor> interceptors = ImmutableSet.builder();
        interceptors.add(new GlueHiveExecutionInterceptor(config));
        catalogId.ifPresent(id -> interceptors.add(new GlueCatalogIdInterceptor(new GlueHiveMetastoreConfig().setCatalogId(id))));
        return GlueMetastoreModule.createGlueClient(config, interceptors.build());
    }

    /**
     * A rule without catalog id reuses the default Glue metastore when one exists,
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
        for (SchemaMappingRule rule : parseRules(rules)) {
            if (rule.catalogId().isEmpty() && defaultGlueMetastore.isPresent()) {
                delegates.put(rule.prefix(), defaultGlueMetastore.get());
                continue;
            }
            delegates.put(rule.prefix(), new GlueHiveMetastore(
                    createGlueClient(config, rule.catalogId()),
                    GlueCache.NOOP,
                    new GlueMetastoreStats(),
                    fileSystemFactory,
                    config,
                    new CatalogName(catalogName + "-" + rule.prefix()),
                    visibleTableKinds));
        }
        return delegates.buildOrThrow();
    }
}
