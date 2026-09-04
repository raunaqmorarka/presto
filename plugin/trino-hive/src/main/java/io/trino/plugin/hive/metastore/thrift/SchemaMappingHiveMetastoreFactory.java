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
package io.trino.plugin.hive.metastore.thrift;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.glue.SchemaMappingDelegates;
import io.trino.plugin.hive.metastore.glue.SchemaMappingHiveMetastore;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.ConnectorIdentity;

import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Applies schema mapping rules on top of the thrift metastore, so unprefixed schema names
 * stay on thrift while prefixed schema names go to per-rule Glue metastores.
 */
public class SchemaMappingHiveMetastoreFactory
        implements HiveMetastoreFactory
{
    private final BridgingHiveMetastoreFactory delegateFactory;
    private final Map<String, HiveMetastore> delegatesByPrefix;

    @Inject
    public SchemaMappingHiveMetastoreFactory(
            BridgingHiveMetastoreFactory delegateFactory,
            GlueHiveMetastoreConfig glueConfig,
            TrinoFileSystemFactory fileSystemFactory,
            CatalogName catalogName)
    {
        this.delegateFactory = requireNonNull(delegateFactory, "delegateFactory is null");
        checkArgument(glueConfig.getSchemaMappingRules().isPresent(), "schema mapping rules are not set");
        // The generic metastore cache wraps the whole mapping, so the Glue delegates run without their own cache.
        this.delegatesByPrefix = SchemaMappingDelegates.createDelegates(
                glueConfig.getSchemaMappingRules().get(),
                Optional.empty(),
                glueConfig,
                fileSystemFactory,
                catalogName,
                EnumSet.allOf(GlueHiveMetastore.TableKind.class));
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return delegateFactory.isImpersonationEnabled();
    }

    @Override
    public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        return new SchemaMappingHiveMetastore(delegateFactory.createMetastore(identity), delegatesByPrefix);
    }
}
