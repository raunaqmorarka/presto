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

import com.google.common.collect.ImmutableMap;
import io.trino.metastore.AcidOperation;
import io.trino.metastore.AcidTransactionOwner;
import io.trino.metastore.Database;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HivePartition;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
import io.trino.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Routes prefixed schema names to per-rule metastore delegates, exposing each delegate's databases under the prefix.
 */
public class SchemaMappingHiveMetastore
        implements HiveMetastore
{
    private final HiveMetastore defaultDelegate;
    private final Map<String, HiveMetastore> delegatesByPrefix;

    public SchemaMappingHiveMetastore(HiveMetastore defaultDelegate, Map<String, HiveMetastore> delegatesByPrefix)
    {
        this.defaultDelegate = requireNonNull(defaultDelegate, "defaultDelegate is null");
        this.delegatesByPrefix = ImmutableMap.copyOf(requireNonNull(delegatesByPrefix, "delegatesByPrefix is null"));
    }

    private record Resolved(HiveMetastore delegate, String realDatabaseName) {}

    private Resolved resolve(String databaseName)
    {
        for (Map.Entry<String, HiveMetastore> entry : delegatesByPrefix.entrySet()) {
            if (databaseName.startsWith(entry.getKey())) {
                return new Resolved(entry.getValue(), databaseName.substring(entry.getKey().length()));
            }
        }
        return new Resolved(defaultDelegate, databaseName);
    }

    private Resolved resolve(Table table)
    {
        return resolve(table.getDatabaseName());
    }

    private static Table withDatabaseName(Table table, String databaseName)
    {
        if (table.getDatabaseName().equals(databaseName)) {
            return table;
        }
        return Table.builder(table).setDatabaseName(databaseName).build();
    }

    private static Partition withDatabaseName(Partition partition, String databaseName)
    {
        if (partition.getDatabaseName().equals(databaseName)) {
            return partition;
        }
        return Partition.builder(partition).setDatabaseName(databaseName).build();
    }

    private static PartitionWithStatistics withDatabaseName(PartitionWithStatistics partition, String databaseName)
    {
        return new PartitionWithStatistics(
                withDatabaseName(partition.getPartition(), databaseName),
                partition.getPartitionName(),
                partition.getStatistics());
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        Resolved resolved = resolve(databaseName);
        return resolved.delegate().getDatabase(resolved.realDatabaseName())
                .map(database -> Database.builder(database).setDatabaseName(databaseName).build());
    }

    @Override
    public List<String> getAllDatabases()
    {
        return Stream.concat(
                        defaultDelegate.getAllDatabases().stream(),
                        delegatesByPrefix.entrySet().stream()
                                .flatMap(entry -> entry.getValue().getAllDatabases().stream()
                                        .map(name -> entry.getKey() + name)))
                .collect(toImmutableList());
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        Resolved resolved = resolve(databaseName);
        return resolved.delegate().getTable(resolved.realDatabaseName(), tableName)
                .map(table -> withDatabaseName(table, databaseName));
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        Resolved resolved = resolve(databaseName);
        return resolved.delegate().getTableColumnStatistics(resolved.realDatabaseName(), tableName, columnNames);
    }

    @Override
    public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames)
    {
        Resolved resolved = resolve(databaseName);
        return resolved.delegate().getPartitionColumnStatistics(resolved.realDatabaseName(), tableName, partitionNames, columnNames);
    }

    @Override
    public boolean useSparkTableStatistics()
    {
        return defaultDelegate.useSparkTableStatistics();
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().updateTableStatistics(resolved.realDatabaseName(), tableName, acidWriteId, mode, statisticsUpdate);
    }

    @Override
    public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
    {
        Resolved resolved = resolve(table);
        resolved.delegate().updatePartitionStatistics(withDatabaseName(table, resolved.realDatabaseName()), mode, partitionUpdates);
    }

    @Override
    public List<TableInfo> getTables(String databaseName)
    {
        Resolved resolved = resolve(databaseName);
        return resolved.delegate().getTables(resolved.realDatabaseName()).stream()
                .map(tableInfo -> new TableInfo(
                        new SchemaTableName(databaseName, tableInfo.tableName().getTableName()),
                        tableInfo.extendedRelationType()))
                .collect(toImmutableList());
    }

    @Override
    public List<String> getTableNamesWithParameters(String databaseName, String parameterKey, Set<String> parameterValues)
    {
        Resolved resolved = resolve(databaseName);
        return resolved.delegate().getTableNamesWithParameters(resolved.realDatabaseName(), parameterKey, parameterValues);
    }

    @Override
    public void createDatabase(Database database)
    {
        Resolved resolved = resolve(database.getDatabaseName());
        resolved.delegate().createDatabase(Database.builder(database).setDatabaseName(resolved.realDatabaseName()).build());
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().dropDatabase(resolved.realDatabaseName(), deleteData);
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        Resolved from = resolve(databaseName);
        Resolved to = resolve(newDatabaseName);
        if (from.delegate() != to.delegate()) {
            throw new TrinoException(NOT_SUPPORTED, "Rename across schema mapping targets is not supported");
        }
        from.delegate().renameDatabase(from.realDatabaseName(), to.realDatabaseName());
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().setDatabaseOwner(resolved.realDatabaseName(), principal);
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        Resolved resolved = resolve(table);
        resolved.delegate().createTable(withDatabaseName(table, resolved.realDatabaseName()), principalPrivileges);
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().dropTable(resolved.realDatabaseName(), tableName, deleteData);
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges, Map<String, String> environmentContext)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().replaceTable(resolved.realDatabaseName(), tableName, withDatabaseName(newTable, resolved.realDatabaseName()), principalPrivileges, environmentContext);
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        Resolved from = resolve(databaseName);
        Resolved to = resolve(newDatabaseName);
        if (from.delegate() != to.delegate()) {
            throw new TrinoException(NOT_SUPPORTED, "Rename across schema mapping targets is not supported");
        }
        from.delegate().renameTable(from.realDatabaseName(), tableName, to.realDatabaseName(), newTableName);
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().commentTable(resolved.realDatabaseName(), tableName, comment);
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().setTableOwner(resolved.realDatabaseName(), tableName, principal);
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().commentColumn(resolved.realDatabaseName(), tableName, columnName, comment);
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().addColumn(resolved.realDatabaseName(), tableName, columnName, columnType, columnComment);
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().renameColumn(resolved.realDatabaseName(), tableName, oldColumnName, newColumnName);
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().dropColumn(resolved.realDatabaseName(), tableName, columnName);
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        Resolved resolved = resolve(table);
        return resolved.delegate().getPartition(withDatabaseName(table, resolved.realDatabaseName()), partitionValues)
                .map(partition -> withDatabaseName(partition, table.getDatabaseName()));
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        Resolved resolved = resolve(databaseName);
        return resolved.delegate().getPartitionNamesByFilter(resolved.realDatabaseName(), tableName, columnNames, partitionKeysFilter);
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        Resolved resolved = resolve(table);
        Map<String, Optional<Partition>> partitions = resolved.delegate().getPartitionsByNames(withDatabaseName(table, resolved.realDatabaseName()), partitionNames);
        ImmutableMap.Builder<String, Optional<Partition>> result = ImmutableMap.builder();
        partitions.forEach((name, partition) -> result.put(name, partition.map(value -> withDatabaseName(value, table.getDatabaseName()))));
        return result.buildOrThrow();
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().addPartitions(
                resolved.realDatabaseName(),
                tableName,
                partitions.stream()
                        .map(partition -> withDatabaseName(partition, resolved.realDatabaseName()))
                        .collect(toImmutableList()));
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().dropPartition(resolved.realDatabaseName(), tableName, parts, deleteData);
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().alterPartition(resolved.realDatabaseName(), tableName, withDatabaseName(partition, resolved.realDatabaseName()));
    }

    @Override
    public void createRole(String role, String grantor)
    {
        defaultDelegate.createRole(role, grantor);
    }

    @Override
    public void dropRole(String role)
    {
        defaultDelegate.dropRole(role);
    }

    @Override
    public Set<String> listRoles()
    {
        return defaultDelegate.listRoles();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        defaultDelegate.grantRoles(roles, grantees, adminOption, grantor);
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        defaultDelegate.revokeRoles(roles, grantees, adminOption, grantor);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return defaultDelegate.listRoleGrants(principal);
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().grantTablePrivileges(resolved.realDatabaseName(), tableName, tableOwner, grantee, grantor, privileges, grantOption);
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().revokeTablePrivileges(resolved.realDatabaseName(), tableName, tableOwner, grantee, grantor, privileges, grantOption);
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        Resolved resolved = resolve(databaseName);
        return resolved.delegate().listTablePrivileges(resolved.realDatabaseName(), tableName, tableOwner, principal);
    }

    @Override
    public void checkSupportsTransactions()
    {
        defaultDelegate.checkSupportsTransactions();
    }

    @Override
    public long openTransaction(AcidTransactionOwner transactionOwner)
    {
        return defaultDelegate.openTransaction(transactionOwner);
    }

    @Override
    public void commitTransaction(long transactionId)
    {
        defaultDelegate.commitTransaction(transactionId);
    }

    @Override
    public void abortTransaction(long transactionId)
    {
        defaultDelegate.abortTransaction(transactionId);
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
    {
        defaultDelegate.sendTransactionHeartbeat(transactionId);
    }

    @Override
    public void acquireSharedReadLock(AcidTransactionOwner transactionOwner, String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions)
    {
        defaultDelegate.acquireSharedReadLock(transactionOwner, queryId, transactionId, fullTables, partitions);
    }

    @Override
    public String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId)
    {
        return defaultDelegate.getValidWriteIds(tables, currentTransactionId);
    }

    @Override
    public Optional<String> getConfigValue(String name)
    {
        return defaultDelegate.getConfigValue(name);
    }

    @Override
    public long allocateWriteId(String dbName, String tableName, long transactionId)
    {
        Resolved resolved = resolve(dbName);
        return resolved.delegate().allocateWriteId(resolved.realDatabaseName(), tableName, transactionId);
    }

    @Override
    public void acquireTableWriteLock(AcidTransactionOwner transactionOwner, String queryId, long transactionId, String dbName, String tableName, AcidOperation operation, boolean isDynamicPartitionWrite)
    {
        Resolved resolved = resolve(dbName);
        resolved.delegate().acquireTableWriteLock(transactionOwner, queryId, transactionId, resolved.realDatabaseName(), tableName, operation, isDynamicPartitionWrite);
    }

    @Override
    public void updateTableWriteId(String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        Resolved resolved = resolve(dbName);
        resolved.delegate().updateTableWriteId(resolved.realDatabaseName(), tableName, transactionId, writeId, rowCountChange);
    }

    @Override
    public void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        Resolved resolved = resolve(dbName);
        resolved.delegate().addDynamicPartitions(resolved.realDatabaseName(), tableName, partitionNames, transactionId, writeId, operation);
    }

    @Override
    public void alterTransactionalTable(Table table, long transactionId, long writeId, PrincipalPrivileges principalPrivileges)
    {
        Resolved resolved = resolve(table);
        resolved.delegate().alterTransactionalTable(withDatabaseName(table, resolved.realDatabaseName()), transactionId, writeId, principalPrivileges);
    }

    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        Resolved resolved = resolve(databaseName);
        return resolved.delegate().functionExists(resolved.realDatabaseName(), functionName, signatureToken);
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName)
    {
        Resolved resolved = resolve(databaseName);
        return resolved.delegate().getAllFunctions(resolved.realDatabaseName());
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        Resolved resolved = resolve(databaseName);
        return resolved.delegate().getFunctions(resolved.realDatabaseName(), functionName);
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().createFunction(resolved.realDatabaseName(), functionName, function);
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().replaceFunction(resolved.realDatabaseName(), functionName, function);
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        Resolved resolved = resolve(databaseName);
        resolved.delegate().dropFunction(resolved.realDatabaseName(), functionName, signatureToken);
    }

    @Override
    public Metrics getMetrics()
    {
        return defaultDelegate.getMetrics();
    }
}
