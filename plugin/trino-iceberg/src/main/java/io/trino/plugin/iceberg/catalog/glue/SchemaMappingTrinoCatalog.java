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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import io.trino.metastore.TableInfo;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;
import jakarta.annotation.Nullable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Routes prefixed namespace names to per-rule Iceberg Glue catalogs, exposing each delegate's namespaces under the prefix.
 */
public class SchemaMappingTrinoCatalog
        implements TrinoCatalog
{
    private final TrinoCatalog defaultDelegate;
    private final Map<String, TrinoCatalog> delegatesByPrefix;

    public SchemaMappingTrinoCatalog(TrinoCatalog defaultDelegate, Map<String, TrinoCatalog> delegatesByPrefix)
    {
        this.defaultDelegate = requireNonNull(defaultDelegate, "defaultDelegate is null");
        this.delegatesByPrefix = ImmutableMap.copyOf(requireNonNull(delegatesByPrefix, "delegatesByPrefix is null"));
    }

    private record Resolved(String prefix, TrinoCatalog delegate, String realNamespace)
    {
        SchemaTableName realName(SchemaTableName name)
        {
            return new SchemaTableName(realNamespace, name.getTableName());
        }

        SchemaTableName prefixedName(SchemaTableName name)
        {
            return new SchemaTableName(prefix + name.getSchemaName(), name.getTableName());
        }
    }

    private Resolved resolve(String namespace)
    {
        for (Map.Entry<String, TrinoCatalog> entry : delegatesByPrefix.entrySet()) {
            if (namespace.startsWith(entry.getKey())) {
                return new Resolved(entry.getKey(), entry.getValue(), namespace.substring(entry.getKey().length()));
            }
        }
        return new Resolved("", defaultDelegate, namespace);
    }

    private Resolved resolve(SchemaTableName name)
    {
        return resolve(name.getSchemaName());
    }

    private Resolved resolveSame(SchemaTableName source, SchemaTableName target)
    {
        Resolved resolvedSource = resolve(source);
        Resolved resolvedTarget = resolve(target);
        if (resolvedSource.delegate() != resolvedTarget.delegate()) {
            throw new TrinoException(NOT_SUPPORTED, "Renaming across schema mapping rules is not supported");
        }
        return resolvedSource;
    }

    private record Delegate(String prefix, TrinoCatalog catalog)
    {
        SchemaTableName prefixedName(SchemaTableName name)
        {
            return new SchemaTableName(prefix + name.getSchemaName(), name.getTableName());
        }
    }

    private Stream<Delegate> allDelegates()
    {
        return Stream.concat(
                Stream.of(new Delegate("", defaultDelegate)),
                delegatesByPrefix.entrySet().stream().map(entry -> new Delegate(entry.getKey(), entry.getValue())));
    }

    private <T> List<T> collectAll(Optional<String> namespace, Function<Delegate, Stream<T>> lister)
    {
        if (namespace.isPresent()) {
            Resolved resolved = resolve(namespace.get());
            return lister.apply(new Delegate(resolved.prefix(), resolved.delegate())).collect(toImmutableList());
        }
        return allDelegates().flatMap(lister).collect(toImmutableList());
    }

    @Override
    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        Resolved resolved = resolve(namespace);
        return resolved.delegate().namespaceExists(session, resolved.realNamespace());
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        return allDelegates()
                .flatMap(delegate -> delegate.catalog().listNamespaces(session).stream().map(name -> delegate.prefix() + name))
                .collect(toImmutableList());
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        Resolved resolved = resolve(namespace);
        resolved.delegate().dropNamespace(session, resolved.realNamespace());
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        Resolved resolved = resolve(namespace);
        return resolved.delegate().loadNamespaceMetadata(session, resolved.realNamespace());
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        Resolved resolved = resolve(namespace);
        return resolved.delegate().getNamespacePrincipal(session, resolved.realNamespace());
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        Resolved resolved = resolve(namespace);
        resolved.delegate().createNamespace(session, resolved.realNamespace(), properties, owner);
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        Resolved resolved = resolve(namespace);
        resolved.delegate().setNamespacePrincipal(session, resolved.realNamespace(), principal);
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        Resolved resolvedSource = resolve(source);
        Resolved resolvedTarget = resolve(target);
        if (resolvedSource.delegate() != resolvedTarget.delegate()) {
            throw new TrinoException(NOT_SUPPORTED, "Renaming across schema mapping rules is not supported");
        }
        resolvedSource.delegate().renameNamespace(session, resolvedSource.realNamespace(), resolvedTarget.realNamespace());
    }

    @Override
    public List<TableInfo> listTables(ConnectorSession session, Optional<String> namespace)
    {
        return collectAll(namespace, delegate -> delegate.catalog().listTables(session, namespace.map(name -> resolve(name).realNamespace())).stream()
                .map(info -> new TableInfo(delegate.prefixedName(info.tableName()), info.extendedRelationType())));
    }

    @Override
    public List<SchemaTableName> listIcebergTables(ConnectorSession session, List<String> filter)
    {
        if (filter.isEmpty()) {
            return allDelegates()
                    .flatMap(delegate -> delegate.catalog().listIcebergTables(session, ImmutableList.of()).stream().map(delegate::prefixedName))
                    .collect(toImmutableList());
        }
        return filter.stream()
                .flatMap(namespace -> {
                    Resolved resolved = resolve(namespace);
                    return resolved.delegate().listIcebergTables(session, ImmutableList.of(resolved.realNamespace())).stream()
                            .map(resolved::prefixedName);
                })
                .collect(toImmutableList());
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        return collectAll(namespace, delegate -> delegate.catalog().listViews(session, namespace.map(name -> resolve(name).realNamespace())).stream()
                .map(delegate::prefixedName));
    }

    @Override
    public Optional<Iterator<RelationColumnsMetadata>> streamRelationColumns(
            ConnectorSession session,
            Optional<String> namespace,
            UnaryOperator<Set<SchemaTableName>> relationFilter,
            Predicate<SchemaTableName> isRedirected)
    {
        return streamRelations(
                namespace,
                (delegate, realNamespace) -> delegate.catalog().streamRelationColumns(
                        session,
                        realNamespace,
                        realNames -> unprefix(delegate, relationFilter.apply(prefix(delegate, realNames))),
                        realName -> isRedirected.test(delegate.prefixedName(realName))),
                (delegate, metadata) -> new RelationColumnsMetadata(
                        delegate.prefixedName(metadata.name()),
                        metadata.materializedViewColumns(),
                        metadata.viewColumns(),
                        metadata.tableColumns(),
                        metadata.redirected()));
    }

    @Override
    public Optional<Iterator<RelationCommentMetadata>> streamRelationComments(
            ConnectorSession session,
            Optional<String> namespace,
            UnaryOperator<Set<SchemaTableName>> relationFilter,
            Predicate<SchemaTableName> isRedirected)
    {
        return streamRelations(
                namespace,
                (delegate, realNamespace) -> delegate.catalog().streamRelationComments(
                        session,
                        realNamespace,
                        realNames -> unprefix(delegate, relationFilter.apply(prefix(delegate, realNames))),
                        realName -> isRedirected.test(delegate.prefixedName(realName))),
                (delegate, metadata) -> new RelationCommentMetadata(
                        delegate.prefixedName(metadata.name()),
                        metadata.tableRedirected(),
                        metadata.comment()));
    }

    private interface RelationStreamer<T>
    {
        Optional<Iterator<T>> stream(Delegate delegate, Optional<String> realNamespace);
    }

    private interface RelationMapper<T>
    {
        T map(Delegate delegate, T metadata);
    }

    private <T> Optional<Iterator<T>> streamRelations(Optional<String> namespace, RelationStreamer<T> streamer, RelationMapper<T> mapper)
    {
        if (namespace.isPresent()) {
            Resolved resolved = resolve(namespace.get());
            Delegate delegate = new Delegate(resolved.prefix(), resolved.delegate());
            return streamer.stream(delegate, Optional.of(resolved.realNamespace()))
                    .map(iterator -> Iterators.transform(iterator, metadata -> mapper.map(delegate, metadata)));
        }
        List<Iterator<T>> iterators = new ArrayList<>();
        for (Delegate delegate : allDelegates().collect(toImmutableList())) {
            Optional<Iterator<T>> iterator = streamer.stream(delegate, Optional.empty());
            if (iterator.isEmpty()) {
                return Optional.empty();
            }
            iterators.add(Iterators.transform(iterator.get(), metadata -> mapper.map(delegate, metadata)));
        }
        return Optional.of(Iterators.concat(iterators.iterator()));
    }

    private static Set<SchemaTableName> prefix(Delegate delegate, Set<SchemaTableName> realNames)
    {
        return realNames.stream()
                .map(delegate::prefixedName)
                .collect(toImmutableSet());
    }

    private static Set<SchemaTableName> unprefix(Delegate delegate, Set<SchemaTableName> prefixedNames)
    {
        return prefixedNames.stream()
                .filter(name -> name.getSchemaName().startsWith(delegate.prefix()))
                .map(name -> new SchemaTableName(name.getSchemaName().substring(delegate.prefix().length()), name.getTableName()))
                .collect(toImmutableSet());
    }

    @Override
    public Transaction newCreateTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            SortOrder sortOrder,
            Optional<String> location,
            Map<String, String> properties)
    {
        Resolved resolved = resolve(schemaTableName);
        return resolved.delegate().newCreateTableTransaction(session, resolved.realName(schemaTableName), schema, partitionSpec, sortOrder, location, properties);
    }

    @Override
    public Transaction newCreateOrReplaceTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            SortOrder sortOrder,
            String location,
            Map<String, String> properties)
    {
        Resolved resolved = resolve(schemaTableName);
        return resolved.delegate().newCreateOrReplaceTableTransaction(session, resolved.realName(schemaTableName), schema, partitionSpec, sortOrder, location, properties);
    }

    @Override
    public void registerTable(ConnectorSession session, SchemaTableName tableName, TableMetadata tableMetadata)
    {
        Resolved resolved = resolve(tableName);
        resolved.delegate().registerTable(session, resolved.realName(tableName), tableMetadata);
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName tableName)
    {
        Resolved resolved = resolve(tableName);
        resolved.delegate().unregisterTable(session, resolved.realName(tableName));
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Resolved resolved = resolve(schemaTableName);
        resolved.delegate().dropTable(session, resolved.realName(schemaTableName));
    }

    @Override
    public void dropCorruptedTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Resolved resolved = resolve(schemaTableName);
        resolved.delegate().dropCorruptedTable(session, resolved.realName(schemaTableName));
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        Resolved resolved = resolveSame(from, to);
        resolved.delegate().renameTable(session, resolved.realName(from), resolve(to).realName(to));
    }

    @Override
    public BaseTable loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Resolved resolved = resolve(schemaTableName);
        return resolved.delegate().loadTable(session, resolved.realName(schemaTableName));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> tryGetColumnMetadata(ConnectorSession session, List<SchemaTableName> tables)
    {
        Map<Delegate, List<SchemaTableName>> tablesByDelegate = new HashMap<>();
        for (SchemaTableName table : tables) {
            Resolved resolved = resolve(table);
            tablesByDelegate.computeIfAbsent(new Delegate(resolved.prefix(), resolved.delegate()), _ -> new ArrayList<>()).add(resolved.realName(table));
        }
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> result = ImmutableMap.builder();
        for (Map.Entry<Delegate, List<SchemaTableName>> entry : tablesByDelegate.entrySet()) {
            Delegate delegate = entry.getKey();
            delegate.catalog().tryGetColumnMetadata(session, entry.getValue())
                    .forEach((realName, columns) -> result.put(delegate.prefixedName(realName), columns));
        }
        return result.buildOrThrow();
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        Resolved resolved = resolve(schemaTableName);
        resolved.delegate().updateTableComment(session, resolved.realName(schemaTableName), comment);
    }

    @Override
    public void updateViewComment(ConnectorSession session, SchemaTableName schemaViewName, Optional<String> comment)
    {
        Resolved resolved = resolve(schemaViewName);
        resolved.delegate().updateViewComment(session, resolved.realName(schemaViewName), comment);
    }

    @Override
    public void updateViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        Resolved resolved = resolve(schemaViewName);
        resolved.delegate().updateViewColumnComment(session, resolved.realName(schemaViewName), columnName, comment);
    }

    @Nullable
    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Resolved resolved = resolve(schemaTableName);
        return resolved.delegate().defaultTableLocation(session, resolved.realName(schemaTableName));
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        Resolved resolved = resolve(schemaTableName);
        resolved.delegate().setTablePrincipal(session, resolved.realName(schemaTableName), principal);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        Resolved resolved = resolve(schemaViewName);
        resolved.delegate().createView(session, resolved.realName(schemaViewName), definition, viewProperties, replace);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        Resolved resolved = resolveSame(source, target);
        resolved.delegate().renameView(session, resolved.realName(source), resolve(target).realName(target));
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        Resolved resolved = resolve(schemaViewName);
        resolved.delegate().setViewPrincipal(session, resolved.realName(schemaViewName), principal);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        Resolved resolved = resolve(schemaViewName);
        resolved.delegate().dropView(session, resolved.realName(schemaViewName));
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        return collectAll(namespace, delegate -> delegate.catalog().getViews(session, namespace.map(name -> resolve(name).realNamespace())).entrySet().stream()
                .map(entry -> Map.entry(delegate.prefixedName(entry.getKey()), entry.getValue())))
                .stream()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        Resolved resolved = resolve(viewName);
        return resolved.delegate().getView(session, resolved.realName(viewName));
    }

    @Override
    public Map<String, Object> getViewProperties(ConnectorSession session, SchemaTableName viewName)
    {
        Resolved resolved = resolve(viewName);
        return resolved.delegate().getViewProperties(session, resolved.realName(viewName));
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties,
            boolean replace,
            boolean ignoreExisting)
    {
        Resolved resolved = resolve(viewName);
        resolved.delegate().createMaterializedView(session, resolved.realName(viewName), definition, materializedViewProperties, replace, ignoreExisting);
    }

    @Override
    public void updateMaterializedViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        Resolved resolved = resolve(schemaViewName);
        resolved.delegate().updateMaterializedViewColumnComment(session, resolved.realName(schemaViewName), columnName, comment);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        Resolved resolved = resolve(viewName);
        resolved.delegate().dropMaterializedView(session, resolved.realName(viewName));
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        Resolved resolved = resolve(viewName);
        return resolved.delegate().getMaterializedView(session, resolved.realName(viewName));
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
    {
        Resolved resolved = resolve(viewName);
        return resolved.delegate().getMaterializedViewProperties(session, resolved.realName(viewName), definition);
    }

    @Override
    public Optional<BaseTable> getMaterializedViewStorageTable(ConnectorSession session, SchemaTableName viewName)
    {
        Resolved resolved = resolve(viewName);
        return resolved.delegate().getMaterializedViewStorageTable(session, resolved.realName(viewName));
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        Resolved resolved = resolveSame(source, target);
        resolved.delegate().renameMaterializedView(session, resolved.realName(source), resolve(target).realName(target));
    }

    @Override
    public void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment)
    {
        Resolved resolved = resolve(schemaTableName);
        resolved.delegate().updateColumnComment(session, resolved.realName(schemaTableName), columnIdentity, comment);
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName, String hiveCatalogName)
    {
        Resolved resolved = resolve(tableName);
        return resolved.delegate().redirectTable(session, resolved.realName(tableName), hiveCatalogName)
                .map(redirected -> new CatalogSchemaTableName(redirected.getCatalogName(), resolved.prefixedName(redirected.getSchemaTableName())));
    }
}
