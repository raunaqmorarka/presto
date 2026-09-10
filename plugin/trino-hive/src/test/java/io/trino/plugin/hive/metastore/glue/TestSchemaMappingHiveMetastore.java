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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestSchemaMappingHiveMetastore
{
    @TempDir
    private Path tempDirectory;

    private HiveMetastore localDelegate;
    private HiveMetastore firstRemoteDelegate;
    private HiveMetastore secondRemoteDelegate;
    private SchemaMappingHiveMetastore metastore;

    @BeforeEach
    void setUp()
    {
        localDelegate = createTestingFileHiveMetastore(tempDirectory.resolve("local").toFile());
        firstRemoteDelegate = createTestingFileHiveMetastore(tempDirectory.resolve("first").toFile());
        secondRemoteDelegate = createTestingFileHiveMetastore(tempDirectory.resolve("second").toFile());
        metastore = new SchemaMappingHiveMetastore(
                localDelegate,
                ImmutableMap.of("first_", firstRemoteDelegate, "second_", secondRemoteDelegate));
    }

    @Test
    void testUnprefixedNameResolvesToTheOnlyDelegateHoldingIt()
    {
        createTable(firstRemoteDelegate, "metrics", "visits");

        assertThat(metastore.getDatabase("first_metrics")).isPresent();
        assertThat(metastore.getTable("first_metrics", "visits")).isPresent();

        assertThat(metastore.getDatabase("metrics").orElseThrow().getDatabaseName()).isEqualTo("metrics");
        assertThat(metastore.getTable("metrics", "visits").orElseThrow().getDatabaseName()).isEqualTo("metrics");
        assertThat(metastore.getTables("metrics"))
                .extracting(TableInfo::tableName)
                .extracting(SchemaTableName::getSchemaName)
                .containsExactly("metrics");
    }

    @Test
    void testLocalSchemaWinsOverRemoteWithTheSameName()
    {
        createTable(localDelegate, "shared", "local_table");
        createTable(firstRemoteDelegate, "shared", "remote_table");

        assertThat(metastore.getTable("shared", "local_table")).isPresent();
        assertThat(metastore.getTable("shared", "remote_table")).isEmpty();
    }

    @Test
    void testAmbiguousUnprefixedNameIsRejected()
    {
        createTable(firstRemoteDelegate, "shared", "first_table");
        createTable(secondRemoteDelegate, "shared", "second_table");

        assertThatThrownBy(() -> metastore.getTable("shared", "first_table"))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Schema shared exists in multiple schema mapping rules, qualify it with a prefix");
    }

    @Test
    void testListingExposesPrefixedNamesAndResolvableRealNames()
    {
        createTable(localDelegate, "local_only", "local_table");
        createTable(firstRemoteDelegate, "metrics", "visits");
        createTable(firstRemoteDelegate, "shared", "first_table");
        createTable(secondRemoteDelegate, "shared", "second_table");

        assertThat(metastore.getAllDatabases())
                .containsExactlyInAnyOrder("local_only", "first_metrics", "first_shared", "second_shared", "metrics");
    }

    @Test
    void testMissingNameStaysMissing()
    {
        assertThat(metastore.getDatabase("absent")).isEmpty();
        assertThat(metastore.getTable("absent", "absent")).isEmpty();
        assertThat(metastore.getTables("absent")).isEmpty();
    }

    private static void createTable(HiveMetastore delegate, String databaseName, String tableName)
    {
        if (delegate.getDatabase(databaseName).isEmpty()) {
            delegate.createDatabase(Database.builder()
                    .setDatabaseName(databaseName)
                    .setOwnerName(Optional.of("public"))
                    .setOwnerType(Optional.of(PrincipalType.ROLE))
                    .build());
        }
        delegate.createTable(
                Table.builder()
                        .setDatabaseName(databaseName)
                        .setTableName(tableName)
                        .setOwner(Optional.of("public"))
                        .setTableType(VIRTUAL_VIEW.name())
                        .setViewOriginalText(Optional.of("SELECT value FROM " + databaseName + ".source"))
                        .setViewExpandedText(Optional.of("SELECT value FROM " + databaseName + ".source"))
                        .setDataColumns(ImmutableList.of(new Column("value", HIVE_STRING, Optional.empty(), ImmutableMap.of())))
                        .withStorage(storage -> storage.setStorageFormat(VIEW_STORAGE_FORMAT).setLocation(""))
                        .build(),
                NO_PRIVILEGES);
    }
}
