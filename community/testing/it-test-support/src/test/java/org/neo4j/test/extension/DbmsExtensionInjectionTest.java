/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.test.extension;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.utils.TestDirectory;

@DbmsExtension
class DbmsExtensionInjectionTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private DatabaseManagementService dbms;

    @Inject
    private GraphDatabaseService db;

    @Inject
    private GraphDatabaseAPI dbApi;

    @Inject
    private Neo4jLayout neo4jLayout;

    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void shouldInject() {
        assertThat(fs).isNotNull();
        assertThat(testDirectory).isNotNull();
        assertThat(dbms).isNotNull();
        assertThat(db).isNotNull();
        assertThat(dbApi).isNotNull();
        assertThat(neo4jLayout).isNotNull();
        assertThat(databaseLayout).isNotNull();

        assertThat(fs).isEqualTo(testDirectory.getFileSystem());
        assertThat(fs).isInstanceOf(DefaultFileSystemAbstraction.class);

        assertThat(db).isSameAs(dbApi);
        assertThat(neo4jLayout.homeDirectory()).isEqualTo(testDirectory.homePath());
        assertThat(databaseLayout.getDatabaseName()).isEqualTo(db.databaseName());
        assertThat(neo4jLayout).isEqualTo(databaseLayout.getNeo4jLayout());
    }

    @Nested
    class NestedTest {
        @Test
        void injectedFieldsShouldBeAvailableForNestedTests() {
            shouldInject();
        }
    }
}
