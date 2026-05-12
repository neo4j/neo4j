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
import static org.neo4j.configuration.GraphDatabaseSettings.initial_default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.utils.TestDirectory;

@Neo4jLayoutExtension
class Neo4jLayoutSupportExtensionTest {
    @Inject
    private Neo4jLayout neo4jLayout;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldInjectLayouts() {
        assertThat(neo4jLayout).isNotNull();
        assertThat(databaseLayout).isNotNull();
        assertThat(fs).isNotNull();
        assertThat(databaseLayout).isNotNull();
    }

    @Test
    void shouldCreateDirectories() {
        assertThat(neo4jLayout.homeDirectory()).exists();
        assertThat(neo4jLayout.databasesDirectory()).exists();
        assertThat(neo4jLayout.transactionLogsRootDirectory()).exists();

        assertThat(fs.fileExists(databaseLayout.databaseDirectory())).isTrue();
        assertThat(fs.fileExists(databaseLayout.getTransactionLogsDirectory())).isTrue();
    }

    @Test
    void shouldUseDefaultConfig() {
        Config defaultConfig = Config.defaults(neo4j_home, testDirectory.homePath());
        Neo4jLayout defaultNeo4jLayout = Neo4jLayout.of(defaultConfig);
        DatabaseLayout defaultDatabaseLayout =
                defaultNeo4jLayout.databaseLayout(defaultConfig.get(initial_default_database));

        assertThat(neo4jLayout.homeDirectory()).isEqualTo(defaultNeo4jLayout.homeDirectory());
        assertThat(neo4jLayout.databasesDirectory()).isEqualTo(defaultNeo4jLayout.databasesDirectory());
        assertThat(neo4jLayout.transactionLogsRootDirectory())
                .isEqualTo(defaultNeo4jLayout.transactionLogsRootDirectory());

        assertThat(databaseLayout.databaseDirectory()).isEqualTo(defaultDatabaseLayout.databaseDirectory());
        assertThat(databaseLayout.getTransactionLogsDirectory())
                .isEqualTo(defaultDatabaseLayout.getTransactionLogsDirectory());
    }
}
