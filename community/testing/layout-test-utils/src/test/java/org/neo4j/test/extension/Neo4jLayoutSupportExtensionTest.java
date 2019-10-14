/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.test.extension;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;

@Neo4jLayoutExtension
class Neo4jLayoutSupportExtensionTest
{
    @Inject
    private Neo4jLayout neo4jLayout;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldInjectLayouts()
    {
        assertNotNull( neo4jLayout );
        assertNotNull( databaseLayout );
        assertNotNull( fs );
        assertNotNull( databaseLayout );
    }

    @Test
    void shouldCreateDirectories()
    {
        assertTrue( fs.fileExists( neo4jLayout.homeDirectory() ) );
        assertTrue( fs.fileExists( neo4jLayout.databasesDirectory() ) );
        assertTrue( fs.fileExists( neo4jLayout.transactionLogsRootDirectory() ) );

        assertTrue( fs.fileExists( databaseLayout.databaseDirectory() ) );
        assertTrue( fs.fileExists( databaseLayout.getTransactionLogsDirectory() ) );
    }

    @Test
    void shouldUseDefaultConfig()
    {
        Config defaultConfig = Config.defaults( neo4j_home, testDirectory.homeDir().toPath() );
        Neo4jLayout defaultNeo4jLayout = Neo4jLayout.of( defaultConfig );
        DatabaseLayout defaultDatabaseLayout = defaultNeo4jLayout.databaseLayout( defaultConfig.get( default_database ) );

        assertEquals( defaultNeo4jLayout.homeDirectory(), neo4jLayout.homeDirectory() );
        assertEquals( defaultNeo4jLayout.databasesDirectory(), neo4jLayout.databasesDirectory() );
        assertEquals( defaultNeo4jLayout.transactionLogsRootDirectory(), neo4jLayout.transactionLogsRootDirectory() );

        assertEquals( defaultDatabaseLayout.databaseDirectory(), databaseLayout.databaseDirectory() );
        assertEquals( defaultDatabaseLayout.getTransactionLogsDirectory(), databaseLayout.getTransactionLogsDirectory() );
    }
}
