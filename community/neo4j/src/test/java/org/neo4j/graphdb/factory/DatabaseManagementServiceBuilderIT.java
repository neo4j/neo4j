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
package org.neo4j.graphdb.factory;

import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;
import static org.neo4j.io.fs.FileSystemUtils.isEmptyOrNonExistingDirectory;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

@Neo4jLayoutExtension
class DatabaseManagementServiceBuilderIT
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private Neo4jLayout neo4jLayout;

    @Test
    void startSystemAndDefaultDatabase()
    {
        DatabaseManagementService managementService = new DatabaseManagementServiceBuilder( testDirectory.homeDir() ).build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            DependencyResolver dependencyResolver = database.getDependencyResolver();
            DatabaseManager<?> databaseManager = dependencyResolver.resolveDependency( DatabaseManager.class );
            assertThat( databaseManager.getDatabaseContext( DEFAULT_DATABASE_NAME ) ).isNotEmpty();
            assertThat( databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID ) ).isNotEmpty();
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void configuredDatabasesRootPath()
    {
        File homeDir = testDirectory.homeDir();
        File storeDir = testDirectory.homeDir();
        File databasesDir = testDirectory.directory( "my_databases" );

        DatabaseManagementService managementService = new DatabaseManagementServiceBuilder( homeDir )
                .setConfig( databases_root_path, databasesDir.toPath() )
                .build();
        try
        {
            assertTrue( isEmptyOrNonExistingDirectory( fs, new File( storeDir, DEFAULT_DATABASE_NAME ) ) );
            assertTrue( isEmptyOrNonExistingDirectory( fs, new File( storeDir, SYSTEM_DATABASE_NAME ) ) );

            assertFalse( isEmptyOrNonExistingDirectory( fs, new File( databasesDir, DEFAULT_DATABASE_NAME ) ) );
            assertFalse( isEmptyOrNonExistingDirectory( fs, new File( databasesDir, SYSTEM_DATABASE_NAME ) ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void notConfiguredDatabasesRootPath()
    {
        Neo4jLayout layout = neo4jLayout;

        DatabaseManagementService managementService = new DatabaseManagementServiceBuilder( layout.homeDirectory() ).build();
        try
        {
            assertFalse( isEmptyOrNonExistingDirectory( fs, layout.databaseLayout( DEFAULT_DATABASE_NAME ).databaseDirectory() ) );
            assertFalse( isEmptyOrNonExistingDirectory( fs, layout.databaseLayout( SYSTEM_DATABASE_NAME ).databaseDirectory() ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }
}
