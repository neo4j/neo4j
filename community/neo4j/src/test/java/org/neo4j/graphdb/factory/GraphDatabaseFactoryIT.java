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
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static co.unruly.matchers.OptionalMatchers.empty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.io.fs.FileSystemUtils.isEmptyOrNonExistingDirectory;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class GraphDatabaseFactoryIT
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    @Test
    void startSystemAndDefaultDatabase()
    {
        GraphDatabaseAPI database = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( testDirectory.databaseDir() );
        try
        {
            DependencyResolver dependencyResolver = database.getDependencyResolver();
            DatabaseManager<?> databaseManager = dependencyResolver.resolveDependency( DatabaseManager.class );
            Config config = dependencyResolver.resolveDependency( Config.class );
            assertThat( databaseManager.getDatabaseContext( new DatabaseId( config.get( default_database ) ) ), not( empty() ) );
            assertThat( databaseManager.getDatabaseContext( new DatabaseId( SYSTEM_DATABASE_NAME ) ), not( empty() ) );
        }
        finally
        {
            database.shutdown();
        }
    }

    @Test
    void configuredDatabasesRootPath()
    {
        File factoryDir = testDirectory.databaseDir();
        File databasesDir = testDirectory.directory( "my_databases" );

        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( factoryDir )
                .setConfig( databases_root_path, databasesDir.toString() )
                .newGraphDatabase();
        try
        {
            assertTrue( isEmptyOrNonExistingDirectory( fs, new File( factoryDir.getParent(), DEFAULT_DATABASE_NAME ) ) );
            assertTrue( isEmptyOrNonExistingDirectory( fs, new File( factoryDir.getParent(), SYSTEM_DATABASE_NAME ) ) );

            assertFalse( isEmptyOrNonExistingDirectory( fs, new File( databasesDir, DEFAULT_DATABASE_NAME ) ) );
            assertFalse( isEmptyOrNonExistingDirectory( fs, new File( databasesDir, SYSTEM_DATABASE_NAME ) ) );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    void notConfiguredDatabasesRootPath()
    {
        File factoryDir = testDirectory.databaseDir();

        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( factoryDir );
        try
        {
            assertFalse( isEmptyOrNonExistingDirectory( fs, new File( factoryDir.getParent(), DEFAULT_DATABASE_NAME ) ) );
            assertFalse( isEmptyOrNonExistingDirectory( fs, new File( factoryDir.getParent(), SYSTEM_DATABASE_NAME ) ) );
        }
        finally
        {
            db.shutdown();
        }
    }
}
