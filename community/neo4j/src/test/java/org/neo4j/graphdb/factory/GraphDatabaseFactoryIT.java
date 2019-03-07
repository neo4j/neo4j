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

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;

@ExtendWith( TestDirectoryExtension.class )
class GraphDatabaseFactoryIT
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void startSystemAndDefaultDatabase()
    {
        GraphDatabaseAPI database = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( testDirectory.databaseDir() );
        try
        {
            DependencyResolver dependencyResolver = database.getDependencyResolver();
            DatabaseManager databaseManager = dependencyResolver.resolveDependency( DatabaseManager.class );
            Config config = dependencyResolver.resolveDependency( Config.class );
            assertTrue( databaseManager.getDatabaseContext( config.get( default_database ) ).isPresent() );
            assertTrue( databaseManager.getDatabaseContext( SYSTEM_DATABASE_NAME ).isPresent() );
        }
        finally
        {
            database.shutdown();
        }
    }
}
