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
package org.neo4j.test;

import org.junit.jupiter.api.Test;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

@TestDirectoryExtension
class TestDatabaseManagementServiceBuilderTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void databaseStartsWithSystemAndDefaultDatabase()
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() ).build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            checkAvailableDatabases( database );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void impermanentDatabaseStartsWithSystemAndDefaultDatabase()
    {
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() ).impermanent().build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            checkAvailableDatabases( database );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private static void checkAvailableDatabases( GraphDatabaseAPI database )
    {
        DependencyResolver resolver = database.getDependencyResolver();
        DatabaseManager<?> databaseManager = resolver.resolveDependency( DatabaseManager.class );

        assertTrue( databaseManager.getDatabaseContext( SYSTEM_DATABASE_ID ).isPresent() );
        assertTrue( databaseManager.getDatabaseContext( DEFAULT_DATABASE_NAME ).isPresent() );
    }
}
