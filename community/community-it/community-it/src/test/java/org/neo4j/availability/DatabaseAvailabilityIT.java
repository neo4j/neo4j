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
package org.neo4j.availability;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityRequirement;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailability;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

@TestDirectoryExtension
class DatabaseAvailabilityIT
{
    private DatabaseId defaultDatabaseId;

    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseAPI database;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        managementService = new DatabaseManagementServiceBuilder( testDirectory.storeDir() ).build();
        database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        var databaseManager = database.getDependencyResolver().resolveDependency( DatabaseManager.class );
        defaultDatabaseId = databaseManager.databaseIdRepository().get( DEFAULT_DATABASE_NAME ).get();
    }

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void anyOfDatabaseUnavailabilityIsGlobalUnavailability()
    {
        AvailabilityRequirement outerSpaceRequirement = () -> "outer space";
        DependencyResolver dependencyResolver = database.getDependencyResolver();
        DatabaseManager<?> databaseManager = getDatabaseManager( dependencyResolver );
        CompositeDatabaseAvailabilityGuard compositeGuard = dependencyResolver.resolveDependency( CompositeDatabaseAvailabilityGuard.class );
        assertTrue( compositeGuard.isAvailable() );

        DatabaseContext systemContext = databaseManager.getDatabaseContext( SYSTEM_DATABASE_ID ).get();
        DatabaseContext defaultContext = databaseManager.getDatabaseContext( defaultDatabaseId ).get();

        AvailabilityGuard systemGuard = systemContext.dependencies().resolveDependency( DatabaseAvailabilityGuard.class );
        systemGuard.require( outerSpaceRequirement );
        assertFalse( compositeGuard.isAvailable() );

        systemGuard.fulfill( outerSpaceRequirement );
        assertTrue( compositeGuard.isAvailable() );

        AvailabilityGuard defaultGuard = defaultContext.dependencies().resolveDependency( DatabaseAvailabilityGuard.class );
        defaultGuard.require( outerSpaceRequirement );
        assertFalse( compositeGuard.isAvailable() );

        defaultGuard.fulfill( outerSpaceRequirement );
        assertTrue( compositeGuard.isAvailable() );
    }

    @Test
    void stoppedDatabaseIsNotAvailable()
    {
        DependencyResolver dependencyResolver = database.getDependencyResolver();
        DatabaseManager<?> databaseManager = getDatabaseManager( dependencyResolver );
        DatabaseContext databaseContext = databaseManager.getDatabaseContext( defaultDatabaseId ).get();
        databaseContext.database().stop();

        assertThrows( DatabaseShutdownException.class, () -> database.beginTx() );
    }

    @Test
    void notConfusingMessageOnDatabaseNonAvailability()
    {
        DependencyResolver dependencyResolver = database.getDependencyResolver();
        DatabaseManager<?> databaseManager = getDatabaseManager( dependencyResolver );
        DatabaseContext databaseContext = databaseManager.getDatabaseContext( defaultDatabaseId ).get();
        DatabaseAvailability databaseAvailability = databaseContext.database().getDependencyResolver().resolveDependency( DatabaseAvailability.class );
        databaseAvailability.stop();

        TransactionFailureException exception = assertThrows( TransactionFailureException.class, () -> database.beginTx() );
        assertEquals( "Timeout waiting for database to become available and allow new transactions. Waited 1s. 1 reasons for blocking: Database unavailable.",
                exception.getMessage() );
    }

    @Test
    void restartedDatabaseIsAvailable()
    {
        DependencyResolver dependencyResolver = database.getDependencyResolver();
        DatabaseManager<?> databaseManager = getDatabaseManager( dependencyResolver );
        DatabaseContext databaseContext = databaseManager.getDatabaseContext( defaultDatabaseId ).get();
        Database database = databaseContext.database();

        executeTransactionOnDefaultDatabase();

        database.stop();
        assertThrows( DatabaseShutdownException.class, () -> this.database.beginTx() );

        database.start();
        executeTransactionOnDefaultDatabase();
    }

    private void executeTransactionOnDefaultDatabase()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.commit();
        }
    }

    private static DatabaseManager<?> getDatabaseManager( DependencyResolver dependencyResolver )
    {
        return dependencyResolver.resolveDependency( DatabaseManager.class );
    }
}
