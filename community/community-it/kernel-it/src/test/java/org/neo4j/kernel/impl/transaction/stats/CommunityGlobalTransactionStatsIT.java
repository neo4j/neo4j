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
package org.neo4j.kernel.impl.transaction.stats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

@TestDirectoryExtension
class CommunityGlobalTransactionStatsIT
{

    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() ).build();
        database = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void tearDown()
    {
        if ( database != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void useAggregatedTransactionMonitorForSystemAndDefaultDatabase() throws InterruptedException
    {
        ExecutorService transactionExecutor = Executors.newSingleThreadExecutor();
        DatabaseManager<?> databaseManager = getDatabaseManager();
        var defaultDatabase = databaseManager.getDatabaseContext( DEFAULT_DATABASE_NAME );
        var systemDatabase = databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID );

        assertTrue( defaultDatabase.isPresent() );
        assertTrue( systemDatabase.isPresent() );

        GlobalTransactionStats globalTransactionStats = ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( GlobalTransactionStats.class );
        assertEquals( 0, globalTransactionStats.getNumberOfActiveTransactions() );
        CountDownLatch startSeparateTransaction = new CountDownLatch( 1 );
        try
        {
            GraphDatabaseFacade systemFacade = systemDatabase.get().databaseFacade();
            GraphDatabaseFacade defaultFacade = defaultDatabase.get().databaseFacade();
            transactionExecutor.execute( () ->
            {
                systemFacade.beginTx();
                startSeparateTransaction.countDown();
            } );
            startSeparateTransaction.await();
            assertEquals( 1, globalTransactionStats.getNumberOfActiveTransactions() );

            try ( Transaction tx = defaultFacade.beginTx() )
            {
                TransactionCounters databaseStats = ((GraphDatabaseAPI) defaultFacade).getDependencyResolver().resolveDependency( TransactionCounters.class );
                assertEquals( 2, globalTransactionStats.getNumberOfActiveTransactions() );
                assertEquals( 1, databaseStats.getNumberOfActiveTransactions() );
            }
        }
        finally
        {
            transactionExecutor.shutdown();
        }
    }

    private DatabaseManager<?> getDatabaseManager()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

}
