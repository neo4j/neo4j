package org.neo4j.kernel.impl.transaction.stats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@ExtendWith( TestDirectoryExtension.class )
class CommunityGlobalTransactionStatsIT
{

    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;

    @BeforeEach
    void setUp()
    {
        database = new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.databaseDir() );
    }

    @AfterEach
    void tearDown()
    {
        database.shutdown();
    }

    @Test
    void useAggregatedTransactionMonitorForSystemAndDefaultDatabase() throws InterruptedException
    {
        ExecutorService transactionExecutor = Executors.newSingleThreadExecutor();
        DatabaseManager databaseManager = getDatabaseManager();
        Optional<DatabaseContext> defaultDatabase = databaseManager.getDatabaseContext( DEFAULT_DATABASE_NAME );
        Optional<DatabaseContext> systemDatabase = databaseManager.getDatabaseContext( SYSTEM_DATABASE_NAME );

        assertTrue( defaultDatabase.isPresent() );
        assertTrue( systemDatabase.isPresent() );

        GlobalTransactionStats globalTransactionStats = ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( GlobalTransactionStats.class );
        assertEquals( 0, globalTransactionStats.getNumberOfActiveTransactions() );
        CountDownLatch startSeparateTransaction = new CountDownLatch( 1 );
        try
        {
            GraphDatabaseFacade systemFacade = systemDatabase.get().getDatabaseFacade();
            GraphDatabaseFacade defaultFacade = defaultDatabase.get().getDatabaseFacade();
            transactionExecutor.execute( () ->
            {
                systemFacade.beginTx();
                startSeparateTransaction.countDown();
            } );
            startSeparateTransaction.await();
            assertEquals( 1, globalTransactionStats.getNumberOfActiveTransactions() );

            try ( Transaction ignored = defaultFacade.beginTx() )
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

    private DatabaseManager getDatabaseManager()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

}