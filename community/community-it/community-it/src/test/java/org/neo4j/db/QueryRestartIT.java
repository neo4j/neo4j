/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.db;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.neo4j.collection.Dependencies;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.snapshot.TestTransactionVersionContextSupplier;
import org.neo4j.snapshot.TestVersionContext;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.snapshot_query;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.index_background_sampling_enabled;

@TestDirectoryExtension
class QueryRestartIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private TestTransactionVersionContextSupplier testContextSupplier;
    private Path storeDir;
    private TestVersionContext testCursorContext;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp() throws IOException
    {
        storeDir = testDirectory.homePath();
        testContextSupplier = new TestTransactionVersionContextSupplier();
        database = startSnapshotQueryDb();
        createData();
        // Checkpoint to make the counts store flush its changes map so that it will need to read on next query
        checkpoint();

        testCursorContext = TestVersionContext.testCursorContext( managementService, DEFAULT_DATABASE_NAME );
        testContextSupplier.setCursorContext( testCursorContext );
    }

    private void checkpoint() throws IOException
    {
        ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint( new SimpleTriggerInfo( "Test" ) );
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
    void executeQueryWithoutRestarts()
    {
        testCursorContext.setWrongLastClosedTxId( false );

        try ( Transaction transaction = database.beginTx() )
        {
            Result result = transaction.execute( "MATCH (n:label) RETURN n.c" );
            while ( result.hasNext() )
            {
                assertEquals( "d", result.next().get( "n.c" ) );
            }
            // This extra printing is here to investigate flakiness of this test
            if ( testCursorContext.getAdditionalAttempts() > 0 )
            {
                System.err.println( "Unexpected call to markAsDirty/isDirty:" );
                testCursorContext.printDirtyCalls( System.err );
            }
            assertEquals( 0, testCursorContext.getAdditionalAttempts() );
            transaction.commit();
        }
    }

    @Test
    void executeQueryWithSingleRetry()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Result result = transaction.execute( "MATCH (n) RETURN n.c" );
            assertThat( testCursorContext.getAdditionalAttempts() ).isNotZero();
            while ( result.hasNext() )
            {
                assertEquals( "d", result.next().get( "n.c" ) );
            }
            transaction.commit();
        }
    }

    @Test
    void executeCountStoreQueryWithSingleRetry()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Result result = transaction.execute( "MATCH (n:toRetry) RETURN count(n)" );
            assertThat( testCursorContext.getAdditionalAttempts() ).isNotZero();
            while ( result.hasNext() )
            {
                assertEquals( 1L, result.next().get( "count(n)" ) );
            }
            transaction.commit();
        }
    }

    @Test
    void executeLabelScanQueryWithSingleRetry()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Result result = transaction.execute( "MATCH (n:toRetry) RETURN n.c" );
            assertThat( testCursorContext.getAdditionalAttempts() ).isNotZero();
            while ( result.hasNext() )
            {
                assertEquals( "d", result.next().get( "n.c" ) );
            }
            transaction.commit();
        }
    }

    @Test
    void queryThatModifiesDataAndSeesUnstableSnapshotShouldThrowException()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            QueryExecutionException e = assertThrows( QueryExecutionException.class, () -> transaction.execute( "MATCH (n:toRetry) CREATE () RETURN n.c" ) );
            assertEquals( "Unable to get clean data snapshot for query " + "'MATCH (n:toRetry) CREATE () RETURN n.c' that performs updates.", e.getMessage() );
            transaction.commit();
        }
    }

    private GraphDatabaseService startSnapshotQueryDb()
    {
        // Inject TransactionVersionContextSupplier
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( testContextSupplier );
        dependencies.satisfyDependency( new LimitedJobScheduler() );

        managementService = new TestDatabaseManagementServiceBuilder( storeDir )
                .setExternalDependencies( dependencies )
                .setConfig( snapshot_query, true )
                .setConfig( index_background_sampling_enabled, false )
                .build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private void createData()
    {
        Label label = Label.label( "toRetry" );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode( label );
            node.setProperty( "c", "d" );
            transaction.commit();
        }
    }

    private static class LimitedJobScheduler extends CentralJobScheduler
    {
        protected LimitedJobScheduler()
        {
            super( Clocks.nanoClock() );
        }

        @Override
        public JobHandle<?> scheduleRecurring( Group group, JobMonitoringParams monitoredJobParams, Runnable runnable, long period, TimeUnit timeUnit )
        {
            if ( isRestricted( group ) )
            {
                return JobHandle.EMPTY;
            }
            return super.scheduleRecurring( group, monitoredJobParams, runnable, period, timeUnit );
        }

        private boolean isRestricted( Group group )
        {
            return Group.STORAGE_MAINTENANCE == group;
        }
    }
}
