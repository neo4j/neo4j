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
package org.neo4j.bolt.runtime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.impl.BoltKernelGraphDatabaseServiceProvider;
import org.neo4j.bolt.txtracking.DefaultReconciledTransactionTracker;
import org.neo4j.bolt.txtracking.TransactionIdTracker;
import org.neo4j.bolt.txtracking.TransactionIdTrackerException;
import org.neo4j.bolt.v3.runtime.bookmarking.BookmarkWithPrefix;
import org.neo4j.collection.Dependencies;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;
import org.neo4j.time.SystemNanoClock;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.NamedThreadFactory.daemon;

class DatabaseServiceBookmarkTest
{
    private static final NamedDatabaseId DATABASE_ID = new TestDatabaseIdRepository().defaultDatabase();

    private final ExecutorService executor = newSingleThreadExecutor( daemon( getClass() + "-thread" ) );

    @AfterEach
    void afterEach() throws Exception
    {
        executor.shutdownNow();
        assertTrue( executor.awaitTermination( 20, SECONDS ) );
    }

    @Test
    void throwsWhenTxAwaitDurationExpires()
    {
        long lastClosedTransactionId = 100;
        TransactionIdStore txIdStore = fixedTxIdStore( lastClosedTransactionId );
        var txAwaitDuration = Duration.ofSeconds( 42 );
        var clock = new FakeClock();

        var guard = new DatabaseAvailabilityGuard( DATABASE_ID,
                clock,
                NullLog.getInstance(), 0,
                mock( CompositeDatabaseAvailabilityGuard.class ) );
        var databaseAvailabilityGuard = spy( guard );
        when( databaseAvailabilityGuard.isAvailable() ).then( invocation ->
        {
            // move clock forward on avery availability check
            // this check is executed on every tx id polling iteration
            clock.forward( 1, SECONDS );
            return true;
        } );

        var dbSpi = createDbSpi( txIdStore, txAwaitDuration, databaseAvailabilityGuard, clock );

        var resultFuture = executor.submit( () ->
        {
            begin( dbSpi, List.of( new BookmarkWithPrefix( lastClosedTransactionId + 42 ) ) );
            return null;
        } );

        try
        {
            resultFuture.get( 20, SECONDS );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ExecutionException.class ) );
            assertThat( e.getCause(), instanceOf( TransactionIdTrackerException.class ) );
        }
    }

    @Test
    void doesNotWaitWhenTxIdUpToDate() throws Exception
    {
        long lastClosedTransactionId = 100;
        TransactionIdStore txIdStore = fixedTxIdStore( lastClosedTransactionId );

        var dbSpi = createDbSpi( txIdStore, Duration.ofSeconds( 1 ), Clocks.fakeClock() );

        var resultFuture = executor.submit( () ->
        {
            begin( dbSpi, List.of( new BookmarkWithPrefix( lastClosedTransactionId - 42 ) ) );
            return null;
        } );

        assertNull( resultFuture.get( 20, SECONDS ) );
    }

    private BoltGraphDatabaseServiceSPI createDbSpi( TransactionIdStore txIdStore, Duration txAwaitDuration, SystemNanoClock clock )
            throws Exception
    {
        var compositeGuard = mock( CompositeDatabaseAvailabilityGuard.class );
        var databaseAvailabilityGuard = new DatabaseAvailabilityGuard( DATABASE_ID, clock, NullLog.getInstance(), 0, compositeGuard );
        databaseAvailabilityGuard.init();
        databaseAvailabilityGuard.start();
        return createDbSpi( txIdStore, txAwaitDuration, databaseAvailabilityGuard, clock );
    }

    private BoltGraphDatabaseServiceSPI createDbSpi( TransactionIdStore txIdStore, Duration txAwaitDuration,
            DatabaseAvailabilityGuard availabilityGuard, SystemNanoClock clock )
    {
        var queryExecutionEngine = mock( QueryExecutionEngine.class );

        var db = mock( Database.class );
        when( db.getNamedDatabaseId() ).thenReturn( DATABASE_ID );
        when( db.getDatabaseAvailabilityGuard() ).thenReturn( availabilityGuard );

        var dependencyResolver = mock( Dependencies.class );
        when( dependencyResolver.resolveDependency( QueryExecutionEngine.class ) ).thenReturn( queryExecutionEngine );
        when( dependencyResolver.resolveDependency( DatabaseAvailabilityGuard.class ) ).thenReturn( availabilityGuard );
        when( dependencyResolver.resolveDependency( TransactionIdStore.class ) ).thenReturn( txIdStore );
        when( dependencyResolver.resolveDependency( Database.class ) ).thenReturn( db );

        when( db.getDependencyResolver() ).thenReturn( dependencyResolver );

        var facade = mock( GraphDatabaseAPI.class );
        when( facade.getDependencyResolver() ).thenReturn( dependencyResolver );

        var tx = mock( InternalTransaction.class );
        when( facade.beginTransaction( any(), any(), any() ) ).thenReturn( tx );

        var queryService = mock( GraphDatabaseQueryService.class );
        when( queryService.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( dependencyResolver.resolveDependency( GraphDatabaseQueryService.class ) ).thenReturn( queryService );

        var managementService = mock( DatabaseManagementService.class );
        when( managementService.database( DATABASE_ID.name() ) ).thenReturn( facade );

        var reconciledTxTracker = new DefaultReconciledTransactionTracker( NullLogService.getInstance() );
        var transactionIdTracker = new TransactionIdTracker( managementService, reconciledTxTracker, new Monitors(), clock );
        return new BoltKernelGraphDatabaseServiceProvider( facade, transactionIdTracker, txAwaitDuration );
    }

    private void begin( BoltGraphDatabaseServiceSPI dbSpi, List<Bookmark> bookmarks )
    {
        dbSpi.beginTransaction( null, null, null, bookmarks, null, null, null );
    }

    private static TransactionIdStore fixedTxIdStore( long lastClosedTransactionId )
    {
        var txIdStore = mock( TransactionIdStore.class );
        when( txIdStore.getLastClosedTransactionId() ).thenReturn( lastClosedTransactionId );
        return txIdStore;
    }
}
