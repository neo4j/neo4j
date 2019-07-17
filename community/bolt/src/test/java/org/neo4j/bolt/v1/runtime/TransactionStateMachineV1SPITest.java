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
package org.neo4j.bolt.v1.runtime;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.impl.BoltKernelGraphDatabaseServiceProvider;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.v1.runtime.bookmarking.BookmarkWithPrefix;
import org.neo4j.collection.Dependencies;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLog;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;
import org.neo4j.time.SystemNanoClock;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.NamedThreadFactory.daemon;

@Timeout( 60 )
class TransactionStateMachineV1SPITest
{
    private static final DatabaseId DATABASE_ID = new TestDatabaseIdRepository().defaultDatabase();

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
        Supplier<TransactionIdStore> txIdStore = () -> fixedTxIdStore( lastClosedTransactionId );
        var txAwaitDuration = Duration.ofSeconds( 42 );
        var clock = new FakeClock();

        var guard = new DatabaseAvailabilityGuard( DATABASE_ID,
                clock,
                NullLog.getInstance(), 0,
                mock( CompositeDatabaseAvailabilityGuard.class ) );
        var databaseAvailabilityGuard = spy( guard );
        when( databaseAvailabilityGuard.isAvailable() ).then( invocation ->
        {
            // move clock forward on the first availability check
            // this check is executed on every tx id polling iteration
            var available = (boolean) invocation.callRealMethod();
            clock.forward( txAwaitDuration.getSeconds() + 1, SECONDS );
            return available;
        } );

        var txSpi = createTxSpi( txIdStore, txAwaitDuration, databaseAvailabilityGuard, clock );

        var resultFuture = executor.submit( () ->
        {
            txSpi.awaitUpToDate( List.of( new BookmarkWithPrefix( lastClosedTransactionId + 42 ) ) );
            return null;
        } );

        try
        {
            resultFuture.get( 20, SECONDS );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ExecutionException.class ) );
            assertThat( e.getCause(), instanceOf( TransactionFailureException.class ) );
        }
    }

    @Test
    void doesNotWaitWhenTxIdUpToDate() throws Exception
    {
        long lastClosedTransactionId = 100;
        Supplier<TransactionIdStore> txIdStore = () -> fixedTxIdStore( lastClosedTransactionId );

        var txSpi = createTxSpi( txIdStore, Duration.ofSeconds( 1 ), Clocks.fakeClock() );

        var resultFuture = executor.submit( () ->
        {
            txSpi.awaitUpToDate( List.of( new BookmarkWithPrefix( lastClosedTransactionId - 42 ) ) );
            return null;
        } );

        assertNull( resultFuture.get( 20, SECONDS ) );
    }

    @Test
    void shouldNotCheckDatabaseIdInBookmark() throws Throwable
    {
        // Given
        var dbSpi = mock( BoltGraphDatabaseServiceSPI.class );
        var bookmarkAwaitDuration = Duration.ofMinutes( 10 );
        var spi = new TransactionStateMachineV1SPI( dbSpi, mock( BoltChannel.class ), bookmarkAwaitDuration, mock( SystemNanoClock.class ),
                mock( StatementProcessorReleaseManager.class ) );

        var bookmarks = List.<Bookmark>of( new BookmarkWithPrefix( 42 ) );

        // When
        spi.awaitUpToDate( bookmarks );

        // Then
        verify( dbSpi ).awaitUpToDate( bookmarks, bookmarkAwaitDuration );
    }

    @Test
    void shouldReturnBookmarkWithPrefix()
    {
        // Given
        var dbSpi = mock( BoltGraphDatabaseServiceSPI.class );
        when( dbSpi.newestEncounteredTxId() ).thenReturn( 42L );
        var txDuration = Duration.ofMinutes( 10 );
        var spi = new TransactionStateMachineV1SPI( dbSpi, mock( BoltChannel.class ), txDuration, mock( SystemNanoClock.class ),
                mock( StatementProcessorReleaseManager.class ) );

        // When
        var bookmark = spi.newestBookmark();

        // Then
        verify( dbSpi ).newestEncounteredTxId();
        assertThat( bookmark, instanceOf( BookmarkWithPrefix.class ) );
        assertThat( bookmark.txId(), equalTo( 42L ) );
    }

    @Test
    void shouldFailWhenGivenMultipleBookmarks()
    {
        var dbSpi = mock( BoltGraphDatabaseServiceSPI.class );
        when( dbSpi.newestEncounteredTxId() ).thenReturn( 42L );
        var txDuration = Duration.ofMinutes( 10 );
        var spi = new TransactionStateMachineV1SPI( dbSpi, mock( BoltChannel.class ), txDuration, mock( SystemNanoClock.class ),
                mock( StatementProcessorReleaseManager.class ) );

        var bookmarks = List.<Bookmark>of( new BookmarkWithPrefix( 42 ), new BookmarkWithPrefix( 4242 ) );

        assertThrows( IllegalArgumentException.class, () -> spi.awaitUpToDate( bookmarks ) );
    }

    private static TransactionIdStore fixedTxIdStore( long lastClosedTransactionId )
    {
        var txIdStore = mock( TransactionIdStore.class );
        when( txIdStore.getLastClosedTransactionId() ).thenReturn( lastClosedTransactionId )
                .thenThrow( new RuntimeException( "More then one check is an indication of polling" ) );
        return txIdStore;
    }

    private static TransactionStateMachineV1SPI createTxSpi( Supplier<TransactionIdStore> txIdStore, Duration txAwaitDuration, SystemNanoClock clock )
            throws Exception
    {
        var compositeGuard = mock( CompositeDatabaseAvailabilityGuard.class );
        var databaseAvailabilityGuard = new DatabaseAvailabilityGuard( DATABASE_ID, clock, NullLog.getInstance(), 0, compositeGuard );
        databaseAvailabilityGuard.init();
        databaseAvailabilityGuard.start();
        return createTxSpi( txIdStore, txAwaitDuration, databaseAvailabilityGuard, clock );
    }

    private static TransactionStateMachineV1SPI createTxSpi( Supplier<TransactionIdStore> txIdStore, Duration txAwaitDuration,
            DatabaseAvailabilityGuard availabilityGuard, SystemNanoClock clock )
    {
        var queryExecutionEngine = mock( QueryExecutionEngine.class );

        var dependencyResolver = mock( Dependencies.class );
        var bridge = new ThreadToStatementContextBridge();
        when( dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class ) ).thenReturn( bridge );
        when( dependencyResolver.resolveDependency( QueryExecutionEngine.class ) ).thenReturn( queryExecutionEngine );
        when( dependencyResolver.resolveDependency( DatabaseAvailabilityGuard.class ) ).thenReturn( availabilityGuard );
        when( dependencyResolver.provideDependency( TransactionIdStore.class ) ).thenReturn( txIdStore );
        when( dependencyResolver.resolveDependency( Database.class ) ).thenReturn( mock( Database.class ) );

        var facade = mock( GraphDatabaseAPI.class );
        when( facade.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( facade.isAvailable( anyLong() ) ).thenReturn( true );

        var queryService = mock( GraphDatabaseQueryService.class );
        when( queryService.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( dependencyResolver.resolveDependency( GraphDatabaseQueryService.class ) ).thenReturn( queryService );

        var boltChannel = new BoltChannel( "bolt-42", "bolt", new EmbeddedChannel() );

        var databaseServiceProvider = new BoltKernelGraphDatabaseServiceProvider( facade, clock );
        return new TransactionStateMachineV1SPI( databaseServiceProvider, boltChannel, txAwaitDuration, clock, mock( StatementProcessorReleaseManager.class ) );
    }
}
