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
package org.neo4j.bolt.txtracking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import org.neo4j.collection.Dependencies;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.time.Clocks;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseNotFound;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseUnavailable;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.BookmarkTimeout;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

class TransactionIdTrackerTest
{
    private static final Duration DEFAULT_DURATION = ofSeconds( 10 );

    private final TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
    private final DatabaseAvailabilityGuard databaseAvailabilityGuard = mock( DatabaseAvailabilityGuard.class );
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final NamedDatabaseId namedDatabaseId = databaseIdRepository.getRaw( "foo" );
    private final Database db = mock( Database.class );
    private final DatabaseManagementService managementService = mock( DatabaseManagementService.class );
    private final ReconciledTransactionTracker reconciledTransactionTracker = spy( new DefaultReconciledTransactionTracker( NullLogService.getInstance() ) );

    private TransactionIdTracker transactionIdTracker;

    @BeforeEach
    void setup()
    {
        var dbApi = mock( GraphDatabaseAPI.class );
        var resolver = mock( Dependencies.class );

        when( managementService.database( namedDatabaseId.name() ) ).thenReturn( dbApi );
        when( dbApi.getDependencyResolver() ).thenReturn( resolver );

        when( db.getNamedDatabaseId() ).thenReturn( namedDatabaseId );
        when( db.isSystem() ).thenReturn( false );
        when( db.getDependencyResolver() ).thenReturn( resolver );
        when( db.getDatabaseAvailabilityGuard() ).thenReturn( databaseAvailabilityGuard );

        when( resolver.resolveDependency( Database.class ) ).thenReturn( db );
        when( resolver.resolveDependency( TransactionIdStore.class ) ).thenReturn( transactionIdStore );

        when( databaseAvailabilityGuard.isAvailable() ).thenReturn( true );
        transactionIdTracker = new TransactionIdTracker( managementService, reconciledTransactionTracker, new Monitors(), Clocks.fakeClock() );
    }

    @Test
    void shouldReturnImmediatelyForBaseTxIdOrLess() throws Exception
    {
        // when
        transactionIdTracker.awaitUpToDate( namedDatabaseId, BASE_TX_ID, ofSeconds( 5 ) );

        // then
        verify( transactionIdStore, never() ).getLastClosedTransactionId();
        verify( reconciledTransactionTracker, never() ).getLastReconciledTransactionId();
    }

    @Test
    void shouldReturnImmediatelyForBaseTxIdOrLessUsingSystemDb() throws Exception
    {
        // given
        when( db.isSystem() ).thenReturn( true );

        // when
        transactionIdTracker.awaitUpToDate( namedDatabaseId, BASE_TX_ID, ofSeconds( 5 ) );

        // then
        verify( reconciledTransactionTracker, never() ).getLastReconciledTransactionId();
        verifyZeroInteractions( transactionIdStore );
    }

    @Test
    void shouldWaitForRequestedVersion() throws Exception
    {
        // given
        var version = 5L;

        when( transactionIdStore.getLastClosedTransactionId() ).thenReturn( 1L ).thenReturn( 2L ).thenReturn( 6L );

        // when
        transactionIdTracker.awaitUpToDate( namedDatabaseId, version, DEFAULT_DURATION );

        // then
        verify( transactionIdStore, times( 3 ) ).getLastClosedTransactionId();
    }

    @Test
    void shouldWaitForRequestedVersionUsingSystemDb() throws Exception
    {
        // given
        when( db.isSystem() ).thenReturn( true );
        var version = 42L;

        when( reconciledTransactionTracker.getLastReconciledTransactionId() ).thenReturn( 1L ).thenReturn( 2L ).thenReturn( 41L ).thenReturn( 44L );

        // when
        transactionIdTracker.awaitUpToDate( namedDatabaseId, version, DEFAULT_DURATION );

        // then
        verify( reconciledTransactionTracker, times( 4 ) ).getLastReconciledTransactionId();
        verifyZeroInteractions( transactionIdStore );
    }

    @Test
    void shouldWrapAnyStoreCheckExceptions()
    {
        // given
        var version = 5L;
        var checkException = new RuntimeException();
        doThrow( checkException ).when( transactionIdStore ).getLastClosedTransactionId();

        // when
        var exception = assertThrows( TransactionIdTrackerException.class,
                () -> transactionIdTracker.awaitUpToDate( namedDatabaseId, version + 1, ofMillis( 50 ) ) );

        // then
        assertEquals( BookmarkTimeout, exception.status() );
        assertEquals( checkException, exception.getCause() );
    }

    @Test
    void shouldWrapAnyStoreCheckExceptionsUsingSystemDb()
    {
        // given
        when( db.isSystem() ).thenReturn( true );
        var version = 3L;
        var checkException = new RuntimeException();
        doThrow( checkException ).when( reconciledTransactionTracker ).getLastReconciledTransactionId();

        // when
        var exception = assertThrows( TransactionIdTrackerException.class,
                () -> transactionIdTracker.awaitUpToDate( namedDatabaseId, version + 1, ofMillis( 50 ) ) );

        // then
        assertEquals( BookmarkTimeout, exception.status() );
        assertEquals( checkException, exception.getCause() );
        verifyZeroInteractions( transactionIdStore );
    }

    @Test
    void shouldThrowDatabaseIsShutdownWhenStoreShutdownAfterCheck()
    {
        // given
        var version = 5L;
        var checkException = new RuntimeException();
        doThrow( checkException ).when( transactionIdStore ).getLastClosedTransactionId();
        when( databaseAvailabilityGuard.isAvailable() ).thenReturn( true, true, false );

        // when
        var exception = assertThrows( TransactionIdTrackerException.class,
                () -> transactionIdTracker.awaitUpToDate( namedDatabaseId, version + 1, ofMillis( 50 ) ) );

        // then
        assertEquals( DatabaseUnavailable, exception.status() );
        assertEquals( checkException, exception.getCause() );
    }

    @Test
    void shouldThrowDatabaseIsShutdownWhenStoreShutdownAfterCheckUsingSystemDb()
    {
        // given
        when( db.isSystem() ).thenReturn( true );
        var version = 42L;
        var checkException = new RuntimeException();
        doThrow( checkException ).when( reconciledTransactionTracker ).getLastReconciledTransactionId();
        when( databaseAvailabilityGuard.isAvailable() ).thenReturn( true, true, false );

        // when
        var exception = assertThrows( TransactionIdTrackerException.class,
                () -> transactionIdTracker.awaitUpToDate( namedDatabaseId, version + 1, ofMillis( 50 ) ) );

        // then
        assertEquals( DatabaseUnavailable, exception.status() );
        assertEquals( checkException, exception.getCause() );
        verifyZeroInteractions( transactionIdStore );
    }

    @Test
    void shouldNotWaitIfTheDatabaseIsUnavailable()
    {
        // given
        when( databaseAvailabilityGuard.isAvailable() ).thenReturn( false );

        // when
        var exception = assertThrows( TransactionIdTrackerException.class,
                () -> transactionIdTracker.awaitUpToDate( namedDatabaseId, 1000, ofMillis( 60_000 ) ) );

        // then
        assertEquals( DatabaseUnavailable, exception.status() );
        verify( transactionIdStore, never() ).getLastClosedTransactionId();
    }

    @Test
    void shouldNotWaitIfTheSystemDatabaseIsUnavailable()
    {
        // given
        when( db.isSystem() ).thenReturn( true );
        when( databaseAvailabilityGuard.isAvailable() ).thenReturn( false );

        // when
        var exception = assertThrows( TransactionIdTrackerException.class,
                () -> transactionIdTracker.awaitUpToDate( namedDatabaseId, 1000, ofMillis( 60_000 ) ) );

        // then
        assertEquals( DatabaseUnavailable, exception.status() );
        verify( reconciledTransactionTracker, never() ).getLastReconciledTransactionId();
        verifyZeroInteractions( transactionIdStore );
    }

    @Test
    void shouldReturnNewestTransactionId()
    {
        // given
        when( transactionIdStore.getLastClosedTransactionId() ).thenReturn( 42L );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 4242L );

        // then
        assertEquals( 4242L, transactionIdTracker.newestTransactionId( namedDatabaseId ) );
    }

    @Test
    void shouldReturnNewestTransactionIdUsingSystemDb()
    {
        // given
        when( db.isSystem() ).thenReturn( true );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 42L );
        when( reconciledTransactionTracker.getLastReconciledTransactionId() ).thenReturn( 4242L );

        // then
        assertEquals( 42L, transactionIdTracker.newestTransactionId( namedDatabaseId ) );
        verifyZeroInteractions( reconciledTransactionTracker );
    }

    @Test
    void shouldNotReturnNewestTransactionIdForDatabaseThatDoesNotExist()
    {
        // given
        var unknownDatabaseId = databaseIdRepository.getRaw( "bar" );
        when( managementService.database( unknownDatabaseId.name() ) ).thenThrow( DatabaseNotFoundException.class );

        // when
        var exception = assertThrows( TransactionIdTrackerException.class,
                () -> transactionIdTracker.newestTransactionId( unknownDatabaseId ) );

        // then
        assertEquals( DatabaseNotFound, exception.status() );
    }

    @Test
    void shouldNotAwaitForTransactionForDatabaseThatDoesNotExist()
    {
        // given
        var unknownDatabaseId = databaseIdRepository.getRaw( "bar" );
        when( managementService.database( unknownDatabaseId.name() ) ).thenThrow( DatabaseNotFoundException.class );

        // when
        var exception = assertThrows( TransactionIdTrackerException.class,
                () -> transactionIdTracker.awaitUpToDate( unknownDatabaseId, 1, ofMillis( 1 ) ) );

        // then
        assertEquals( DatabaseNotFound, exception.status() );
    }
}
