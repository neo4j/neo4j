/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha.lock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.stubbing.OngoingStubbing;

import org.neo4j.com.ComException;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionObligationResponse;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.graphdb.TransientDatabaseFailureException;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.locking.LockClientStoppedException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.storageengine.api.lock.ResourceType;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.com.ResourceReleaser.NO_OP;
import static org.neo4j.com.TransactionStream.EMPTY;
import static org.neo4j.kernel.api.exceptions.Status.General.DatabaseUnavailable;
import static org.neo4j.kernel.configuration.Config.defaults;
import static org.neo4j.kernel.ha.lock.LockStatus.NOT_LOCKED;
import static org.neo4j.kernel.ha.lock.LockStatus.OK_LOCKED;
import static org.neo4j.kernel.impl.locking.LockTracer.NONE;
import static org.neo4j.kernel.impl.locking.Locks.Client;
import static org.neo4j.kernel.impl.locking.ResourceTypes.LABEL;
import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;
import static org.neo4j.kernel.impl.locking.ResourceTypes.RELATIONSHIP_TYPE;
import static org.neo4j.kernel.impl.store.StoreId.DEFAULT;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.logging.NullLog.getInstance;
import static org.neo4j.time.Clocks.fakeClock;
import static org.neo4j.time.Clocks.systemClock;

class SlaveLocksClientTest
{
    private Master master;
    private Locks lockManager;
    private Client local;
    private SlaveLocksClient client;
    private AvailabilityGuard availabilityGuard;
    private AssertableLogProvider logProvider;

    @BeforeEach
    void setUp()
    {
        master = mock( Master.class );
        availabilityGuard = new AvailabilityGuard( fakeClock(), getInstance() );

        lockManager = new CommunityLockManger( defaults(), systemClock() );
        local = spy( lockManager.newClient() );
        logProvider = new AssertableLogProvider();

        LockResult lockResultOk = new LockResult( OK_LOCKED );
        TransactionStreamResponse<LockResult> responseOk =
                new TransactionStreamResponse<>( lockResultOk, null, EMPTY, NO_OP );

        whenMasterAcquireShared().thenReturn( responseOk );

        whenMasterAcquireExclusive().thenReturn( responseOk );

        client = new SlaveLocksClient( master, local, lockManager, mock( RequestContextFactory.class ),
                availabilityGuard, logProvider );
    }

    private OngoingStubbing<Response<LockResult>> whenMasterAcquireShared()
    {
        return when( master.acquireSharedLock( isNull(), any( ResourceType.class ), ArgumentMatchers.<long[]>any() ) );
    }

    private OngoingStubbing<Response<LockResult>> whenMasterAcquireExclusive()
    {
        return when( master.acquireExclusiveLock( isNull(), any( ResourceType.class ), ArgumentMatchers.<long[]>any() ) );
    }

    @AfterEach
    void tearDown()
    {
        local.close();
    }

    @Test
    void shouldNotTakeSharedLockOnMasterIfWeAreAlreadyHoldingSaidLock()
    {
        // When taking a lock twice
        client.acquireShared( NONE, NODE, 1 );
        client.acquireShared( NONE, NODE, 1 );

        // Then only a single network round-trip should be observed
        verify( master ).acquireSharedLock( null, NODE, 1 );
    }

    @Test
    void shouldNotTakeExclusiveLockOnMasterIfWeAreAlreadyHoldingSaidLock()
    {
        // When taking a lock twice
        client.acquireExclusive( NONE, NODE, 1 );
        client.acquireExclusive( NONE, NODE, 1 );

        // Then only a single network roundtrip should be observed
        verify( master ).acquireExclusiveLock( null, NODE, 1 );
    }

    @Test
    void shouldAllowAcquiringReleasingAndReacquiringExclusive()
    {
        // Given we have grabbed and released a lock
        client.acquireExclusive( NONE, NODE, 1L );
        client.releaseExclusive( NODE, 1L );

        // When we grab and release that lock again
        client.acquireExclusive( NONE, NODE, 1L );
        client.releaseExclusive( NODE, 1L );

        // Then this should cause the local lock manager to hold the lock
        verify( local, times( 2 ) ).tryExclusiveLock( NODE, 1L );
        verify( local, times( 2 ) ).releaseExclusive( NODE, 1L );
    }

    @Test
    void shouldAllowAcquiringReleasingAndReacquiringShared()
    {
        // Given we have grabbed and released a lock
        client.acquireShared( NONE, NODE, 1L );
        client.releaseShared( NODE, 1L );

        // When we grab and release that lock again
        client.acquireShared( NONE, NODE, 1L );
        client.releaseShared( NODE, 1L );

        // Then this should cause the local lock manager to hold the lock
        verify( local, times( 2 ) ).trySharedLock( NODE, 1L );
        verify( local, times( 2 ) ).releaseShared( NODE, 1L );
    }

    @Test
    void shouldUseReEntryMethodsOnLocalLocksForReEntryExclusive()
    {
        // Given we have grabbed and released a lock
        client.acquireExclusive( NONE, NODE, 1L );

        // When we grab and release that lock again
        client.acquireExclusive( NONE, NODE, 1L );
        client.releaseExclusive( NODE, 1L );

        // Then this should cause the local lock manager to hold the lock
        InOrder order = inOrder( local );
        order.verify( local, times( 1 ) ).reEnterExclusive( NODE, 1L );
        order.verify( local, times( 1 ) ).tryExclusiveLock( NODE, 1L );
        order.verify( local, times( 1 ) ).reEnterExclusive( NODE, 1L );
        order.verify( local, times( 1 ) ).releaseExclusive( NODE, 1L );
        order.verifyNoMoreInteractions();
    }

    @Test
    void shouldUseReEntryMethodsOnLocalLocksForReEntryShared()
    {
        // Given we have grabbed and released a lock
        client.acquireShared( NONE, NODE, 1L );

        // When we grab and release that lock again
        client.acquireShared( NONE, NODE, 1L );
        client.releaseShared( NODE, 1L );

        // Then this should cause the local lock manager to hold the lock
        InOrder order = inOrder( local );
        order.verify( local, times( 1 ) ).reEnterShared( NODE, 1L );
        order.verify( local, times( 1 ) ).trySharedLock( NODE, 1L );
        order.verify( local, times( 1 ) ).reEnterShared( NODE, 1L );
        order.verify( local, times( 1 ) ).releaseShared( NODE, 1L );
        order.verifyNoMoreInteractions();
    }

    @Test
    void shouldReturnNoLockSessionIfNotInitialized()
    {
        // When
        int lockSessionId = client.getLockSessionId();

        // Then
        assertThat( lockSessionId, equalTo( -1 ) );
    }

    @Test
    void shouldReturnDelegateIdIfInitialized()
    {
        // Given
        client.acquireExclusive( NONE, NODE, 1L );

        // When
        int lockSessionId = client.getLockSessionId();

        // Then
        assertThat( lockSessionId, equalTo( local.getLockSessionId() ) );
    }

    @Test
    void mustThrowIfStartingNewLockSessionOnMasterThrowsComException()
    {
        assertThrows( DistributedLockFailureException.class, () -> {
            when( master.newLockSession( isNull() ) ).thenThrow( new ComException() );

            client.acquireShared( NONE, NODE, 1 );
        } );
    }

    @Test
    void mustThrowIfStartingNewLockSessionOnMasterThrowsTransactionFailureException()
    {
        assertThrows( DistributedLockFailureException.class, () -> {
            when( master.newLockSession( isNull() ) )
                    .thenThrow( new TransactionFailureException( DatabaseUnavailable, "Not now" ) );

            client.acquireShared( NONE, NODE, 1 );
        } );
    }

    @Test
    void acquireSharedMustThrowIfMasterThrows()
    {
        assertThrows( DistributedLockFailureException.class, () -> {
            whenMasterAcquireShared().thenThrow( new ComException() );

            client.acquireShared( NONE, NODE, 1 );
        } );
    }

    @Test
    void acquireExclusiveMustThrowIfMasterThrows()
    {
        assertThrows( DistributedLockFailureException.class, () -> {
            whenMasterAcquireExclusive().thenThrow( new ComException() );

            client.acquireExclusive( NONE, NODE, 1 );
        } );
    }

    @Test
    void tryExclusiveMustBeUnsupported()
    {
        assertThrows( UnsupportedOperationException.class, () -> client.tryExclusiveLock( NODE, 1 ) );
    }

    @Test
    void trySharedMustBeUnsupported()
    {
        assertThrows( UnsupportedOperationException.class, () -> client.trySharedLock( NODE, 1 ) );
    }

    @Test
    void closeMustThrowIfMasterThrows()
    {
        assertThrows( DistributedLockFailureException.class, () -> {
            when( master.endLockSession( isNull(), anyBoolean() ) ).thenThrow( new ComException() );

            client.acquireExclusive( NONE, NODE, 1 ); // initialise
            client.close();
        } );
    }

    @Test
    void mustCloseLocalClientEvenIfMasterThrows()
    {
        when( master.endLockSession( isNull(), anyBoolean() ) ).thenThrow( new ComException() );

        try
        {
            client.acquireExclusive( NONE, NODE, 1 ); // initialise
            client.close();
            fail( "Expected client.close to throw" );
        }
        catch ( Exception ignore )
        {
        }
        verify( local ).close();
    }

    @Test
    void mustThrowTransientTransactionFailureIfDatabaseUnavailable()
    {
        assertThrows( TransientDatabaseFailureException.class, () -> {
            availabilityGuard.shutdown();

            client.acquireExclusive( NONE, NODE, 1 );
        } );
    }

    @Test
    void shouldFailWithTransientErrorOnDbUnavailable()
    {
        // GIVEN
        availabilityGuard.shutdown();

        // WHEN
        try
        {
            client.acquireExclusive( NONE, NODE, 0 );
            fail( "Should fail" );
        }
        catch ( TransientFailureException e )
        {
            // THEN Good
        }
    }

    @Test
    void acquireSharedFailsWhenClientStopped()
    {
        SlaveLocksClient client = stoppedClient();
        try
        {
            client.acquireShared( NONE, NODE, 1 );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( LockClientStoppedException.class ) );
        }
    }

    @Test
    void releaseSharedFailsWhenClientStopped()
    {
        SlaveLocksClient client = stoppedClient();
        try
        {
            client.releaseShared( NODE, 1 );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( LockClientStoppedException.class ) );
        }
    }

    @Test
    void acquireExclusiveFailsWhenClientStopped()
    {
        SlaveLocksClient client = stoppedClient();
        try
        {
            client.acquireExclusive( NONE, NODE, 1 );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( LockClientStoppedException.class ) );
        }
    }

    @Test
    void releaseExclusiveFailsWhenClientStopped()
    {
        SlaveLocksClient client = stoppedClient();
        try
        {
            client.releaseExclusive( NODE, 1 );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( LockClientStoppedException.class ) );
        }
    }

    @Test
    void getLockSessionIdWhenClientStopped()
    {
        SlaveLocksClient client = stoppedClient();
        try
        {
            client.getLockSessionId();
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( LockClientStoppedException.class ) );
        }
    }

    @Test
    void acquireSharedFailsWhenClientClosed()
    {
        SlaveLocksClient client = closedClient();
        try
        {
            client.acquireShared( NONE, NODE, 1 );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( LockClientStoppedException.class ) );
        }
    }

    @Test
    void releaseSharedFailsWhenClientClosed()
    {
        SlaveLocksClient client = closedClient();
        try
        {
            client.releaseShared( NODE, 1 );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( LockClientStoppedException.class ) );
        }
    }

    @Test
    void acquireExclusiveFailsWhenClientClosed()
    {
        SlaveLocksClient client = closedClient();
        try
        {
            client.acquireExclusive( NONE, NODE, 1 );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( LockClientStoppedException.class ) );
        }
    }

    @Test
    void releaseExclusiveFailsWhenClientClosed()
    {
        SlaveLocksClient client = closedClient();
        try
        {
            client.releaseExclusive( NODE, 1 );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( LockClientStoppedException.class ) );
        }
    }

    @Test
    void getLockSessionIdWhenClientClosed()
    {
        SlaveLocksClient client = closedClient();
        try
        {
            client.getLockSessionId();
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( LockClientStoppedException.class ) );
        }
    }

    @Test
    void stopLocalLocksAndEndLockSessionOnMasterWhenStopped()
    {
        client.acquireShared( NONE, NODE, 1 );

        client.stop();

        verify( local ).stop();
        verify( master ).endLockSession( isNull(), eq( false ) );
    }

    @Test
    void closeLocalLocksAndEndLockSessionOnMasterWhenClosed()
    {
        client.acquireShared( NONE, NODE, 1 );

        client.close();

        verify( local ).close();
        verify( master ).endLockSession( isNull(), eq( true ) );
    }

    @Test
    void closeAfterStopped()
    {
        client.acquireShared( NONE, NODE, 1 );

        client.stop();
        client.close();

        InOrder inOrder = inOrder( master, local );
        inOrder.verify( master ).endLockSession( isNull(), eq( false ) );
        inOrder.verify( local ).close();
    }

    @Test
    void closeWhenNotInitialized()
    {
        client.close();

        verify( local ).close();
        verifyNoMoreInteractions( master );
    }

    @Test
    void stopDoesNotThrowWhenMasterCommunicationThrowsComException()
    {
        ComException error = new ComException( "Communication failure" );
        when( master.endLockSession( isNull(), anyBoolean() ) ).thenThrow( error );

        client.stop();

        logProvider.assertExactly( inLog( SlaveLocksClient.class )
                .warn( equalTo( "Unable to stop lock session on master" ),
                        instanceOf( DistributedLockFailureException.class ) ) );
    }

    @Test
    void stopDoesNotThrowWhenMasterCommunicationThrows()
    {
        RuntimeException error = new IllegalArgumentException( "Wrong params" );
        when( master.endLockSession( isNull(), anyBoolean() ) ).thenThrow( error );

        client.stop();

        logProvider.assertExactly( inLog( SlaveLocksClient.class )
                .warn( equalTo( "Unable to stop lock session on master" ), equalTo( error ) ) );
    }

    @Test
    void shouldIncludeReasonForNotLocked()
    {
        // GIVEN
        SlaveLocksClient client = newSlaveLocksClient( lockManager );
        LockResult lockResult = new LockResult( NOT_LOCKED, "Simply not locked" );
        Response<LockResult> response = new TransactionObligationResponse<>( lockResult, DEFAULT, 2, NO_OP );
        long nodeId = 0;
        ResourceTypes resourceType = NODE;
        when( master.acquireExclusiveLock( isNull(), eq( resourceType ), anyLong() ) ).thenReturn( response );

        // WHEN
        try
        {
            client.acquireExclusive( NONE, resourceType, nodeId );
            fail( "Should have failed" );
        }
        catch ( UnsupportedOperationException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( lockResult.getMessage() ) );
            assertThat( e.getMessage(), containsString( lockResult.getStatus().name() ) );
        }
    }

    @Test
    void acquireDeferredSharedLocksForLabelsAndRelationshipTypes()
    {
        for ( ResourceTypes type : ResourceTypes.values() )
        {
            client.acquireShared( NONE, type, 1, 2 );
        }
        for ( ResourceTypes type : ResourceTypes.values() )
        {
            client.acquireShared( NONE, type, 2, 3 );
        }
        client.acquireShared( NONE, LABEL, 7 );
        client.acquireShared( NONE, RELATIONSHIP_TYPE, 12 );

        client.acquireDeferredSharedLocks( NONE );

        verify( master ).acquireSharedLock( null, LABEL, 1, 2, 3, 7 );
        verify( master ).acquireSharedLock( null, RELATIONSHIP_TYPE, 1, 2, 3, 12 );
    }

    private SlaveLocksClient newSlaveLocksClient( Locks lockManager )
    {
        return new SlaveLocksClient( master, local, lockManager, mock( RequestContextFactory.class ), availabilityGuard,
                logProvider );
    }

    private SlaveLocksClient stoppedClient()
    {
        client.stop();
        return client;
    }

    private SlaveLocksClient closedClient()
    {
        client.acquireShared( NONE, NODE, 1 ); // trigger new lock session initialization
        client.close();
        return client;
    }
}
