/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.ha.lock;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.stubbing.OngoingStubbing;

import org.neo4j.com.ComException;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionObligationResponse;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.locking.LockClientStoppedException;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.time.Clocks;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
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
import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;
import static org.neo4j.kernel.impl.store.StoreId.DEFAULT;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.logging.NullLog.getInstance;

public class SlaveLocksClientTest
{
    private Master master;
    private Locks lockManager;
    private Locks.Client local;
    private SlaveLocksClient client;
    private AvailabilityGuard availabilityGuard;
    private AssertableLogProvider logProvider;

    @Before
    public void setUp()
    {
        master = mock( Master.class );
        availabilityGuard = new AvailabilityGuard( Clocks.fakeClock(), getInstance() );

        lockManager = new CommunityLockManger( Config.defaults(), Clocks.systemClock() );
        local = spy( lockManager.newClient() );
        logProvider = new AssertableLogProvider();

        LockResult lockResultOk = new LockResult( LockStatus.OK_LOCKED );
        TransactionStreamResponse<LockResult> responseOk =
                new TransactionStreamResponse<>( lockResultOk, null, TransactionStream.EMPTY, ResourceReleaser.NO_OP );

        whenMasterAcquireShared().thenReturn( responseOk );

        whenMasterAcquireExclusive().thenReturn( responseOk );

        client = new SlaveLocksClient( master, local, lockManager, mock( RequestContextFactory.class ),
                availabilityGuard, logProvider );
    }

    private OngoingStubbing<Response<LockResult>> whenMasterAcquireShared()
    {
        return when( master.acquireSharedLock(
                isNull(),
                any( ResourceType.class ),
                ArgumentMatchers.<long[]>any() ) );
    }

    private OngoingStubbing<Response<LockResult>> whenMasterAcquireExclusive()
    {
        return when( master.acquireExclusiveLock(
                isNull(),
                any( ResourceType.class ),
                ArgumentMatchers.<long[]>any() ) );
    }

    @After
    public void tearDown()
    {
        local.close();
    }

    @Test
    public void shouldNotTakeSharedLockOnMasterIfWeAreAlreadyHoldingSaidLock()
    {
        // When taking a lock twice
        client.acquireShared( LockTracer.NONE, NODE, 1 );
        client.acquireShared( LockTracer.NONE, NODE, 1 );

        // Then only a single network round-trip should be observed
        verify( master ).acquireSharedLock( null, NODE, 1 );
    }

    @Test
    public void shouldNotTakeExclusiveLockOnMasterIfWeAreAlreadyHoldingSaidLock()
    {
        // When taking a lock twice
        client.acquireExclusive( LockTracer.NONE, NODE, 1 );
        client.acquireExclusive( LockTracer.NONE, NODE, 1 );

        // Then only a single network roundtrip should be observed
        verify( master ).acquireExclusiveLock( null, NODE, 1 );
    }

    @Test
    public void shouldAllowAcquiringReleasingAndReacquiringExclusive()
    {
        // Given we have grabbed and released a lock
        client.acquireExclusive( LockTracer.NONE, NODE, 1L );
        client.releaseExclusive( NODE, 1L );

        // When we grab and release that lock again
        client.acquireExclusive( LockTracer.NONE, NODE, 1L );
        client.releaseExclusive( NODE, 1L );

        // Then this should cause the local lock manager to hold the lock
        verify( local, times( 2 ) ).tryExclusiveLock( NODE, 1L );
        verify( local, times( 2 ) ).releaseExclusive( NODE, 1L );
    }

    @Test
    public void shouldAllowAcquiringReleasingAndReacquiringShared()
    {
        // Given we have grabbed and released a lock
        client.acquireShared( LockTracer.NONE, NODE, 1L );
        client.releaseShared( NODE, 1L );

        // When we grab and release that lock again
        client.acquireShared( LockTracer.NONE, NODE, 1L );
        client.releaseShared( NODE, 1L );

        // Then this should cause the local lock manager to hold the lock
        verify( local, times( 2 ) ).trySharedLock( NODE, 1L );
        verify( local, times( 2 ) ).releaseShared( NODE, 1L );
    }

    @Test
    public void shouldUseReEntryMethodsOnLocalLocksForReEntryExclusive()
    {
        // Given we have grabbed and released a lock
        client.acquireExclusive( LockTracer.NONE, NODE, 1L );

        // When we grab and release that lock again
        client.acquireExclusive( LockTracer.NONE, NODE, 1L );
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
    public void shouldUseReEntryMethodsOnLocalLocksForReEntryShared()
    {
        // Given we have grabbed and released a lock
        client.acquireShared( LockTracer.NONE, NODE, 1L );

        // When we grab and release that lock again
        client.acquireShared( LockTracer.NONE, NODE, 1L );
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
    public void shouldReturnNoLockSessionIfNotInitialized()
    {
        // When
        int lockSessionId = client.getLockSessionId();

        // Then
        assertThat( lockSessionId, equalTo( -1 ) );
    }

    @Test
    public void shouldReturnDelegateIdIfInitialized()
    {
        // Given
        client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 1L );

        // When
        int lockSessionId = client.getLockSessionId();

        // Then
        assertThat( lockSessionId, equalTo( local.getLockSessionId() ) );
    }

    @Test( expected = DistributedLockFailureException.class )
    public void mustThrowIfStartingNewLockSessionOnMasterThrowsComException() throws Exception
    {
        when( master.newLockSession( isNull() ) ).thenThrow( new ComException() );

        client.acquireShared( LockTracer.NONE, NODE, 1 );
    }

    @Test( expected = DistributedLockFailureException.class )
    public void mustThrowIfStartingNewLockSessionOnMasterThrowsTransactionFailureException() throws Exception
    {
        when( master.newLockSession( isNull() ) ).thenThrow(
                new TransactionFailureException( Status.General.DatabaseUnavailable, "Not now" ) );

        client.acquireShared( LockTracer.NONE, NODE, 1 );
    }

    @Test( expected = DistributedLockFailureException.class )
    public void acquireSharedMustThrowIfMasterThrows()
    {
        whenMasterAcquireShared().thenThrow( new ComException() );

        client.acquireShared( LockTracer.NONE, NODE, 1 );
    }

    @Test( expected = DistributedLockFailureException.class )
    public void acquireExclusiveMustThrowIfMasterThrows()
    {
        whenMasterAcquireExclusive().thenThrow( new ComException() );

        client.acquireExclusive( LockTracer.NONE, NODE, 1 );
    }

    @Test( expected = UnsupportedOperationException.class )
    public void tryExclusiveMustBeUnsupported()
    {
        client.tryExclusiveLock( NODE, 1 );
    }

    @Test( expected = UnsupportedOperationException.class )
    public void trySharedMustBeUnsupported()
    {
        client.trySharedLock( NODE, 1 );
    }

    @Test( expected = DistributedLockFailureException.class )
    public void closeMustThrowIfMasterThrows()
    {
        when( master.endLockSession( isNull(), anyBoolean() ) ).thenThrow( new ComException() );

        client.acquireExclusive( LockTracer.NONE, NODE, 1 ); // initialise
        client.close();
    }

    @Test
    public void mustCloseLocalClientEvenIfMasterThrows()
    {
        when( master.endLockSession( isNull(), anyBoolean() ) ).thenThrow( new ComException() );

        try
        {
            client.acquireExclusive( LockTracer.NONE, NODE, 1 ); // initialise
            client.close();
            fail( "Expected client.close to throw" );
        }
        catch ( Exception ignore )
        {
        }
        verify( local ).close();
    }

    @Test( expected = org.neo4j.graphdb.TransientDatabaseFailureException.class )
    public void mustThrowTransientTransactionFailureIfDatabaseUnavailable()
    {
        availabilityGuard.shutdown();

        client.acquireExclusive( LockTracer.NONE, NODE, 1 );
    }

    @Test
    public void shouldFailWithTransientErrorOnDbUnavailable()
    {
        // GIVEN
        availabilityGuard.shutdown();

        // WHEN
        try
        {
            client.acquireExclusive( LockTracer.NONE, NODE, 0 );
            fail( "Should fail" );
        }
        catch ( TransientFailureException e )
        {
            // THEN Good
        }
    }

    @Test
    public void acquireSharedFailsWhenClientStopped()
    {
        SlaveLocksClient client = stoppedClient();
        try
        {
            client.acquireShared( LockTracer.NONE, NODE, 1 );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( LockClientStoppedException.class ) );
        }
    }

    @Test
    public void releaseSharedFailsWhenClientStopped()
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
    public void acquireExclusiveFailsWhenClientStopped()
    {
        SlaveLocksClient client = stoppedClient();
        try
        {
            client.acquireExclusive( LockTracer.NONE, NODE, 1 );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( LockClientStoppedException.class ) );
        }
    }

    @Test
    public void releaseExclusiveFailsWhenClientStopped()
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
    public void getLockSessionIdWhenClientStopped()
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
    public void acquireSharedFailsWhenClientClosed()
    {
        SlaveLocksClient client = closedClient();
        try
        {
            client.acquireShared( LockTracer.NONE, NODE, 1 );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( LockClientStoppedException.class ) );
        }
    }

    @Test
    public void releaseSharedFailsWhenClientClosed()
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
    public void acquireExclusiveFailsWhenClientClosed()
    {
        SlaveLocksClient client = closedClient();
        try
        {
            client.acquireExclusive( LockTracer.NONE, NODE, 1 );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( LockClientStoppedException.class ) );
        }
    }

    @Test
    public void releaseExclusiveFailsWhenClientClosed()
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
    public void getLockSessionIdWhenClientClosed()
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
    public void stopLocalLocksAndEndLockSessionOnMasterWhenStopped()
    {
        client.acquireShared( LockTracer.NONE, NODE, 1 );

        client.stop();

        verify( local ).stop();
        verify( master ).endLockSession( isNull(), eq( false ) );
    }

    @Test
    public void closeLocalLocksAndEndLockSessionOnMasterWhenClosed()
    {
        client.acquireShared( LockTracer.NONE, NODE, 1 );

        client.close();

        verify( local ).close();
        verify( master ).endLockSession( isNull(), eq( true ) );
    }

    @Test
    public void closeAfterStopped()
    {
        client.acquireShared( LockTracer.NONE, NODE, 1 );

        client.stop();
        client.close();

        InOrder inOrder = inOrder( master, local );
        inOrder.verify( master ).endLockSession( isNull(), eq( false ) );
        inOrder.verify( local ).close();
    }

    @Test
    public void closeWhenNotInitialized()
    {
        client.close();

        verify( local ).close();
        verifyNoMoreInteractions( master );
    }

    @Test
    public void stopDoesNotThrowWhenMasterCommunicationThrowsComException()
    {
        ComException error = new ComException( "Communication failure" );
        when( master.endLockSession( isNull(), anyBoolean() ) ).thenThrow( error );

        client.stop();

        logProvider.assertExactly( inLog( SlaveLocksClient.class )
                .warn( equalTo( "Unable to stop lock session on master" ),
                        CoreMatchers.instanceOf( DistributedLockFailureException.class ) ) );
    }

    @Test
    public void stopDoesNotThrowWhenMasterCommunicationThrows()
    {
        RuntimeException error = new IllegalArgumentException( "Wrong params" );
        when( master.endLockSession( isNull(), anyBoolean() ) ).thenThrow( error );

        client.stop();

        logProvider.assertExactly( inLog( SlaveLocksClient.class )
                .warn( equalTo( "Unable to stop lock session on master" ),
                        CoreMatchers.equalTo( error ) ) );
    }

    @Test
    public void shouldIncludeReasonForNotLocked()
    {
        // GIVEN
        SlaveLocksClient client = newSlaveLocksClient( lockManager );
        LockResult lockResult = new LockResult( LockStatus.NOT_LOCKED, "Simply not locked" );
        Response<LockResult> response = new TransactionObligationResponse<>( lockResult, DEFAULT, 2, NO_OP );
        long nodeId = 0;
        ResourceTypes resourceType = NODE;
        when( master.acquireExclusiveLock( isNull(),
                eq( resourceType ), anyLong() ) ).thenReturn( response );

        // WHEN
        try
        {
            client.acquireExclusive( LockTracer.NONE, resourceType, nodeId );
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
    public void acquireDeferredSharedLocksForLabelsAndRelationshipTypes()
    {
        for ( ResourceTypes type : ResourceTypes.values() )
        {
            client.acquireShared( LockTracer.NONE, type, 1, 2 );
        }
        for ( ResourceTypes type : ResourceTypes.values() )
        {
            client.acquireShared( LockTracer.NONE, type, 2, 3 );
        }
        client.acquireShared( LockTracer.NONE, ResourceTypes.LABEL, 7 );
        client.acquireShared( LockTracer.NONE, ResourceTypes.RELATIONSHIP_TYPE, 12 );

        client.acquireDeferredSharedLocks( LockTracer.NONE );

        verify( master ).acquireSharedLock( null, ResourceTypes.LABEL, 1, 2, 3, 7 );
        verify( master ).acquireSharedLock( null, ResourceTypes.RELATIONSHIP_TYPE, 1, 2, 3, 12 );
    }

    private SlaveLocksClient newSlaveLocksClient( Locks lockManager )
    {
        return new SlaveLocksClient( master, local, lockManager, mock( RequestContextFactory.class ),
                availabilityGuard, logProvider );
    }

    private SlaveLocksClient stoppedClient()
    {
        client.stop();
        return client;
    }

    private SlaveLocksClient closedClient()
    {
        client.acquireShared( LockTracer.NONE, NODE, 1 ); // trigger new lock session initialization
        client.close();
        return client;
    }
}
