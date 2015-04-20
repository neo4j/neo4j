/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import org.neo4j.com.RequestContext;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;

public class SlaveLocksClientTest
{
    private SlaveLocksClient client;
    private Master master;
    private Locks.Client local;

    @Before
    public void setUp() throws Exception
    {
        Locks lockManager = mock( Locks.class );

        master = mock( Master.class );
        local = mock( Locks.Client.class );

        when( local.tryExclusiveLock( any( Locks.ResourceType.class ), any( long.class ) ) ).thenReturn( true );
        when( local.trySharedLock( any( Locks.ResourceType.class ), any( long.class ) ) ).thenReturn( true );

        when( lockManager.newClient() ).thenReturn( local );

        RequestContextFactory requestContextFactory = mock( RequestContextFactory.class );

        when( master.acquireSharedLock(
                any( RequestContext.class ),
                any( Locks.ResourceType.class ),
                Matchers.<long[]>anyVararg() ) ).thenReturn( new TransactionStreamResponse<>( new LockResult( LockStatus.OK_LOCKED ),
                null, TransactionStream.EMPTY, ResourceReleaser.NO_OP ) );

        when( master.acquireExclusiveLock(
                any( RequestContext.class ),
                any( Locks.ResourceType.class ),
                Matchers.<long[]>anyVararg() ) ).thenReturn( new TransactionStreamResponse<>( new LockResult( LockStatus.OK_LOCKED ),
                null, TransactionStream.EMPTY, ResourceReleaser.NO_OP ) );
        AvailabilityGuard availabilityGuard = mock( AvailabilityGuard.class );
        when( availabilityGuard.isAvailable( anyLong() ) ).thenReturn( true );
        SlaveLockManager.Configuration config = mock( SlaveLockManager.Configuration.class );

        client = new SlaveLocksClient( master, local, lockManager, requestContextFactory, availabilityGuard, config );
    }

    @Test
    public void shouldNotTakeSharedLockOnMasterIfWeAreAlreadyHoldingSaidLock()
    {
        // When taking a lock twice
        client.acquireShared( NODE, 1 );
        client.acquireShared( NODE, 1 );

        // Then only a single network roundtrip should be observed
        verify( master ).acquireSharedLock( null, NODE, 1 );
    }

    @Test
    public void shouldNotTakeSharedLockOnMasterIfWeAreAlreadyHoldingSaidLock_OverlappingBatch()
    {
        // Given the local locks do what they are supposed to do
        when( local.trySharedLock( NODE, 1, 2 ) ).thenReturn( true );
        when( local.trySharedLock( NODE, 2, 3 ) ).thenReturn( true );

        // When taking locks twice
        client.acquireShared( NODE, 1, 2 );
        client.acquireShared( NODE, 2, 3 );

        // Then only the relevant network roundtrip should be observed
        verify( master ).acquireSharedLock( null, NODE, 1, 2 );
        verify( master ).acquireSharedLock( null, NODE, 3 );
    }

    @Test
    public void shouldNotTakeExclusiveLockOnMasterIfWeAreAlreadyHoldingSaidLock()
    {
        // When taking a lock twice
        client.acquireExclusive( NODE, 1 );
        client.acquireExclusive( NODE, 1 );

        // Then only a single network roundtrip should be observed
        verify( master ).acquireExclusiveLock( null, NODE, 1 );
    }

    @Test
    public void shouldNotTakeExclusiveLockOnMasterIfWeAreAlreadyHoldingSaidLock_OverlappingBatch()
    {
        // Given the local locks do what they are supposed to do
        when( local.tryExclusiveLock( NODE, 1, 2 ) ).thenReturn( true );
        when( local.tryExclusiveLock( NODE, 2, 3 ) ).thenReturn( true );

        // When taking locks twice
        client.acquireExclusive( NODE, 1, 2 );
        client.acquireExclusive( NODE, 2, 3 );

        // Then only the relevant network roundtrip should be observed
        verify( master ).acquireExclusiveLock( null, NODE, 1, 2 );
        verify( master ).acquireExclusiveLock( null, NODE, 3 );
    }

    @Test
    public void shouldAllowAcquiringReleasingAndReacquiringExclusive() throws Exception
    {
        // Given we have grabbed and released a lock
        client.acquireExclusive( NODE, 1l );
        client.releaseExclusive( NODE, 1l );

        // When we grab and release that lock again
        client.acquireExclusive( NODE, 1l );
        client.releaseExclusive( NODE, 1l );

        // Then this should cause the local lock manager to hold the lock
        verify( local, times( 2 ) ).tryExclusiveLock( NODE, 1l );
        verify( local, times( 2 ) ).releaseExclusive( NODE, 1l );
    }

    @Test
    public void shouldAllowAcquiringReleasingAndReacquiringShared() throws Exception
    {
        // Given we have grabbed and released a lock
        client.acquireShared( NODE, 1l );
        client.releaseShared( NODE, 1l );

        // When we grab and release that lock again
        client.acquireShared( NODE, 1l );
        client.releaseShared( NODE, 1l );

        // Then this should cause the local lock manager to hold the lock
        verify( local, times( 2 ) ).trySharedLock( NODE, 1l );
        verify( local, times( 2 ) ).releaseShared( NODE, 1l );
    }

    @Test
    public void shouldNotTalkToLocalLocksOnReentrancyExclusive() throws Exception
    {
        // Given we have grabbed and released a lock
        client.acquireExclusive( NODE, 1l );

        // When we grab and release that lock again
        client.acquireExclusive( NODE, 1l );
        client.releaseExclusive( NODE, 1l );

        // Then this should cause the local lock manager to hold the lock
        verify( local, times( 1 ) ).tryExclusiveLock( NODE, 1l );
        verify( local, times( 0 ) ).releaseExclusive( NODE, 1l );
    }

    @Test
    public void shouldNotTalkToLocalLocksOnReentrancyShared() throws Exception
    {
        // Given we have grabbed and released a lock
        client.acquireShared( NODE, 1l );

        // When we grab and release that lock again
        client.acquireShared( NODE, 1l );
        client.releaseShared( NODE, 1l );

        // Then this should cause the local lock manager to hold the lock
        verify( local, times( 1 ) ).trySharedLock( NODE, 1l );
        verify( local, times( 0 ) ).releaseShared( NODE, 1l );
    }

    @Test
    public void shouldReturnNoLockSessionIfNotInitialized() throws Exception
    {
        // When
        int lockSessionId = client.getLockSessionId();

        // Then
        assertThat(lockSessionId, equalTo(-1));
    }

    @Test
    public void shouldReturnDelegateIdIfInitialized() throws Exception
    {
        // Given
        client.acquireExclusive( ResourceTypes.NODE, 1l );

        // When
        int lockSessionId = client.getLockSessionId();

        // Then
        assertThat(lockSessionId, equalTo(0));
    }
}
