/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.ha.HaXaDataSourceManager;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.lock.forseti.ForsetiLockManager;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.RemoteTxHook;
import org.neo4j.kernel.impl.transaction.TxManager;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SlaveLocksClientTest
{
    private SlaveLocksClient client;
    private Master master;
    private AvailabilityGuard availabilityGuard;
    private HaXaDataSourceManager xaDsm;

    @Before
    public void setUp() throws Exception
    {
        master = mock( Master.class );
        Locks localLockManager = new ForsetiLockManager( ResourceTypes.values() );
        Locks.Client local = localLockManager.newClient();
        RequestContextFactory requestContextFactory = mock( RequestContextFactory.class );
        xaDsm = mock( HaXaDataSourceManager.class );
        when( xaDsm.applyTransactions(
                (org.neo4j.com.Response<LockResult>) anyObject() )).thenReturn(
                new LockResult( LockStatus.OK_LOCKED ) );
        AbstractTransactionManager txManager = mock( TxManager.class );
        RemoteTxHook txHook = mock( RemoteTxHook.class );
        AvailabilityGuard availabilityGuard = mock( AvailabilityGuard.class );
        when( availabilityGuard.isAvailable( anyLong() )).thenReturn( true );
        SlaveLockManager.Configuration config = mock( SlaveLockManager.Configuration.class );
        client = new SlaveLocksClient(
                master, local, localLockManager, requestContextFactory, xaDsm,
                txManager, txHook, availabilityGuard, config );
    }

    @Test
    public void shouldNotTakeSharedLockOnMasterIfWeAreAlreadyHoldingSaidLock()
    {
        // When taking a lock twice
        client.acquireShared( ResourceTypes.NODE, 1 );
        client.acquireShared( ResourceTypes.NODE, 1 );

        // Then only a single network roundtrip should be observed
        verify( master ).acquireSharedLock( null, ResourceTypes.NODE, 1 );
    }

    @Test
    public void shouldNotTakeSharedLockOnMasterIfWeAreAlreadyHoldingSaidLock_OverlappingBatch()
    {
        // When taking locks twice
        client.acquireShared( ResourceTypes.NODE, 1, 2 );
        client.acquireShared( ResourceTypes.NODE, 2, 3 );

        // Then only the relevant network roundtrip should be observed
        verify( master ).acquireSharedLock( null, ResourceTypes.NODE, 1, 2 );
        verify( master ).acquireSharedLock( null, ResourceTypes.NODE, 3 );
    }

    @Test
    public void shouldNotTakeExclusiveLockOnMasterIfWeAreAlreadyHoldingSaidLock()
    {
        // When taking a lock twice
        client.acquireExclusive( ResourceTypes.NODE, 1 );
        client.acquireExclusive( ResourceTypes.NODE, 1 );

        // Then only a single network roundtrip should be observed
        verify( master ).acquireExclusiveLock( null, ResourceTypes.NODE, 1 );
    }

    @Test
    public void shouldNotTakeExclusiveLockOnMasterIfWeAreAlreadyHoldingSaidLock_OverlappingBatch()
    {
        // When taking locks twice
        client.acquireExclusive( ResourceTypes.NODE, 1, 2 );
        client.acquireExclusive( ResourceTypes.NODE, 2, 3 );

        // Then only the relevant network roundtrip should be observed
        verify( master ).acquireExclusiveLock( null, ResourceTypes.NODE, 1, 2 );
        verify( master ).acquireExclusiveLock( null, ResourceTypes.NODE, 3 );
    }
}
