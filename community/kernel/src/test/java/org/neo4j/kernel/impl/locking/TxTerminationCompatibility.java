/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.locking;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.impl.api.tx.TxTermination;
import org.neo4j.kernel.impl.api.tx.TxTerminationImpl;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.locking.Locks.Client;
import static org.neo4j.kernel.impl.locking.Locks.ResourceType;
import static org.neo4j.kernel.impl.locking.Locks.Visitor;

@Ignore( "Not a test. This is a compatibility suite, run from LockingCompatibilityTestSuite." )
public class TxTerminationCompatibility extends LockingCompatibilityTestSuite.Compatibility
{
    private static final ResourceType RESOURCE_TYPE = ResourceTypes.NODE;
    private static final long RESOURCE_ID = 42;
    private static final long OTHER_RESOURCE_ID = 4242;

    private ExecutorService executor;
    private Client client;

    public TxTerminationCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Before
    public void setUp() throws Exception
    {
        executor = Executors.newSingleThreadExecutor();
        client = locks.newClient( TxTermination.NONE );
    }

    @After
    public void tearDown() throws Exception
    {
        client.close();
        executor.shutdownNow();
        executor.awaitTermination( 1, TimeUnit.MINUTES );
    }

    @Test
    public void sharedLockIsTransactionTerminationAware() throws Exception
    {
        acquireExclusiveLockInThisThread();

        TxTerminationImpl txTermination = new TxTerminationImpl();
        Future<?> sharedLockAcquisition = acquireSharedLockInAnotherThread( txTermination );
        assertThreadIsWaitingForLock( sharedLockAcquisition );

        txTermination.markForTermination();
        assertLockAcquisitionFailed( sharedLockAcquisition );
    }

    @Test
    public void exclusiveLockIsTransactionTerminationAware() throws Exception
    {
        acquireExclusiveLockInThisThread();

        TxTerminationImpl txTermination = new TxTerminationImpl();
        Future<?> exclusiveLockAcquisition = acquireExclusiveLockInAnotherThread( txTermination );
        assertThreadIsWaitingForLock( exclusiveLockAcquisition );

        txTermination.markForTermination();
        assertLockAcquisitionFailed( exclusiveLockAcquisition );
    }

    @Test
    public void acquireSharedLockAfterSharedLockFailureOnTransactionTerminationOtherThread() throws Exception
    {
        acquireExclusiveLockInThisThread();

        TxTerminationImpl txTermination = new TxTerminationImpl();
        Future<?> sharedLockAcquisition1 = acquireSharedLockInAnotherThread( txTermination );
        assertThreadIsWaitingForLock( sharedLockAcquisition1 );

        txTermination.markForTermination();
        assertLockAcquisitionFailed( sharedLockAcquisition1 );

        releaseAllLocksInThisThread();

        Future<?> sharedLockAcquisition2 = acquireSharedLockInAnotherThread( txTermination );
        assertLockAcquisitionSucceeded( sharedLockAcquisition2 );
    }

    @Test
    public void acquireExclusiveLockAfterExclusiveLockFailureOnTransactionTerminationOtherThread() throws Exception
    {
        acquireExclusiveLockInThisThread();

        TxTerminationImpl txTermination = new TxTerminationImpl();
        Future<?> exclusiveLockAcquisition1 = acquireExclusiveLockInAnotherThread( txTermination );
        assertThreadIsWaitingForLock( exclusiveLockAcquisition1 );

        txTermination.markForTermination();
        assertLockAcquisitionFailed( exclusiveLockAcquisition1 );

        releaseAllLocksInThisThread();

        Future<?> exclusiveLockAcquisition2 = acquireExclusiveLockInAnotherThread( txTermination );
        assertLockAcquisitionSucceeded( exclusiveLockAcquisition2 );
    }

    @Test
    public void acquireSharedLockAfterExclusiveLockFailureOnTransactionTerminationOtherThread() throws Exception
    {
        acquireExclusiveLockInThisThread();

        TxTerminationImpl txTermination = new TxTerminationImpl();
        Future<?> exclusiveLockAcquisition = acquireExclusiveLockInAnotherThread( txTermination );
        assertThreadIsWaitingForLock( exclusiveLockAcquisition );

        txTermination.markForTermination();
        assertLockAcquisitionFailed( exclusiveLockAcquisition );

        releaseAllLocksInThisThread();

        Future<?> sharedLockAcquisition = acquireSharedLockInAnotherThread( txTermination );
        assertLockAcquisitionSucceeded( sharedLockAcquisition );
    }

    @Test
    public void acquireExclusiveLockAfterSharedLockFailureOnTransactionTerminationOtherThread() throws Exception
    {
        acquireExclusiveLockInThisThread();

        TxTerminationImpl txTermination = new TxTerminationImpl();
        Future<?> sharedLockAcquisition = acquireSharedLockInAnotherThread( txTermination );
        assertThreadIsWaitingForLock( sharedLockAcquisition );

        txTermination.markForTermination();
        assertLockAcquisitionFailed( sharedLockAcquisition );

        releaseAllLocksInThisThread();

        Future<?> exclusiveLockAcquisition = acquireExclusiveLockInAnotherThread( txTermination );
        assertLockAcquisitionSucceeded( exclusiveLockAcquisition );
    }

    @Test
    public void acquireSharedLockAfterSharedLockFailureOnTransactionTerminationSameThread() throws Exception
    {
        acquireLockAfterOtherLockFailureOnTransactionTerminationSameThread( true, true );
    }

    @Test
    public void acquireExclusiveLockAfterExclusiveLockFailureOnTransactionTerminationSameThread() throws Exception
    {
        acquireLockAfterOtherLockFailureOnTransactionTerminationSameThread( false, false );
    }

    @Test
    public void acquireSharedLockAfterExclusiveLockFailureOnTransactionTerminationSameThread() throws Exception
    {
        acquireLockAfterOtherLockFailureOnTransactionTerminationSameThread( true, false );
    }

    @Test
    public void acquireExclusiveLockAfterSharedLockFailureOnTransactionTerminationSameThread() throws Exception
    {
        acquireLockAfterOtherLockFailureOnTransactionTerminationSameThread( false, true );
    }

    @Test
    public void closeClientAfterSharedLockFailureOnTransactionTermination() throws Exception
    {
        closeClientAfterLockFailureOnTransactionTermination( true );
    }

    @Test
    public void closeClientAfterExclusiveLockFailureOnTransactionTermination() throws Exception
    {
        closeClientAfterLockFailureOnTransactionTermination( false );
    }

    private void closeClientAfterLockFailureOnTransactionTermination( boolean shared ) throws Exception
    {
        acquireExclusiveLockInThisThread();

        TxTerminationImpl txTermination = new TxTerminationImpl();
        CountDownLatch firstLockAcquired = new CountDownLatch( 1 );
        Future<?> acquisition = tryAcquireTwoLocksLockInAnotherThread( shared, txTermination, firstLockAcquired );

        await( firstLockAcquired );
        assertThreadIsWaitingForLock( acquisition );
        assertLocksHeld( RESOURCE_ID, OTHER_RESOURCE_ID );

        txTermination.markForTermination();
        assertLockAcquisitionFailed( acquisition );
        assertLocksHeld( RESOURCE_ID );

        releaseAllLocksInThisThread();
        assertNoLocksHeld();
    }

    private void acquireLockAfterOtherLockFailureOnTransactionTerminationSameThread( boolean firstLockShared,
            boolean secondLockShared ) throws Exception
    {
        acquireExclusiveLockInThisThread();

        TxTerminationImpl txTermination = new TxTerminationImpl();
        CountDownLatch firstLockFailed = new CountDownLatch( 1 );
        CountDownLatch startSecondLock = new CountDownLatch( 1 );

        Future<?> locking = acquireTwoLocksInAnotherThread( firstLockShared, secondLockShared, txTermination,
                firstLockFailed, startSecondLock );
        assertThreadIsWaitingForLock( locking );

        txTermination.markForTermination();
        await( firstLockFailed );
        txTermination.reset();
        releaseAllLocksInThisThread();
        startSecondLock.countDown();

        assertLockAcquisitionSucceeded( locking );
    }

    private void acquireExclusiveLockInThisThread()
    {
        client.acquireExclusive( RESOURCE_TYPE, RESOURCE_ID );
        assertLocksHeld( RESOURCE_ID );
    }

    private void releaseAllLocksInThisThread()
    {
        client.releaseAll();
    }

    private Future<?> acquireSharedLockInAnotherThread( TxTermination txTermination )
    {
        return acquireLockInAnotherThread( true, txTermination );
    }

    private Future<?> acquireExclusiveLockInAnotherThread( TxTermination txTermination )
    {
        return acquireLockInAnotherThread( false, txTermination );
    }

    private Future<?> acquireLockInAnotherThread( final boolean shared, final TxTermination txTermination )
    {
        return executor.submit( new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                Client client = locks.newClient( txTermination );
                if ( shared )
                {
                    client.acquireShared( RESOURCE_TYPE, RESOURCE_ID );
                }
                else
                {
                    client.acquireExclusive( RESOURCE_TYPE, RESOURCE_ID );
                }
                return null;
            }
        } );
    }

    private Future<?> acquireTwoLocksInAnotherThread( final boolean firstShared, final boolean secondShared,
            final TxTermination txTermination, final CountDownLatch firstLockFailed,
            final CountDownLatch startSecondLock )
    {
        return executor.submit( new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                try ( Client client = locks.newClient( txTermination ) )
                {
                    try
                    {
                        if ( firstShared )
                        {
                            client.acquireShared( RESOURCE_TYPE, RESOURCE_ID );
                        }
                        else
                        {
                            client.acquireExclusive( RESOURCE_TYPE, RESOURCE_ID );
                        }
                        fail( "Transaction termination expected" );
                    }
                    catch ( Exception e )
                    {
                        assertThat( e, instanceOf( TransactionTerminatedException.class ) );
                    }

                    firstLockFailed.countDown();
                    await( startSecondLock );

                    if ( secondShared )
                    {
                        client.acquireShared( RESOURCE_TYPE, RESOURCE_ID );
                    }
                    else
                    {
                        client.acquireExclusive( RESOURCE_TYPE, RESOURCE_ID );
                    }
                }
                return null;
            }
        } );
    }

    private Future<?> tryAcquireTwoLocksLockInAnotherThread( final boolean shared, final TxTermination txTermination,
            final CountDownLatch firstLockAcquired )
    {
        return executor.submit( new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                try ( Client client = locks.newClient( txTermination ) )
                {
                    if ( shared )
                    {
                        client.acquireShared( RESOURCE_TYPE, OTHER_RESOURCE_ID );
                    }
                    else
                    {
                        client.acquireExclusive( RESOURCE_TYPE, OTHER_RESOURCE_ID );
                    }

                    firstLockAcquired.countDown();

                    if ( shared )
                    {
                        client.acquireShared( RESOURCE_TYPE, RESOURCE_ID );
                    }
                    else
                    {
                        client.acquireExclusive( RESOURCE_TYPE, RESOURCE_ID );
                    }
                }
                return null;
            }
        } );
    }

    private void assertLocksHeld( final Long... expectedResourceIds )
    {
        final List<Long> expectedLockedIds = Arrays.asList( expectedResourceIds );
        final List<Long> seenLockedIds = new ArrayList<>();

        locks.accept( new Visitor()
        {
            @Override
            public void visit( ResourceType resourceType, long resourceId, String description, long estimatedWaitTime )
            {
                seenLockedIds.add( resourceId );
            }
        } );

        Collections.sort( expectedLockedIds );
        Collections.sort( seenLockedIds );
        assertEquals( "unexpected locked resource ids", expectedLockedIds, seenLockedIds );
    }

    private void assertNoLocksHeld()
    {
        locks.accept( new Visitor()
        {
            @Override
            public void visit( ResourceType resourceType, long resourceId, String description, long estimatedWaitTime )
            {
                fail( "Unexpected lock on " + resourceType + " " + resourceId );
            }
        } );
    }

    private void assertThreadIsWaitingForLock( Future<?> lockAcquisition ) throws Exception
    {
        for ( int i = 0; i < 20; i++ )
        {
            try
            {
                lockAcquisition.get( 50, TimeUnit.MILLISECONDS );
                fail( "Timeout expected" );
            }
            catch ( TimeoutException ignore )
            {
            }
        }
        assertFalse( "locking thread completed", lockAcquisition.isDone() );
    }

    private void assertLockAcquisitionSucceeded( Future<?> lockAcquisition ) throws Exception
    {
        boolean completed = false;
        for ( int i = 0; i < 20; i++ )
        {
            try
            {
                assertNull( lockAcquisition.get( 50, TimeUnit.MILLISECONDS ) );
                completed = true;
            }
            catch ( TimeoutException ignore )
            {
            }
        }
        assertTrue( "lock was not acquired in time", completed );
        assertTrue( "locking thread seem to be still in progress", lockAcquisition.isDone() );
    }

    private void assertLockAcquisitionFailed( Future<?> lockAcquisition ) throws Exception
    {
        ExecutionException executionException = null;
        for ( int i = 0; i < 20; i++ )
        {
            try
            {
                lockAcquisition.get( 50, TimeUnit.MILLISECONDS );
                fail( "Transaction termination expected" );
            }
            catch ( ExecutionException e )
            {
                executionException = e;
            }
            catch ( TimeoutException ignore )
            {
            }
        }
        assertNotNull( "execution should fail", executionException );
        assertThat( executionException.getCause(), instanceOf( TransactionTerminatedException.class ) );
        assertTrue( "locking thread seem to be still in progress", lockAcquisition.isDone() );
    }

    private static void await( CountDownLatch latch ) throws InterruptedException
    {
        if ( !latch.await( 1, TimeUnit.MINUTES ) )
        {
            fail( "Count down did not happen" );
        }
    }
}
