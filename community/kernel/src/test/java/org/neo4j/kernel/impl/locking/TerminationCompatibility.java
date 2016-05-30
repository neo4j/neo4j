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
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.graphdb.TransactionTerminatedException;

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
public class TerminationCompatibility extends LockingCompatibilityTestSuite.Compatibility
{
    private static final ResourceType RESOURCE_TYPE = ResourceTypes.NODE;
    private static final long RESOURCE_ID = 42;
    private static final long OTHER_RESOURCE_ID = 4242;

    private ExecutorService executor;
    private Client client;

    public TerminationCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Before
    public void setUp() throws Exception
    {
        executor = Executors.newSingleThreadExecutor();
        client = locks.newClient();
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

        LockAcquisition sharedLockAcquisition = acquireSharedLockInAnotherThread();
        assertThreadIsWaitingForLock( sharedLockAcquisition );

        sharedLockAcquisition.terminate();
        assertLockAcquisitionFailed( sharedLockAcquisition );
    }

    @Test
    public void exclusiveLockIsTransactionTerminationAware() throws Exception
    {
        acquireExclusiveLockInThisThread();

        LockAcquisition exclusiveLockAcquisition = acquireExclusiveLockInAnotherThread();
        assertThreadIsWaitingForLock( exclusiveLockAcquisition );

        exclusiveLockAcquisition.terminate();
        assertLockAcquisitionFailed( exclusiveLockAcquisition );
    }

    @Test
    public void acquireSharedLockAfterSharedLockFailureOnTransactionTerminationOtherThread() throws Exception
    {
        acquireExclusiveLockInThisThread();

        LockAcquisition sharedLockAcquisition1 = acquireSharedLockInAnotherThread();
        assertThreadIsWaitingForLock( sharedLockAcquisition1 );

        sharedLockAcquisition1.terminate();
        assertLockAcquisitionFailed( sharedLockAcquisition1 );

        releaseAllLocksInThisThread();

        LockAcquisition sharedLockAcquisition2 = acquireSharedLockInAnotherThread();
        assertLockAcquisitionSucceeded( sharedLockAcquisition2 );
    }

    @Test
    public void acquireExclusiveLockAfterExclusiveLockFailureOnTransactionTerminationOtherThread() throws Exception
    {
        acquireExclusiveLockInThisThread();

        LockAcquisition exclusiveLockAcquisition1 = acquireExclusiveLockInAnotherThread();
        assertThreadIsWaitingForLock( exclusiveLockAcquisition1 );

        exclusiveLockAcquisition1.terminate();
        assertLockAcquisitionFailed( exclusiveLockAcquisition1 );

        releaseAllLocksInThisThread();

        LockAcquisition exclusiveLockAcquisition2 = acquireExclusiveLockInAnotherThread();
        assertLockAcquisitionSucceeded( exclusiveLockAcquisition2 );
    }

    @Test
    public void acquireSharedLockAfterExclusiveLockFailureOnTransactionTerminationOtherThread() throws Exception
    {
        acquireExclusiveLockInThisThread();

        LockAcquisition exclusiveLockAcquisition = acquireExclusiveLockInAnotherThread();
        assertThreadIsWaitingForLock( exclusiveLockAcquisition );

        exclusiveLockAcquisition.terminate();
        assertLockAcquisitionFailed( exclusiveLockAcquisition );

        releaseAllLocksInThisThread();

        LockAcquisition sharedLockAcquisition = acquireSharedLockInAnotherThread();
        assertLockAcquisitionSucceeded( sharedLockAcquisition );
    }

    @Test
    public void acquireExclusiveLockAfterSharedLockFailureOnTransactionTerminationOtherThread() throws Exception
    {
        acquireExclusiveLockInThisThread();

        LockAcquisition sharedLockAcquisition = acquireSharedLockInAnotherThread();
        assertThreadIsWaitingForLock( sharedLockAcquisition );

        sharedLockAcquisition.terminate();
        assertLockAcquisitionFailed( sharedLockAcquisition );

        releaseAllLocksInThisThread();

        LockAcquisition exclusiveLockAcquisition = acquireExclusiveLockInAnotherThread();
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

    @Test
    public void acquireExclusiveLockWhileHoldingSharedLockCanBeTerminated() throws Exception
    {
        acquireSharedLockInThisThread();

        CountDownLatch sharedLockAcquired = new CountDownLatch( 1 );
        CountDownLatch startExclusiveLock = new CountDownLatch( 1 );
        LockAcquisition acquisition = acquireSharedAndExclusiveLocksInAnotherThread( sharedLockAcquired,
                startExclusiveLock );

        await( sharedLockAcquired );
        startExclusiveLock.countDown();
        assertThreadIsWaitingForLock( acquisition );

        acquisition.terminate();
        assertLockAcquisitionFailed( acquisition );

        releaseAllLocksInThisThread();
        assertNoLocksHeld();
    }

    private void closeClientAfterLockFailureOnTransactionTermination( boolean shared ) throws Exception
    {
        acquireExclusiveLockInThisThread();

        CountDownLatch firstLockAcquired = new CountDownLatch( 1 );
        LockAcquisition acquisition = tryAcquireTwoLocksLockInAnotherThread( shared, firstLockAcquired );

        await( firstLockAcquired );
        assertThreadIsWaitingForLock( acquisition );
        assertLocksHeld( RESOURCE_ID, OTHER_RESOURCE_ID );

        acquisition.terminate();
        assertLockAcquisitionFailed( acquisition );
        assertLocksHeld( RESOURCE_ID );

        releaseAllLocksInThisThread();
        assertNoLocksHeld();
    }

    private void acquireLockAfterOtherLockFailureOnTransactionTerminationSameThread( boolean firstLockShared,
            boolean secondLockShared ) throws Exception
    {
        acquireExclusiveLockInThisThread();

        CountDownLatch firstLockFailed = new CountDownLatch( 1 );
        CountDownLatch startSecondLock = new CountDownLatch( 1 );

        LockAcquisition lockAcquisition = acquireTwoLocksInAnotherThread( firstLockShared, secondLockShared,
                firstLockFailed, startSecondLock );
        assertThreadIsWaitingForLock( lockAcquisition );

        lockAcquisition.terminate();
        await( firstLockFailed );
        releaseAllLocksInThisThread();
        startSecondLock.countDown();

        assertLockAcquisitionSucceeded( lockAcquisition );
    }

    private void acquireSharedLockInThisThread()
    {
        client.acquireShared( RESOURCE_TYPE, RESOURCE_ID );
        assertLocksHeld( RESOURCE_ID );
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

    private LockAcquisition acquireSharedLockInAnotherThread()
    {
        return acquireLockInAnotherThread( true );
    }

    private LockAcquisition acquireExclusiveLockInAnotherThread()
    {
        return acquireLockInAnotherThread( false );
    }

    private LockAcquisition acquireLockInAnotherThread( final boolean shared )
    {
        final LockAcquisition lockAcquisition = new LockAcquisition();

        Future<Void> future = executor.submit( new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                Client client = newLockClient( lockAcquisition );
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
        lockAcquisition.setFuture( future );

        return lockAcquisition;
    }

    private LockAcquisition acquireTwoLocksInAnotherThread( final boolean firstShared, final boolean secondShared,
            final CountDownLatch firstLockFailed, final CountDownLatch startSecondLock )
    {
        final LockAcquisition lockAcquisition = new LockAcquisition();

        Future<Void> future = executor.submit( new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                try ( Client client = newLockClient( lockAcquisition ) )
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
                }

                lockAcquisition.setClient( null );
                firstLockFailed.countDown();
                await( startSecondLock );

                try ( Client client = newLockClient( lockAcquisition ) )
                {
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
        lockAcquisition.setFuture( future );

        return lockAcquisition;
    }

    private LockAcquisition acquireSharedAndExclusiveLocksInAnotherThread( final CountDownLatch sharedLockAcquired,
            final CountDownLatch startExclusiveLock )
    {
        final LockAcquisition lockAcquisition = new LockAcquisition();

        Future<Void> future = executor.submit( new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                try ( Client client = newLockClient( lockAcquisition ) )
                {
                    client.acquireShared( RESOURCE_TYPE, RESOURCE_ID );

                    sharedLockAcquired.countDown();
                    await( startExclusiveLock );

                    client.acquireExclusive( RESOURCE_TYPE, RESOURCE_ID );
                }
                return null;
            }
        } );
        lockAcquisition.setFuture( future );

        return lockAcquisition;
    }

    private LockAcquisition tryAcquireTwoLocksLockInAnotherThread( final boolean shared,
            final CountDownLatch firstLockAcquired )
    {
        final LockAcquisition lockAcquisition = new LockAcquisition();

        Future<Void> future = executor.submit( new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                try ( Client client = newLockClient( lockAcquisition ) )
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
        lockAcquisition.setFuture( future );

        return lockAcquisition;
    }

    private Client newLockClient( LockAcquisition lockAcquisition )
    {
        Client client = locks.newClient();
        lockAcquisition.setClient( client );
        return client;
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

    private void assertThreadIsWaitingForLock( LockAcquisition lockAcquisition ) throws Exception
    {
        for ( int i = 0; i < 20; i++ )
        {
            try
            {
                lockAcquisition.result();
                fail( "Timeout expected" );
            }
            catch ( TimeoutException ignore )
            {
            }
        }
        assertFalse( "locking thread completed", lockAcquisition.completed() );
    }

    private void assertLockAcquisitionSucceeded( LockAcquisition lockAcquisition ) throws Exception
    {
        boolean completed = false;
        for ( int i = 0; i < 20; i++ )
        {
            try
            {
                assertNull( lockAcquisition.result() );
                completed = true;
            }
            catch ( TimeoutException ignore )
            {
            }
        }
        assertTrue( "lock was not acquired in time", completed );
        assertTrue( "locking thread seem to be still in progress", lockAcquisition.completed() );
    }

    private void assertLockAcquisitionFailed( LockAcquisition lockAcquisition ) throws Exception
    {
        ExecutionException executionException = null;
        for ( int i = 0; i < 20; i++ )
        {
            try
            {
                lockAcquisition.result();
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
        assertTrue( "locking thread seem to be still in progress", lockAcquisition.completed() );
    }

    private static void await( CountDownLatch latch ) throws InterruptedException
    {
        if ( !latch.await( 1, TimeUnit.MINUTES ) )
        {
            fail( "Count down did not happen" );
        }
    }

    private static class LockAcquisition
    {
        volatile Future<?> future;
        volatile Locks.Client client;

        Future<?> getFuture()
        {
            Objects.requireNonNull( future, "lock acquisition was not initialized with future" );
            return future;
        }

        void setFuture( Future<?> future )
        {
            this.future = future;
        }

        Client getClient()
        {
            Objects.requireNonNull( client, "lock acquisition was not initialized with client" );
            return client;
        }

        void setClient( Client client )
        {
            this.client = client;
        }

        Object result() throws InterruptedException, ExecutionException, TimeoutException
        {
            return getFuture().get( 50, TimeUnit.MILLISECONDS );
        }

        boolean completed()
        {
            return getFuture().isDone();
        }

        void terminate()
        {
            getClient().markForTermination();
        }
    }
}
