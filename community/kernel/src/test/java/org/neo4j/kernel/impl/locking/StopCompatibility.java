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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.test.OtherThreadExecutor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;

@Ignore( "Not a test. This is a compatibility suite, run from LockingCompatibilityTestSuite." )
public class StopCompatibility extends LockingCompatibilityTestSuite.Compatibility
{
    private static final long FIRST_NODE_ID = 42;
    private static final long SECOND_NODE_ID = 4242;
    private static final LockTracer TRACER = LockTracer.NONE;

    private Locks.Client client;

    public StopCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Before
    public void setUp()
    {
        client = locks.newClient();
    }

    @After
    public void tearDown()
    {
        client.close();
    }

    @Test
    public void mustReleaseWriteLockWaitersOnStop()
    {
        // Given
        clientA.acquireShared( TRACER, NODE, 1L );
        clientB.acquireShared( TRACER, NODE, 2L );
        clientC.acquireShared( TRACER, NODE, 3L );
        acquireExclusive( clientB, TRACER, NODE, 1L ).callAndAssertWaiting();
        acquireExclusive( clientC, TRACER, NODE, 1L ).callAndAssertWaiting();

        // When
        clientC.stop();
        clientB.stop();
        clientA.stop();

        // All locks clients should be stopped at this point, and all all locks should be released because none of the
        // clients entered the prepare phase
        LockCountVisitor lockCountVisitor = new LockCountVisitor();
        locks.accept( lockCountVisitor );
        assertEquals( 0, lockCountVisitor.getLockCount() );
    }

    @Test
    public void mustNotReleaseLocksAfterPrepareOnStop()
    {
        // Given
        clientA.acquireShared( TRACER, NODE, 1L );
        clientA.acquireExclusive( TRACER, NODE, 2L );
        clientA.prepare();

        // When
        clientA.stop();

        // The client entered the prepare phase, so it gets to keep its locks
        LockCountVisitor lockCountVisitor = new LockCountVisitor();
        locks.accept( lockCountVisitor );
        assertEquals( 2, lockCountVisitor.getLockCount() );
    }

    @Test
    public void mustReleaseUnpreparedLocksOnStop()
    {
        // Given
        clientA.acquireShared( TRACER, NODE, 1L );
        clientA.acquireExclusive( TRACER, NODE, 2L );

        // When
        clientA.stop();

        // The client was stopped before it could enter the prepare phase, so all of its locks are released
        LockCountVisitor lockCountVisitor = new LockCountVisitor();
        locks.accept( lockCountVisitor );
        assertEquals( 0, lockCountVisitor.getLockCount() );
    }

    @Test
    public void mustReleaseReadLockWaitersOnStop()
    {
        // Given
        clientA.acquireExclusive( TRACER, NODE, 1L );
        clientB.acquireExclusive( TRACER, NODE, 2L );
        acquireShared( clientB, TRACER, NODE, 1L ).callAndAssertWaiting();

        // When
        clientB.stop();
        clientA.stop();

        // All locks clients should be stopped at this point, and all all locks should be released because none of the
        // clients entered the prepare phase
        LockCountVisitor lockCountVisitor = new LockCountVisitor();
        locks.accept( lockCountVisitor );
        assertEquals( 0, lockCountVisitor.getLockCount() );
    }

    @Test
    public void prepareMustAllowAcquiringNewLocksAfterStop()
    {
        // Given
        clientA.prepare();
        clientA.stop();

        // When
        clientA.acquireShared( TRACER, NODE, 1 );
        clientA.acquireExclusive( TRACER, NODE, 2 );

        // Stopped essentially has no effect when it comes after the client has entered the prepare phase
        LockCountVisitor lockCountVisitor = new LockCountVisitor();
        locks.accept( lockCountVisitor );
        assertEquals( 2, lockCountVisitor.getLockCount() );
    }

    @Test( expected = LockClientStoppedException.class )
    public void prepareMustThrowWhenClientStopped()
    {
        stoppedClient().prepare();
    }

    @Test( expected = LockClientStoppedException.class )
    public void acquireSharedThrowsWhenClientStopped()
    {
        stoppedClient().acquireShared( TRACER, NODE, 1 );
    }

    @Test( expected = LockClientStoppedException.class )
    public void acquireExclusiveThrowsWhenClientStopped()
    {
        stoppedClient().acquireExclusive( TRACER, NODE, 1 );
    }

    @Test( expected = LockClientStoppedException.class )
    public void trySharedLockThrowsWhenClientStopped()
    {
        stoppedClient().trySharedLock( NODE, 1 );
    }

    @Test( expected = LockClientStoppedException.class )
    public void tryExclusiveLockThrowsWhenClientStopped()
    {
        stoppedClient().tryExclusiveLock( NODE, 1 );
    }

    @Test( expected = LockClientStoppedException.class )
    public void releaseSharedThrowsWhenClientStopped()
    {
        stoppedClient().releaseShared( NODE, 1 );
    }

    @Test( expected = LockClientStoppedException.class )
    public void releaseExclusiveThrowsWhenClientStopped()
    {
        stoppedClient().releaseExclusive( NODE, 1 );
    }

    @Test
    public void sharedLockCanBeStopped() throws Exception
    {
        acquireExclusiveLockInThisThread();

        LockAcquisition sharedLockAcquisition = acquireSharedLockInAnotherThread();
        assertThreadIsWaitingForLock( sharedLockAcquisition );

        sharedLockAcquisition.stop();
        assertLockAcquisitionFailed( sharedLockAcquisition );
    }

    @Test
    public void exclusiveLockCanBeStopped() throws Exception
    {
        acquireExclusiveLockInThisThread();

        LockAcquisition exclusiveLockAcquisition = acquireExclusiveLockInAnotherThread();
        assertThreadIsWaitingForLock( exclusiveLockAcquisition );

        exclusiveLockAcquisition.stop();
        assertLockAcquisitionFailed( exclusiveLockAcquisition );
    }

    @Test
    public void acquireSharedLockAfterSharedLockStoppedOtherThread() throws Exception
    {
        AcquiredLock thisThreadsExclusiveLock = acquireExclusiveLockInThisThread();

        LockAcquisition sharedLockAcquisition1 = acquireSharedLockInAnotherThread();
        assertThreadIsWaitingForLock( sharedLockAcquisition1 );

        sharedLockAcquisition1.stop();
        assertLockAcquisitionFailed( sharedLockAcquisition1 );

        thisThreadsExclusiveLock.release();

        LockAcquisition sharedLockAcquisition2 = acquireSharedLockInAnotherThread();
        assertLockAcquisitionSucceeded( sharedLockAcquisition2 );
    }

    @Test
    public void acquireExclusiveLockAfterExclusiveLockStoppedOtherThread() throws Exception
    {
        AcquiredLock thisThreadsExclusiveLock = acquireExclusiveLockInThisThread();

        LockAcquisition exclusiveLockAcquisition1 = acquireExclusiveLockInAnotherThread();
        assertThreadIsWaitingForLock( exclusiveLockAcquisition1 );

        exclusiveLockAcquisition1.stop();
        assertLockAcquisitionFailed( exclusiveLockAcquisition1 );

        thisThreadsExclusiveLock.release();

        LockAcquisition exclusiveLockAcquisition2 = acquireExclusiveLockInAnotherThread();
        assertLockAcquisitionSucceeded( exclusiveLockAcquisition2 );
    }

    @Test
    public void acquireSharedLockAfterExclusiveLockStoppedOtherThread() throws Exception
    {
        AcquiredLock thisThreadsExclusiveLock = acquireExclusiveLockInThisThread();

        LockAcquisition exclusiveLockAcquisition = acquireExclusiveLockInAnotherThread();
        assertThreadIsWaitingForLock( exclusiveLockAcquisition );

        exclusiveLockAcquisition.stop();
        assertLockAcquisitionFailed( exclusiveLockAcquisition );

        thisThreadsExclusiveLock.release();

        LockAcquisition sharedLockAcquisition = acquireSharedLockInAnotherThread();
        assertLockAcquisitionSucceeded( sharedLockAcquisition );
    }

    @Test
    public void acquireExclusiveLockAfterSharedLockStoppedOtherThread() throws Exception
    {
        AcquiredLock thisThreadsExclusiveLock = acquireExclusiveLockInThisThread();

        LockAcquisition sharedLockAcquisition = acquireSharedLockInAnotherThread();
        assertThreadIsWaitingForLock( sharedLockAcquisition );

        sharedLockAcquisition.stop();
        assertLockAcquisitionFailed( sharedLockAcquisition );

        thisThreadsExclusiveLock.release();

        LockAcquisition exclusiveLockAcquisition = acquireExclusiveLockInAnotherThread();
        assertLockAcquisitionSucceeded( exclusiveLockAcquisition );
    }

    @Test
    public void acquireSharedLockAfterSharedLockStoppedSameThread() throws Exception
    {
        acquireLockAfterOtherLockStoppedSameThread( true, true );
    }

    @Test
    public void acquireExclusiveLockAfterExclusiveLockStoppedSameThread() throws Exception
    {
        acquireLockAfterOtherLockStoppedSameThread( false, false );
    }

    @Test
    public void acquireSharedLockAfterExclusiveLockStoppedSameThread() throws Exception
    {
        acquireLockAfterOtherLockStoppedSameThread( true, false );
    }

    @Test
    public void acquireExclusiveLockAfterSharedLockStoppedSameThread() throws Exception
    {
        acquireLockAfterOtherLockStoppedSameThread( false, true );
    }

    @Test
    public void closeClientAfterSharedLockStopped() throws Exception
    {
        closeClientAfterLockStopped( true );
    }

    @Test
    public void closeClientAfterExclusiveLockStopped() throws Exception
    {
        closeClientAfterLockStopped( false );
    }

    @Test
    public void acquireExclusiveLockWhileHoldingSharedLockCanBeStopped() throws Exception
    {
        AcquiredLock thisThreadsSharedLock = acquireSharedLockInThisThread();

        CountDownLatch sharedLockAcquired = new CountDownLatch( 1 );
        CountDownLatch startExclusiveLock = new CountDownLatch( 1 );
        LockAcquisition acquisition = acquireSharedAndExclusiveLocksInAnotherThread( sharedLockAcquired,
                startExclusiveLock );

        await( sharedLockAcquired );
        startExclusiveLock.countDown();
        assertThreadIsWaitingForLock( acquisition );

        acquisition.stop();
        assertLockAcquisitionFailed( acquisition );

        thisThreadsSharedLock.release();
        assertNoLocksHeld();
    }

    private Locks.Client stoppedClient()
    {
        try
        {
            client.stop();
            return client;
        }
        catch ( Throwable t )
        {
            throw new AssertionError( "Unable to stop client", t );
        }
    }

    private void closeClientAfterLockStopped( boolean shared ) throws Exception
    {
        AcquiredLock thisThreadsExclusiveLock = acquireExclusiveLockInThisThread();

        CountDownLatch firstLockAcquired = new CountDownLatch( 1 );
        LockAcquisition
                acquisition = tryAcquireTwoLocksLockInAnotherThread( shared, firstLockAcquired );

        await( firstLockAcquired );
        assertThreadIsWaitingForLock( acquisition );
        assertLocksHeld( FIRST_NODE_ID, SECOND_NODE_ID );

        acquisition.stop();
        assertLockAcquisitionFailed( acquisition );
        assertLocksHeld( FIRST_NODE_ID );

        thisThreadsExclusiveLock.release();
        assertNoLocksHeld();
    }

    private void acquireLockAfterOtherLockStoppedSameThread( boolean firstLockShared, boolean secondLockShared )
            throws Exception
    {
        AcquiredLock thisThreadsExclusiveLock = acquireExclusiveLockInThisThread();

        CountDownLatch firstLockFailed = new CountDownLatch( 1 );
        CountDownLatch startSecondLock = new CountDownLatch( 1 );

        LockAcquisition
                lockAcquisition = acquireTwoLocksInAnotherThread( firstLockShared, secondLockShared,
                firstLockFailed, startSecondLock );
        assertThreadIsWaitingForLock( lockAcquisition );

        lockAcquisition.stop();
        await( firstLockFailed );
        thisThreadsExclusiveLock.release();
        startSecondLock.countDown();

        assertLockAcquisitionSucceeded( lockAcquisition );
    }

    private AcquiredLock acquireSharedLockInThisThread()
    {
        client.acquireShared( TRACER, NODE, FIRST_NODE_ID );
        assertLocksHeld( FIRST_NODE_ID );
        return AcquiredLock.shared( client, NODE, FIRST_NODE_ID );
    }

    private AcquiredLock acquireExclusiveLockInThisThread()
    {
        client.acquireExclusive( TRACER, NODE, FIRST_NODE_ID );
        assertLocksHeld( FIRST_NODE_ID );
        return AcquiredLock.exclusive( client, NODE, FIRST_NODE_ID );
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

        Future<Void> future = threadA.execute( state ->
        {
            Locks.Client client = newLockClient( lockAcquisition );
            if ( shared )
            {
                client.acquireShared( TRACER, NODE, FIRST_NODE_ID );
            }
            else
            {
                client.acquireExclusive( TRACER, NODE, FIRST_NODE_ID );
            }
            return null;
        } );
        lockAcquisition.setFuture( future, threadA.get() );

        return lockAcquisition;
    }

    private LockAcquisition acquireTwoLocksInAnotherThread( final boolean firstShared, final boolean secondShared,
            final CountDownLatch firstLockFailed, final CountDownLatch startSecondLock )
    {
        final LockAcquisition lockAcquisition = new LockAcquisition();

        Future<Void> future = threadA.execute( state ->
        {
            try ( Locks.Client client = newLockClient( lockAcquisition ) )
            {
                try
                {
                    if ( firstShared )
                    {
                        client.acquireShared( TRACER, NODE, FIRST_NODE_ID );
                    }
                    else
                    {
                        client.acquireExclusive( TRACER, NODE, FIRST_NODE_ID );
                    }
                    fail( "Transaction termination expected" );
                }
                catch ( Exception e )
                {
                    assertThat( e, instanceOf( LockClientStoppedException.class ) );
                }
            }

            lockAcquisition.setClient( null );
            firstLockFailed.countDown();
            await( startSecondLock );

            try ( Locks.Client client = newLockClient( lockAcquisition ) )
            {
                if ( secondShared )
                {
                    client.acquireShared( TRACER, NODE, FIRST_NODE_ID );
                }
                else
                {
                    client.acquireExclusive( TRACER, NODE, FIRST_NODE_ID );
                }
            }
            return null;
        } );
        lockAcquisition.setFuture( future, threadA.get() );

        return lockAcquisition;
    }

    private LockAcquisition acquireSharedAndExclusiveLocksInAnotherThread( final CountDownLatch sharedLockAcquired,
            final CountDownLatch startExclusiveLock )
    {
        final LockAcquisition lockAcquisition = new LockAcquisition();

        Future<Void> future = threadA.execute( state ->
        {
            try ( Locks.Client client = newLockClient( lockAcquisition ) )
            {
                client.acquireShared( TRACER, NODE, FIRST_NODE_ID );

                sharedLockAcquired.countDown();
                await( startExclusiveLock );

                client.acquireExclusive( TRACER, NODE, FIRST_NODE_ID );
            }
            return null;
        } );
        lockAcquisition.setFuture( future, threadA.get() );

        return lockAcquisition;
    }

    private LockAcquisition tryAcquireTwoLocksLockInAnotherThread( final boolean shared,
            final CountDownLatch firstLockAcquired )
    {
        final LockAcquisition lockAcquisition = new LockAcquisition();

        Future<Void> future = threadA.execute( state ->
        {
            try ( Locks.Client client = newLockClient( lockAcquisition ) )
            {
                if ( shared )
                {
                    client.acquireShared( TRACER, NODE, SECOND_NODE_ID );
                }
                else
                {
                    client.acquireExclusive( TRACER, NODE, SECOND_NODE_ID );
                }

                firstLockAcquired.countDown();

                if ( shared )
                {
                    client.acquireShared( TRACER, NODE, FIRST_NODE_ID );
                }
                else
                {
                    client.acquireExclusive( TRACER, NODE, FIRST_NODE_ID );
                }
            }
            return null;
        } );
        lockAcquisition.setFuture( future, threadA.get() );

        return lockAcquisition;
    }

    private Locks.Client newLockClient( LockAcquisition lockAcquisition )
    {
        Locks.Client client = locks.newClient();
        lockAcquisition.setClient( client );
        return client;
    }

    private void assertLocksHeld( final Long... expectedResourceIds )
    {
        final List<Long> expectedLockedIds = Arrays.asList( expectedResourceIds );
        final List<Long> seenLockedIds = new ArrayList<>();

        locks.accept( ( resourceType, resourceId, description, estimatedWaitTime, lockIdentityHashCode ) ->
                seenLockedIds.add( resourceId ) );

        Collections.sort( expectedLockedIds );
        Collections.sort( seenLockedIds );
        assertEquals( "unexpected locked resource ids", expectedLockedIds, seenLockedIds );
    }

    private void assertNoLocksHeld()
    {
        locks.accept( ( resourceType, resourceId, description, estimatedWaitTime, lockIdentityHashCode ) ->
                fail( "Unexpected lock on " + resourceType + " " + resourceId ) );
    }

    private void assertThreadIsWaitingForLock( LockAcquisition lockAcquisition ) throws Exception
    {
        for ( int i = 0; i < 30 && !suite.isAwaitingLockAcquisition( lockAcquisition.executor.waitUntilWaiting() ); i++ )
        {
            LockSupport.parkNanos( MILLISECONDS.toNanos( 100 ) );
        }
        assertFalse( "locking thread completed", lockAcquisition.completed() );
    }

    private void assertLockAcquisitionSucceeded( LockAcquisition lockAcquisition ) throws Exception
    {
        boolean completed = false;
        for ( int i = 0; i < 30; i++ )
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
        for ( int i = 0; i < 30; i++ )
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
        assertThat( executionException.getCause(), instanceOf( LockClientStoppedException.class ) );
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
        volatile OtherThreadExecutor<Void> executor;

        Future<?> getFuture()
        {
            Objects.requireNonNull( future, "lock acquisition was not initialized with future" );
            return future;
        }

        void setFuture( Future<?> future, OtherThreadExecutor<Void> executor )
        {
            this.future = future;
            this.executor = executor;
        }

        Locks.Client getClient()
        {
            Objects.requireNonNull( client, "lock acquisition was not initialized with client" );
            return client;
        }

        void setClient( Locks.Client client )
        {
            this.client = client;
        }

        Object result() throws InterruptedException, ExecutionException, TimeoutException
        {
            return getFuture().get( 100, TimeUnit.MILLISECONDS );
        }

        boolean completed()
        {
            return getFuture().isDone();
        }

        void stop()
        {
            getClient().stop();
        }
    }

    private static class AcquiredLock
    {
        final Locks.Client client;
        final boolean shared;
        final ResourceType resourceType;
        final long resourceId;

        AcquiredLock( Locks.Client client, boolean shared, ResourceType resourceType, long resourceId )
        {
            this.client = client;
            this.shared = shared;
            this.resourceType = resourceType;
            this.resourceId = resourceId;
        }

        static AcquiredLock shared( Locks.Client client, ResourceType resourceType, long resourceId )
        {
            return new AcquiredLock( client, true, resourceType, resourceId );
        }

        static AcquiredLock exclusive( Locks.Client client, ResourceType resourceType, long resourceId )
        {
            return new AcquiredLock( client, false, resourceType, resourceId );
        }

        void release()
        {
            if ( shared )
            {
                client.releaseShared( resourceType, resourceId );
            }
            else
            {
                client.releaseExclusive( resourceType, resourceId );
            }
        }
    }
}
