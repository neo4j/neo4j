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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceType;
import org.neo4j.test.extension.actors.Actor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.lock.ResourceTypes.NODE;

abstract class StopCompatibility extends LockCompatibilityTestSupport
{
    private static final long FIRST_NODE_ID = 42;
    private static final long SECOND_NODE_ID = 4242;
    private static final LockTracer TRACER = LockTracer.NONE;

    private Locks.Client client;

    StopCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @BeforeEach
    void setUp()
    {
        client = locks.newClient();
    }

    @AfterEach
    void tearDown()
    {
        client.close();
    }

    @Test
    void mustReleaseWriteLockWaitersOnStop()
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
    void mustNotReleaseLocksAfterPrepareOnStop()
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
    void mustReleaseUnpreparedLocksOnStop()
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
    void mustReleaseReadLockWaitersOnStop()
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
    void prepareMustAllowAcquiringNewLocksAfterStop()
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

    @Test
    void prepareMustThrowWhenClientStopped()
    {
        assertThrows( LockClientStoppedException.class, () -> stoppedClient().prepare() );
    }

    @Test
    void acquireSharedThrowsWhenClientStopped()
    {
        assertThrows( LockClientStoppedException.class, () -> stoppedClient().acquireShared( TRACER, NODE, 1 ) );
    }

    @Test
    void acquireExclusiveThrowsWhenClientStopped()
    {
        assertThrows( LockClientStoppedException.class, () -> stoppedClient().acquireExclusive( TRACER, NODE, 1 ) );
    }

    @Test
    void trySharedLockThrowsWhenClientStopped()
    {
        assertThrows( LockClientStoppedException.class, () -> stoppedClient().trySharedLock( NODE, 1 ) );
    }

    @Test
    void tryExclusiveLockThrowsWhenClientStopped()
    {
        assertThrows( LockClientStoppedException.class, () -> stoppedClient().tryExclusiveLock( NODE, 1 ) );
    }

    @Test
    void releaseSharedThrowsWhenClientStopped()
    {
        assertThrows( LockClientStoppedException.class, () -> stoppedClient().releaseShared( NODE, 1 ) );
    }

    @Test
    void releaseExclusiveThrowsWhenClientStopped()
    {
        assertThrows( LockClientStoppedException.class, () -> stoppedClient().releaseExclusive( NODE, 1 ) );
    }

    @Test
    void sharedLockCanBeStopped() throws Exception
    {
        acquireExclusiveLockInThisThread();

        LockAcquisition sharedLockAcquisition = acquireSharedLockInAnotherThread();
        assertThreadIsWaitingForLock( sharedLockAcquisition );

        sharedLockAcquisition.stop();
        assertLockAcquisitionFailed( sharedLockAcquisition );
    }

    @Test
    void exclusiveLockCanBeStopped() throws Exception
    {
        acquireExclusiveLockInThisThread();

        LockAcquisition exclusiveLockAcquisition = acquireExclusiveLockInAnotherThread();
        assertThreadIsWaitingForLock( exclusiveLockAcquisition );

        exclusiveLockAcquisition.stop();
        assertLockAcquisitionFailed( exclusiveLockAcquisition );
    }

    @Test
    void acquireSharedLockAfterSharedLockStoppedOtherThread() throws Exception
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
    void acquireExclusiveLockAfterExclusiveLockStoppedOtherThread() throws Exception
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
    void acquireSharedLockAfterExclusiveLockStoppedOtherThread() throws Exception
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
    void acquireExclusiveLockAfterSharedLockStoppedOtherThread() throws Exception
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
    void acquireSharedLockAfterSharedLockStoppedSameThread() throws Exception
    {
        acquireLockAfterOtherLockStoppedSameThread( true, true );
    }

    @Test
    void acquireExclusiveLockAfterExclusiveLockStoppedSameThread() throws Exception
    {
        acquireLockAfterOtherLockStoppedSameThread( false, false );
    }

    @Test
    void acquireSharedLockAfterExclusiveLockStoppedSameThread() throws Exception
    {
        acquireLockAfterOtherLockStoppedSameThread( true, false );
    }

    @Test
    void acquireExclusiveLockAfterSharedLockStoppedSameThread() throws Exception
    {
        acquireLockAfterOtherLockStoppedSameThread( false, true );
    }

    @Test
    void closeClientAfterSharedLockStopped() throws Exception
    {
        closeClientAfterLockStopped( true );
    }

    @Test
    void closeClientAfterExclusiveLockStopped() throws Exception
    {
        closeClientAfterLockStopped( false );
    }

    @Test
    void acquireExclusiveLockWhileHoldingSharedLockCanBeStopped() throws Exception
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

        Future<Void> future = threadA.submit( () ->
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
        lockAcquisition.setFuture( future, threadA );

        return lockAcquisition;
    }

    private LockAcquisition acquireTwoLocksInAnotherThread( final boolean firstShared, final boolean secondShared,
            final CountDownLatch firstLockFailed, final CountDownLatch startSecondLock )
    {
        final LockAcquisition lockAcquisition = new LockAcquisition();

        Future<Void> future = threadA.submit( () ->
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
                    assertThat( e ).isInstanceOf( LockClientStoppedException.class );
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
        lockAcquisition.setFuture( future, threadA );

        return lockAcquisition;
    }

    private LockAcquisition acquireSharedAndExclusiveLocksInAnotherThread( final CountDownLatch sharedLockAcquired,
            final CountDownLatch startExclusiveLock )
    {
        final LockAcquisition lockAcquisition = new LockAcquisition();

        Future<Void> future = threadA.submit( () ->
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
        lockAcquisition.setFuture( future, threadA );

        return lockAcquisition;
    }

    private LockAcquisition tryAcquireTwoLocksLockInAnotherThread( final boolean shared,
            final CountDownLatch firstLockAcquired )
    {
        final LockAcquisition lockAcquisition = new LockAcquisition();

        Future<Void> future = threadA.submit( () ->
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
        lockAcquisition.setFuture( future, threadA );

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
        assertEquals( expectedLockedIds, seenLockedIds, "unexpected locked resource ids" );
    }

    private void assertNoLocksHeld()
    {
        locks.accept( ( resourceType, resourceId, description, estimatedWaitTime, lockIdentityHashCode ) ->
                fail( "Unexpected lock on " + resourceType + " " + resourceId ) );
    }

    private void assertThreadIsWaitingForLock( LockAcquisition lockAcquisition ) throws Exception
    {
        // todo do we still need this loop now?
        for ( int i = 0; i < 30 && !suite.isAwaitingLockAcquisition( lockAcquisition.executor ); i++ )
        {
            LockSupport.parkNanos( MILLISECONDS.toNanos( 100 ) );
        }
        assertFalse( lockAcquisition.completed(), "locking thread completed" );
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
        assertTrue( completed, "lock was not acquired in time" );
        assertTrue( lockAcquisition.completed(), "locking thread seem to be still in progress" );
    }

    private void assertLockAcquisitionFailed( LockAcquisition lockAcquisition )
    {
        ExecutionException executionException = null;
        for ( int i = 0; i < 30; i++ )
        {
            Exception e = assertThrows( Exception.class, lockAcquisition::result );
            if ( e instanceof ExecutionException )
            {
                executionException = (ExecutionException) e;
                break;
            }
        }
        assertNotNull( executionException, "execution should fail" );
        assertThat( executionException.getCause() ).isInstanceOf( LockClientStoppedException.class );
        assertTrue( lockAcquisition.completed(), "locking thread seem to be still in progress" );
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
        volatile Actor executor;

        Future<?> getFuture()
        {
            Objects.requireNonNull( future, "lock acquisition was not initialized with future" );
            return future;
        }

        void setFuture( Future<?> future, Actor executor )
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
