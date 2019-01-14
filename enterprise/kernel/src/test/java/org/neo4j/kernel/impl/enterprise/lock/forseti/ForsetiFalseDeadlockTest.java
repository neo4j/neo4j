/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.enterprise.lock.forseti;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.function.ThrowingAction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;
import org.neo4j.kernel.impl.util.concurrent.LockWaitStrategies;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.lock.WaitStrategy;

@RunWith( Parameterized.class )
public class ForsetiFalseDeadlockTest
{
    public static class Fixture
    {
        private final int iterations;
        private final LockManager lockManager;
        private final WaitStrategy waitStrategy;
        private final LockType lockTypeAX;
        private final LockType lockTypeAY;
        private final LockType lockTypeBX;
        private final LockType lockTypeBY;

        Fixture( int iterations,
                 LockManager lockManager,
                 WaitStrategy waitStrategy,
                 LockType lockTypeAX,
                 LockType lockTypeAY,
                 LockType lockTypeBX,
                 LockType lockTypeBY )
        {
            this.iterations = iterations;
            this.lockManager = lockManager;
            this.waitStrategy = waitStrategy;
            this.lockTypeAX = lockTypeAX;
            this.lockTypeAY = lockTypeAY;
            this.lockTypeBX = lockTypeBX;
            this.lockTypeBY = lockTypeBY;
        }

        int iterations()
        {
            return iterations;
        }

        Locks createLockManager( ResourceType resourceType )
        {
            return lockManager.create( resourceType );
        }

        ResourceType createResourceType()
        {
            return new ResourceType()
            {
                @Override
                public int typeId()
                {
                    return 0;
                }

                @Override
                public WaitStrategy waitStrategy()
                {
                    return waitStrategy;
                }

                @Override
                public String name()
                {
                    return "MyTestResource";
                }
            };
        }

        void acquireAX( Locks.Client client, ResourceType resourceType )
        {
            lockTypeAX.acquire( client, resourceType, 1 );
        }

        void releaseAX( Locks.Client client, ResourceType resourceType )
        {
            lockTypeAX.release( client, resourceType, 1 );
        }

        void acquireAY( Locks.Client client, ResourceType resourceType )
        {
            lockTypeAY.acquire( client, resourceType, 2 );
        }

        void releaseAY( Locks.Client client, ResourceType resourceType )
        {
            lockTypeAY.release( client, resourceType, 2 );
        }

        void acquireBX( Locks.Client client, ResourceType resourceType )
        {
            lockTypeBX.acquire( client, resourceType, 1 );
        }

        void releaseBX( Locks.Client client, ResourceType resourceType )
        {
            lockTypeBX.release( client, resourceType, 1 );
        }

        void acquireBY( Locks.Client client, ResourceType resourceType )
        {
            lockTypeBY.acquire( client, resourceType, 2 );
        }

        void releaseBY( Locks.Client client, ResourceType resourceType )
        {
            lockTypeBY.release( client, resourceType, 2 );
        }

        @Override
        public String toString()
        {
            return "iterations=" + iterations +
                   ", lockManager=" + lockManager +
                   ", waitStrategy=" + waitStrategy +
                   ", lockTypeAX=" + lockTypeAX +
                   ", lockTypeAY=" + lockTypeAY +
                   ", lockTypeBX=" + lockTypeBX +
                   ", lockTypeBY=" + lockTypeBY;
        }
    }

    public enum LockType
    {
        EXCLUSIVE
                {
                    @Override
                    public void acquire( Locks.Client client, ResourceType resourceType, int resource )
                    {
                        client.acquireExclusive( LockTracer.NONE, resourceType, resource );
                    }

                    @Override
                    public void release( Locks.Client client, ResourceType resourceType, int resource )
                    {
                        client.releaseExclusive( resourceType, resource );
                    }
                },
        SHARED
                {
                    @Override
                    public void acquire( Locks.Client client, ResourceType resourceType, int resource )
                    {
                        client.acquireShared( LockTracer.NONE, resourceType, resource );
                    }

                    @Override
                    public void release( Locks.Client client, ResourceType resourceType, int resource )
                    {
                        client.releaseShared( resourceType, resource );
                    }
                };

        public abstract void acquire( Locks.Client client, ResourceType resourceType, int resource );

        public abstract void release( Locks.Client client, ResourceType resourceType, int resource );
    }

    public enum LockManager
    {
        COMMUNITY
                {
                    @Override
                    public Locks create( ResourceType resourceType )
                    {
                        return new CommunityLockManger( Config.defaults(), Clock.systemDefaultZone() );
                    }
                },
        FORSETI
                {
                    @Override
                    public Locks create( ResourceType resourceType )
                    {
                        return new ForsetiLockManager( Config.defaults(), Clock.systemDefaultZone(), resourceType );
                    }
                };

        public abstract Locks create( ResourceType resourceType );
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<Object[]> parameters()
    {
        List<Fixture> fixtures = new ArrayList<>();

        // During development I also had iteration counts 1 and 2 here, but they never found anything, so for actually
        // running this test, I leave only iteration count 100 enabled.
        int iteration = 100;
        LockManager[] lockManagers = LockManager.values();
        LockWaitStrategies[] lockWaitStrategies = LockWaitStrategies.values();
        LockType[] lockTypes = LockType.values();
        for ( LockManager lockManager : lockManagers )
        {
            for ( LockWaitStrategies waitStrategy : lockWaitStrategies )
            {
                for ( LockType lockTypeAX : lockTypes )
                {
                    for ( LockType lockTypeAY : lockTypes )
                    {
                        for ( LockType lockTypeBX : lockTypes )
                        {
                            for ( LockType lockTypeBY : lockTypes )
                            {
                                fixtures.add( new Fixture(
                                        iteration, lockManager, waitStrategy,
                                        lockTypeAX, lockTypeAY, lockTypeBX, lockTypeBY ) );
                            }
                        }
                    }
                }
            }
        }

        return fixtures.stream().map( f -> new Object[]{f} ).collect( Collectors.toList() );
    }

    private static ExecutorService executor = Executors.newCachedThreadPool( r ->
    {
        Thread thread = new Thread( r );
        thread.setDaemon( true );
        return thread;
    } );

    public ForsetiFalseDeadlockTest( Fixture fixture )
    {
        this.fixture = fixture;
    }

    private final Fixture fixture;

    /**
     * This takes a fair bit of time, and we don't want to wait that long. But more importantly, false deadlocks are
     * still only very unlikely; they are not impossible, though.
     *
     * So this test is technically flaky, but the probability is, I think, quite low for the 'mild' test, but it is too
     * high for this aggressive test. So therefor I have marked it as @Ignored, but I still keep it here for the sake
     * of this comment, and to allow others to run it and get a feel for the probabilities involved.
     */
    @Ignore
    @Test
    public void testAggressivelyForFalseDeadlocks()
    {
        int testRuns = 2000;
        loopRunTest( testRuns );
    }

    @Test
    public void testMildlyForFalseDeadlocks()
    {
        int testRuns = 10;
        loopRunTest( testRuns );
    }

    private void loopRunTest( int testRuns )
    {
        List<Throwable> exceptionList = new ArrayList<>();
        loopRun( testRuns, exceptionList );

        if ( !exceptionList.isEmpty() )
        {
            // We saw exceptions. Run it 99 more times, and then verify that our false deadlock rate is less than 2%.
            int additionalRuns = testRuns * 99;
            loopRun( additionalRuns, exceptionList );
            double totalRuns = additionalRuns + testRuns;
            double failures = exceptionList.size();
            double failureRate = failures / totalRuns;
            if ( failureRate > 0.02 )
            {
                // We have more than 2% failures. Report it!
                AssertionError error = new AssertionError(
                        "False deadlock failure rate of " + failureRate + " is greater than 2%" );
                for ( Throwable th : exceptionList )
                {
                    error.addSuppressed( th );
                }
                throw error;
            }
        }
    }

    private void loopRun( int testRuns, List<Throwable> exceptionList )
    {
        for ( int i = 0; i < testRuns; i++ )
        {
            try
            {
                runTest();
            }
            catch ( Throwable th )
            {
                th.addSuppressed( new Exception( "Failed at iteration " + i ) );
                exceptionList.add( th );
            }
        }
    }

    private void runTest() throws InterruptedException, java.util.concurrent.ExecutionException
    {
        int iterations = fixture.iterations();
        ResourceType resourceType = fixture.createResourceType();
        Locks manager = fixture.createLockManager( resourceType );
        try ( Locks.Client a = manager.newClient();
              Locks.Client b = manager.newClient() )
        {
            BinaryLatch startLatch = new BinaryLatch();
            BlockedCallable callA = new BlockedCallable( startLatch,
                    () -> workloadA( a, resourceType, iterations ) );
            BlockedCallable callB = new BlockedCallable( startLatch,
                    () -> workloadB( b, resourceType, iterations ) );

            Future<Void> futureA = executor.submit( callA );
            Future<Void> futureB = executor.submit( callB );

            callA.awaitBlocked();
            callB.awaitBlocked();

            startLatch.release();

            futureA.get();
            futureB.get();
        }
        finally
        {
            manager.close();
        }
    }

    private void workloadA( Locks.Client a, ResourceType resourceType, int iterations )
    {
        for ( int i = 0; i < iterations; i++ )
        {
            fixture.acquireAX( a, resourceType );
            fixture.acquireAY( a, resourceType );
            fixture.releaseAY( a, resourceType );
            fixture.releaseAX( a, resourceType );
        }
    }

    private void workloadB( Locks.Client b, ResourceType resourceType, int iterations )
    {
        for ( int i = 0; i < iterations; i++ )
        {
            fixture.acquireBX( b, resourceType );
            fixture.releaseBX( b, resourceType );
            fixture.acquireBY( b, resourceType );
            fixture.releaseBY( b, resourceType );
        }
    }

    public static class BlockedCallable implements Callable<Void>
    {
        private final BinaryLatch startLatch;
        private final ThrowingAction<Exception> delegate;
        private volatile Thread runner;

        BlockedCallable( BinaryLatch startLatch, ThrowingAction<Exception> delegate )
        {
            this.startLatch = startLatch;
            this.delegate = delegate;
        }

        @Override
        public Void call() throws Exception
        {
            runner = Thread.currentThread();
            startLatch.await();
            delegate.apply();
            return null;
        }

        void awaitBlocked()
        {
            Thread t;
            do
            {
                t = runner;
            }
            while ( t == null || t.getState() != Thread.State.WAITING );
        }
    }
}
