/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.neo4j.kernel.impl.locking.community.LockManagerImpl;
import org.neo4j.kernel.impl.locking.community.RagManager;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.nanoTime;

public class LockServiceMicroBenchmark
{
    public static void main( String... args )
    {
        get( Benchmark.class ).execute( get( Implementation.class ) );
    }

    enum Benchmark
    {
        UNCONTENDED
                {
                    @Override
                    void execute( Implementation impl )
                    {
                        int minThreads = Integer.getInteger( "minThreads", 1 );
                        int maxThreads = Integer.getInteger( "maxThreads", cores() * 2 );
                        int iterations = Integer.getInteger( "iterations", 100 );
                        int lockCount = Integer.getInteger( "lockCount", 100_000 );
                        for ( int threads = minThreads; threads <= maxThreads; threads++ )
                        {
                            System.out.printf( "=== %s / %s - %s threads ===%n", this, impl, threads );
                            executeUncontended( impl, threads, iterations, lockCount, false );
                        }
                    }
                },
        REENTRY
                {
                    @Override
                    void execute( Implementation impl )
                    {
                        int minThreads = Integer.getInteger( "minThreads", 1 );
                        int maxThreads = Integer.getInteger( "maxThreads", cores() * 2 );
                        int iterations = Integer.getInteger( "iterations", 100 );
                        int lockCount = Integer.getInteger( "lockCount", 100_000 );
                        for ( int threads = minThreads; threads <= maxThreads; threads++ )
                        {
                            System.out.printf( "=== %s / %s - %s threads ===%n", this, impl, threads );
                            executeUncontended( impl, threads, iterations, lockCount, true );
                        }
                    }
                },
        HANDOVER
                {
                    @Override
                    void execute( Implementation impl )
                    {
                        int minThreads = Integer.getInteger( "minThreads", 1 );
                        int maxThreads = Integer.getInteger( "maxThreads", cores() * 2 );
                        int iterations = Integer.getInteger( "iterations", 100 );
                        int lockCount = Integer.getInteger( "lockCount", 100_000 );
                        for ( int threads = minThreads; threads <= maxThreads; threads++ )
                        {
                            System.out.printf( "=== %s / %s - %s threads ===%n", this, impl, threads );
                            executeHandover( impl, threads, iterations, lockCount );
                        }
                    }
                };

        abstract void execute( Implementation impl );
    }

    enum Implementation
    {
        LOCK_MANAGER
                {
                    @Override
                    LockService create()
                    {
                        return new AdaptedLockManager();
                    }
                },
        REENTRANT_LOCK_SERVICE
                {
                    @Override
                    LockService create()
                    {
                        return new ReentrantLockService();
                    }
                };

        abstract LockService create();
    }

    static class LockingThread extends MeasuringThread
    {
        private final LockService locks;
        private final long nodeId;
        private final boolean reentry;

        LockingThread( long nodeId, LockService locks, int lockCount, boolean reentry )
        {
            super( lockCount );
            this.locks = locks;
            this.nodeId = nodeId;
            this.reentry = reentry;
        }

        @Override
        void init()
        {
            if ( reentry )
            {
                locks.acquireNodeLock( nodeId, LockService.LockType.WRITE_LOCK );
            }
        }

        @Override
        protected void execute()
        {
            long time = nanoTime();
            Lock lock = locks.acquireNodeLock( nodeId, LockService.LockType.WRITE_LOCK );
            update( nanoTime() - time );
            lock.release();
        }
    }

    static void executeUncontended( Implementation impl, int threadCount, int iterations, int lockCount,
                                    boolean reentry )
    {
        for ( int i = 0; i < iterations; i++ )
        {
            LockService locks = impl.create();
            MeasuringThread[] threads = new MeasuringThread[threadCount];
            for ( int nodeId = 0; nodeId < threadCount; nodeId++ )
            {
                threads[nodeId] = new LockingThread( nodeId, locks, lockCount, reentry );
            }
            execute( threads );
        }
    }

    private static void execute( MeasuringThread[] threads )
    {
        for ( MeasuringThread thread : threads )
        {
            thread.start();
        }
        for ( MeasuringThread thread : threads )
        {
            try
            {
                thread.join();
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }
        long minTime = Long.MAX_VALUE, maxTime = 0, totalTime = 0;
        double count = 0.0;
        for ( MeasuringThread thread : threads )
        {
            minTime = min( minTime, thread.minTime );
            maxTime = max( maxTime, thread.maxTime );
            totalTime += thread.totalTime;
            count += thread.iterations;
        }
        System.out.printf( "min=%dns; max=%.3fus; total=%.3fms; avg=%.3fns%n",
                           minTime, maxTime / 1_000.0, totalTime / 1_000_000.0, totalTime / count );
    }

    static void executeHandover( Implementation impl, int threadCount, int iterations, int lockCount )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    static <E extends Enum<E>> E get( Class<E> type )
    {
        try
        {
            return Enum.valueOf( type, System.getProperty( type.getSimpleName() ) );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IllegalArgumentException(
                    "No such " + type.getSimpleName() + ": " + System.getProperty( type.getSimpleName() ) );
        }
        catch ( NullPointerException e )
        {
            throw new IllegalArgumentException( type.getSimpleName() + " not specified." );
        }
    }

    private static int cores()
    {
        return Runtime.getRuntime().availableProcessors();
    }

    static abstract class MeasuringThread extends Thread
    {
        final int iterations;
        long minTime = Long.MAX_VALUE, maxTime, totalTime;

        MeasuringThread( int iterations )
        {
            this.iterations = iterations;
        }

        @Override
        public final void run()
        {
            init();
            for ( int i = 0; i < iterations; i++ )
            {
                execute();
            }
        }

        void init()
        {
        }

        void update( long time )
        {
            minTime = min( minTime, time );
            maxTime = max( maxTime, time );
            totalTime += time;
        }

        protected abstract void execute();
    }

    private static class AdaptedLockManager extends LockManagerImpl implements LockService
    {
        private final ThreadLocal<Transaction> threadMark = new ThreadLocal<Transaction>(){
            @Override
            protected Transaction initialValue()
            {
                return new ThreadMark();
            }
        };

        AdaptedLockManager()
        {
            super( new RagManager() );
        }

        @Override
        public Lock acquireNodeLock( long nodeId, LockType type )
        {
            AbstractLockService.LockedNode resource = new AbstractLockService.LockedNode( nodeId );
            getWriteLock( resource, threadMark.get() );
            return new WriteRelease( resource );
        }

        @Override
        public Lock acquireRelationshipLock( long relationshipId, LockType type )
        {
            AbstractLockService.LockedRelationship resource = new AbstractLockService.LockedRelationship( relationshipId );
            getWriteLock( resource, threadMark.get() );
            return new WriteRelease( resource );
        }

        private class WriteRelease extends Lock
        {
            private final AbstractLockService.LockedPropertyContainer resource;

            WriteRelease( AbstractLockService.LockedPropertyContainer resource )
            {
                this.resource = resource;
            }

            @Override
            public void release()
            {
                releaseWriteLock( resource, threadMark.get() );
            }
        }

        static class ThreadMark implements Transaction
        {
            @Override
            public void commit()
                    throws HeuristicMixedException, HeuristicRollbackException, RollbackException, SecurityException,
                           SystemException
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public boolean delistResource( XAResource xaRes, int flag ) throws IllegalStateException, SystemException
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public boolean enlistResource( XAResource xaRes )
                    throws IllegalStateException, RollbackException, SystemException
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public int getStatus() throws SystemException
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public void registerSynchronization( Synchronization synch )
                    throws IllegalStateException, RollbackException, SystemException
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public void rollback() throws IllegalStateException, SystemException
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public void setRollbackOnly() throws IllegalStateException, SystemException
            {
                throw new UnsupportedOperationException( "not implemented" );
            }
        }
    }
}
