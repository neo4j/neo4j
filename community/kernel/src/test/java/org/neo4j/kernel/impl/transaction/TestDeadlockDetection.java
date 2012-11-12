/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.neo4j.kernel.DeadlockDetectedException;

public class TestDeadlockDetection
{
    private static final Error DONE = new Error()
    {
        public synchronized Throwable fillInStackTrace()
        {
            return this;
        }
    };

    private static enum Task
    {
        GET_WRITE_LOCK
        {
            @Override
            void execute( LockManager lm, Object resource )
            {
                lm.getWriteLock( resource );
            }
        },
        GET_READ_LOCK
        {
            @Override
            void execute( LockManager lm, Object resource )
            {
                lm.getReadLock( resource );
            }
        },
        RELEASE_WRITE_LOCK
        {
            @Override
            void execute( LockManager lm, Object resource )
            {
                lm.releaseWriteLock( resource, null );
            }
        },
        RELEASE_READ_LOCK
        {
            @Override
            void execute( LockManager lm, Object resource )
            {
                lm.releaseReadLock( resource, null );
            }
        },
        QUIT
        {
            @Override
            void execute( LockManager lm, Object resource )
            {
                throw DONE;
            }
        };

        abstract void execute( LockManager lm, Object resource );
    }
    
    private static class ResourceTask
    {
        public static final ResourceTask IDLE = new ResourceTask( null, null )
        {
            @Override
            void execute( HelperThread thread )
            {
                thread.current.set( this );
                HelperThread.sleep();
            }

            public String toString()
            {
                return "IDLE";
            }
        };
        public static final ResourceTask BUSY  = new ResourceTask( null, null );
        public static final ResourceTask QUIT  = new ResourceTask( null, Task.QUIT );
        private final Object resource;
        private final Task task;

        ResourceTask( Object resource, Task task )
        {
            this.resource = resource;
            this.task = task;
        }

        void execute( HelperThread thread )
        {
            try
            {
                task.execute( thread.lm, resource );
                thread.deadlockOnLastWait = false;
            }
            catch ( DeadlockDetectedException dde )
            {
                thread.deadlockOnLastWait = true;
            }
        }

        public String toString()
        {
            return task == null ? "BUSY" : task.toString();
        }
    }

    private static class HelperThread extends Thread
    {
        private final AtomicReference<ResourceTask> current = new AtomicReference<ResourceTask>( ResourceTask.IDLE );
        private volatile boolean deadlockOnLastWait = false;
        private final LockManager lm;

        HelperThread( String name, LockManager lm )
        {
            super( name );
            this.lm = lm;
        }
        
        private void assign( Task task, Object resource )
        {
            while ( !current.compareAndSet( ResourceTask.IDLE, new ResourceTask( resource, task ) ) )
            {
                if ( current.get().resource == null ) continue;
                throw new RuntimeException( "Previous task not completed" );
            }
        }

        public void run()
        {
            try
            {
                for ( ;; )
                {
                    current.getAndSet( ResourceTask.BUSY ).execute( this );
                    current.compareAndSet( ResourceTask.BUSY, ResourceTask.IDLE );
                }
            }
            catch ( Error e )
            {
                if ( e != DONE ) throw e;
            }
        }

        synchronized void waitForCompletionOfTask()
        {
            while ( current.get() != ResourceTask.IDLE )
            {
                try
                {
                    wait( 1 );
                }
                catch ( InterruptedException e )
                {
                }
            }
        }
        
        void waitForWaitingState()
        {
            while ( getState() != State.WAITING || current.get().resource == ResourceTask.IDLE )
                sleep();
        }

        boolean isLastGetLockDeadLock()
        {
            return deadlockOnLastWait;
        }

        synchronized void getWriteLock( Object resource )
        {
            assign( Task.GET_WRITE_LOCK, resource );
        }

        synchronized void getReadLock( Object resource )
        {
            assign( Task.GET_READ_LOCK, resource );
        }

        synchronized void releaseWriteLock( Object resource )
        {
            assign( Task.RELEASE_WRITE_LOCK, resource );
        }

        synchronized void releaseReadLock( Object resource )
        {
            assign( Task.RELEASE_READ_LOCK, resource );
        }

        void quit()
        {
            while ( !current.compareAndSet( ResourceTask.IDLE, ResourceTask.QUIT ) ) sleep();
        }

        static void sleep()
        {
            try
            {
                sleep( 1 );
            }
            catch ( InterruptedException e )
            {
            }
        }

        public String toString()
        {
            return getName();
        }
    }

    private static class ResourceObject
    {
        private String name = null;

        ResourceObject( String name )
        {
            this.name = name;
        }

        public String toString()
        {
            return this.name;
        }
    }

    @Test
    public void testDeadlockDetection()
    {
        Object r1 = new ResourceObject( "R1" );
        Object r2 = new ResourceObject( "R2" );
        Object r3 = new ResourceObject( "R3" );
        Object r4 = new ResourceObject( "R4" );
        
        LockManager lm = new LockManagerImpl( new RagManager(new PlaceboTm()) );

        HelperThread t1 = new HelperThread( "T1", lm );
        HelperThread t2 = new HelperThread( "T2", lm );
        HelperThread t3 = new HelperThread( "T3", lm );
        HelperThread t4 = new HelperThread( "T4", lm );

        try
        {
            t1.start();
            t2.start();
            t3.start();
            t4.start();

            t1.getReadLock( r1 );
            t1.waitForCompletionOfTask();
            t1.getReadLock( r4 );
            t1.waitForCompletionOfTask();
            t2.getReadLock( r2 );
            t2.waitForCompletionOfTask();
            t2.getReadLock( r3 );
            t2.waitForCompletionOfTask();
            t3.getReadLock( r3 );
            t3.waitForCompletionOfTask();
            t3.getWriteLock( r1 );
            t3.waitForWaitingState();// t3-r1-t1
            t2.getWriteLock( r4 );
            t2.waitForWaitingState();
            // t2-r4-t1
            t1.getWriteLock( r2 );
            t1.waitForCompletionOfTask();
            assertTrue( t1.isLastGetLockDeadLock() ); // t1-r2-t2-r4-t1
            // resolve and try one more time
            t1.releaseReadLock( r4 );
            t2.waitForCompletionOfTask(); // will give r4 to t2
            t1.getWriteLock( r2 );
            // t1-r2-t2
            t2.releaseReadLock( r2 );
            t1.waitForCompletionOfTask(); // will give r2 to t1
            t1.getWriteLock( r4 ); // t1-r4-t2
            // dead lock
            t1.waitForWaitingState();
            t2.getWriteLock( r2 );
            t2.waitForCompletionOfTask();
            assertTrue( t2.isLastGetLockDeadLock() );
            // t2-r2-t3-r1-t1-r4-t2 or t2-r2-t1-r4-t2
            t2.releaseWriteLock( r4 );
            t1.waitForCompletionOfTask(); // give r4 to t1
            t1.releaseWriteLock( r4 );
            t2.getReadLock( r4 );
            t2.waitForCompletionOfTask();
            t1.releaseWriteLock( r2 );
            t1.waitForCompletionOfTask();
            t1.getReadLock( r2 );
            t1.waitForCompletionOfTask();
            t1.releaseReadLock( r1 );
            t3.waitForCompletionOfTask(); // give r1 to t3
            t3.getReadLock( r2 );
            t3.waitForCompletionOfTask();
            t3.releaseWriteLock( r1 );
            t1.getReadLock( r1 );
            t1.waitForCompletionOfTask(); // give r1->t1
            t1.getWriteLock( r4 );
            t3.getWriteLock( r1 );
            t4.getReadLock( r2 );
            t4.waitForCompletionOfTask();
            // deadlock
            t2.getWriteLock( r2 );
            t2.waitForCompletionOfTask();
            assertTrue( t2.isLastGetLockDeadLock() );
            // t2-r2-t3-r1-t1-r4-t2
            // resolve
            t2.releaseReadLock( r4 );
            t1.waitForCompletionOfTask();
            t1.releaseWriteLock( r4 );
            t1.waitForCompletionOfTask();
            t1.releaseReadLock( r1 );
            t1.waitForCompletionOfTask();
            t2.getReadLock( r4 );
            t3.waitForCompletionOfTask(); // give r1 to t3
            t3.releaseWriteLock( r1 );
            t1.getReadLock( r1 );
            t1.waitForCompletionOfTask(); // give r1 to t1
            t1.getWriteLock( r4 );
            t3.releaseReadLock( r2 );
            t3.waitForCompletionOfTask();
            t3.getWriteLock( r1 );
            // cleanup
            t2.releaseReadLock( r4 );
            t1.waitForCompletionOfTask(); // give r4 to t1
            t1.releaseWriteLock( r4 );
            t1.waitForCompletionOfTask();
            t1.releaseReadLock( r1 );
            t3.waitForCompletionOfTask(); // give r1 to t3
            t3.releaseWriteLock( r1 );
            t3.waitForCompletionOfTask();
            t1.releaseReadLock( r2 );
            t4.releaseReadLock( r2 );
            t2.releaseReadLock( r3 );
            t3.releaseReadLock( r3 );
            t1.waitForCompletionOfTask();
            t2.waitForCompletionOfTask();
            t3.waitForCompletionOfTask();
            t4.waitForCompletionOfTask();
            // -- special case...
            t1.getReadLock( r1 );
            t1.waitForCompletionOfTask();
            t2.getReadLock( r1 );
            t2.waitForCompletionOfTask();
            t1.getWriteLock( r1 ); // t1->r1-t1&t2
            t1.waitForWaitingState();
            t2.getWriteLock( r1 );
            t2.waitForCompletionOfTask();
            assertTrue( t2.isLastGetLockDeadLock() );
            // t2->r1->t1->r1->t2
            t2.releaseReadLock( r1 );
            t1.waitForCompletionOfTask();
            t1.releaseReadLock( r1 );
            t1.waitForCompletionOfTask();
            t1.releaseWriteLock( r1 );
            t1.waitForCompletionOfTask();
        }
        finally
        {
            t1.quit();
            t2.quit();
            t3.quit();
            t4.quit();
        }
    }

    private void waitForWaitingThreadState( HelperThread... threads )
    {
        for ( HelperThread thread : threads ) thread.waitForWaitingState();
    }

    public static class StressThread extends Thread
    {
        private static java.util.Random rand = new java.util.Random( System
            .currentTimeMillis() );
        private static final Object READ = new Object();
        private static final Object WRITE = new Object();
        private static ResourceObject resources[] = new ResourceObject[10];

        private static boolean go = false;

        private String name;
        private int numberOfIterations;
        private int depthCount;
        private float readWriteRatio;
        private final LockManager lm;

        StressThread( String name, int numberOfIterations, int depthCount,
            float readWriteRatio, LockManager lm )
        {
            super();
            this.name = name;
            this.numberOfIterations = numberOfIterations;
            this.depthCount = depthCount;
            this.readWriteRatio = readWriteRatio;
            this.lm = lm;
        }

        public void run()
        {
            try
            {
                while ( !go )
                {
                    HelperThread.sleep();
                }
                java.util.Stack<Object> lockStack = new java.util.Stack<Object>();
                java.util.Stack<ResourceObject> resourceStack = new java.util.Stack<ResourceObject>();
                try
                {
                    for ( int i = 0; i < numberOfIterations; i++ )
                    {
                        int depth = depthCount;
                        do
                        {
                            float f = rand.nextFloat();
                            int n = rand.nextInt( resources.length );
                            if ( f < readWriteRatio )
                            {
                                lm.getReadLock( resources[n] );
                                lockStack.push( READ );
                            }
                            else
                            {
                                lm.getWriteLock( resources[n] );
                                lockStack.push( WRITE );
                            }
                            resourceStack.push( resources[n] );
                        }
                        while ( --depth > 0 );
                        /*
                         * try { sleep( rand.nextInt( 100 ) ); } catch (
                         * InterruptedException e ) {}
                         */
                        while ( !lockStack.isEmpty() )
                        {
                            if ( lockStack.pop() == READ )
                            {
                                lm.releaseReadLock( resourceStack.pop(), null );
                            }
                            else
                            {
                                lm.releaseWriteLock( resourceStack.pop() , null);
                            }
                        }
                    }
                }
                catch ( DeadlockDetectedException e )
                {

                }
                finally
                {
                    while ( !lockStack.isEmpty() )
                    {
                        if ( lockStack.pop() == READ )
                        {
                            lm.releaseReadLock( resourceStack.pop(), null );
                        }
                        else
                        {
                            lm.releaseWriteLock( resourceStack.pop(), null);
                        }
                    }
                }
            }
            catch ( Exception e )
            {
                e.printStackTrace();
                throw new RuntimeException( e );
            }
        }

        public String toString()
        {
            return this.name;
        }
    }

    @Test
    public void testStressMultipleThreads()
    {
        for ( int i = 0; i < StressThread.resources.length; i++ )
        {
            StressThread.resources[i] = new ResourceObject( "RX" + i );
        }
        Thread stressThreads[] = new Thread[50];
        StressThread.go = false;
        LockManager lm = new LockManagerImpl( new RagManager(new PlaceboTm()) );
        for ( int i = 0; i < stressThreads.length; i++ )
        {
            stressThreads[i] = new StressThread( "T" + i, 100, 10, 0.80f, lm );
        }
        for ( int i = 0; i < stressThreads.length; i++ )
        {
            stressThreads[i].start();
        }
        StressThread.go = true;
    }
}