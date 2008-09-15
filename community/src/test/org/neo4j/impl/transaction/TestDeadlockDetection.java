/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.transaction;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestDeadlockDetection extends TestCase
{
    public TestDeadlockDetection( String name )
    {
        super( name );
    }

    private static LockManager lm = new LockManager( new PlaceboTm() );

    public static Test suite()
    {
        return new TestSuite( TestDeadlockDetection.class );
    }

    private static class HelperThread extends Thread
    {
        private static final int DO_NOTHING_TASK = 0;
        private static final int GET_WRITELOCK_TASK = 1;
        private static final int GET_READLOCK_TASK = 2;
        private static final int RELEASE_WRITELOCK_TASK = 3;
        private static final int RELEASE_READLOCK_TASK = 4;
        private static final int QUIT_TASK = 5;

        private String name = null;
        private int nextTask = 0;
        private boolean taskCompleted = true;
        private Object resource = null;
        private boolean deadlockOnLastWait = false;

        HelperThread( String name )
        {
            super();
            this.name = name;
        }

        public synchronized void run()
        {
            try
            {
                while ( nextTask != QUIT_TASK )
                {
                    switch ( nextTask )
                    {
                        case DO_NOTHING_TASK:
                            wait( 10 );
                            break;
                        case GET_WRITELOCK_TASK:
                            try
                            {
                                lm.getWriteLock( this.resource );
                                deadlockOnLastWait = false;
                            }
                            catch ( DeadlockDetectedException e )
                            {
                                deadlockOnLastWait = true;
                            }
                            taskCompleted = true;
                            nextTask = DO_NOTHING_TASK;
                            break;
                        case GET_READLOCK_TASK:
                            try
                            {
                                lm.getReadLock( this.resource );
                                deadlockOnLastWait = false;
                            }
                            catch ( DeadlockDetectedException e )
                            {
                                deadlockOnLastWait = true;
                            }
                            taskCompleted = true;
                            nextTask = DO_NOTHING_TASK;
                            break;
                        case RELEASE_WRITELOCK_TASK:
                            lm.releaseWriteLock( this.resource );
                            taskCompleted = true;
                            nextTask = DO_NOTHING_TASK;
                            break;
                        case RELEASE_READLOCK_TASK:
                            lm.releaseReadLock( this.resource );
                            taskCompleted = true;
                            nextTask = DO_NOTHING_TASK;
                            break;
                        case QUIT_TASK:
                            break;
                        default:
                            throw new RuntimeException( "Unkown task "
                                + nextTask );
                    }
                }
            }
            catch ( Exception e )
            {
                taskCompleted = true;
                System.out
                    .println( "" + this + " unable to execute task, " + e );
                e.printStackTrace();
                throw new RuntimeException( e );
            }
        }

        synchronized void waitForCompletionOfTask()
        {
            while ( !taskCompleted )
            {
                try
                {
                    wait( 20 );
                }
                catch ( InterruptedException e )
                {
                }
            }
        }

        boolean isLastGetLockDeadLock()
        {
            return deadlockOnLastWait;
        }

        synchronized void getWriteLock( Object resource )
        {
            if ( !taskCompleted )
            {
                throw new RuntimeException( "Previous task not completed" );
            }
            this.resource = resource;
            taskCompleted = false;
            nextTask = GET_WRITELOCK_TASK;
        }

        synchronized void getReadLock( Object resource )
        {
            if ( !taskCompleted )
            {
                throw new RuntimeException( "Previous task not completed" );
            }
            this.resource = resource;
            taskCompleted = false;
            nextTask = GET_READLOCK_TASK;
        }

        synchronized void releaseWriteLock( Object resource )
        {
            if ( !taskCompleted )
            {
                throw new RuntimeException( "Previous task not completed" );
            }
            this.resource = resource;
            taskCompleted = false;
            nextTask = RELEASE_WRITELOCK_TASK;
        }

        synchronized void releaseReadLock( Object resource )
        {
            if ( !taskCompleted )
            {
                throw new RuntimeException( "Previous task not completed" );
            }
            this.resource = resource;
            taskCompleted = false;
            nextTask = RELEASE_READLOCK_TASK;
        }

        void quit()
        {
            this.resource = null;
            taskCompleted = false;
            nextTask = QUIT_TASK;
        }

        public String toString()
        {
            return name;
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

    public void testDeadlockDetection()
    {
        Object r1 = new ResourceObject( "R1" );
        Object r2 = new ResourceObject( "R2" );
        Object r3 = new ResourceObject( "R3" );
        Object r4 = new ResourceObject( "R4" );

        HelperThread t1 = new HelperThread( "T1" );
        HelperThread t2 = new HelperThread( "T2" );
        HelperThread t3 = new HelperThread( "T3" );
        HelperThread t4 = new HelperThread( "T4" );

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
            // t3-r1-t1
            t2.getWriteLock( r4 );
            sleepSome();
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
            sleepSome();
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
            sleepSome();
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
        catch ( Exception e )
        {
            // RagManager.getManager().dumpStack();
            // LockManager.getManager().dumpRagStack();
            e.printStackTrace();
            fail( "Deadlock detection failed" + e );
        }
        finally
        {
            t1.quit();
            t2.quit();
            t3.quit();
            t4.quit();
        }
    }

    private void sleepSome()
    {
        try
        {
            Thread.sleep( 1000 );
        }
        catch ( InterruptedException e )
        {
        }
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

        StressThread( String name, int numberOfIterations, int depthCount,
            float readWriteRatio )
        {
            super();
            this.name = name;
            this.numberOfIterations = numberOfIterations;
            this.depthCount = depthCount;
            this.readWriteRatio = readWriteRatio;
        }

        public void run()
        {
            try
            {
                while ( !go )
                {
                    try
                    {
                        sleep( 100 );
                    }
                    catch ( InterruptedException e )
                    {
                    }
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
                                lm.releaseReadLock( resourceStack.pop() );
                            }
                            else
                            {
                                lm.releaseWriteLock( resourceStack.pop() );
                            }
                        }
                    }
                }
                catch ( DeadlockDetectedException e )
                {
                    // System.out.println( "Deadlock detected!" );
                }
                finally
                {
                    while ( !lockStack.isEmpty() )
                    {
                        if ( lockStack.pop() == READ )
                        {
                            lm.releaseReadLock( resourceStack.pop() );
                        }
                        else
                        {
                            lm.releaseWriteLock( resourceStack.pop() );
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

    public void testStressMultipleThreads()
    {
        for ( int i = 0; i < StressThread.resources.length; i++ )
        {
            StressThread.resources[i] = new ResourceObject( "RX" + i );
        }
        Thread stressThreads[] = new Thread[50];
        StressThread.go = false;
        for ( int i = 0; i < stressThreads.length; i++ )
        {
            stressThreads[i] = new StressThread( "T" + i, 100, 10, 0.80f );
        }
        for ( int i = 0; i < stressThreads.length; i++ )
        {
            stressThreads[i].start();
        }
        StressThread.go = true;
    }
}