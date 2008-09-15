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

import org.neo4j.impl.transaction.DeadlockDetectedException;
import org.neo4j.impl.transaction.LockManager;

public class TestRWLock extends TestCase
{
    private LockManager lm = new LockManager( new PlaceboTm() );

    public TestRWLock( String testName )
    {
        super( testName );
    }

    public static void main( java.lang.String[] args )
    {
        junit.textui.TestRunner.run( suite() );
    }

    public static Test suite()
    {
        TestSuite suite = new TestSuite( TestRWLock.class );
        return suite;
    }

    public void testSingleThread() throws Exception
    {
        try
        {
            lm.getReadLock( null );
            fail( "Null parameter should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }
        try
        {
            lm.getWriteLock( null );
            fail( "Null parameter should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }
        try
        {
            lm.releaseReadLock( null );
            fail( "Null parameter should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }
        try
        {
            lm.releaseWriteLock( null );
            fail( "Null parameter should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }

        Object entity = new Object();
        try
        {
            lm.releaseWriteLock( entity );
            fail( "Invalid release should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }
        try
        {
            lm.releaseReadLock( entity );
            fail( "Invalid release should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }

        lm.getReadLock( entity );
        try
        {
            lm.releaseWriteLock( entity );
            fail( "Invalid release should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }
        lm.releaseReadLock( entity );
        lm.getWriteLock( entity );
        try
        {
            lm.releaseReadLock( entity );
            fail( "Invalid release should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }
        lm.releaseWriteLock( entity );

        lm.getReadLock( entity );
        lm.getWriteLock( entity );
        lm.releaseWriteLock( entity );
        lm.releaseReadLock( entity );

        lm.getWriteLock( entity );
        lm.getReadLock( entity );
        lm.releaseReadLock( entity );
        lm.releaseWriteLock( entity );

        for ( int i = 0; i < 10; i++ )
        {
            if ( (i % 2) == 0 )
            {
                lm.getWriteLock( entity );
            }
            else
            {
                lm.getReadLock( entity );
            }
        }
        for ( int i = 9; i >= 0; i-- )
        {
            if ( (i % 2) == 0 )
            {
                lm.releaseWriteLock( entity );
            }
            else
            {
                lm.releaseReadLock( entity );
            }
        }
    }

    private class HelperThread extends Thread
    {
        private static final long MAX_WAIT_LOOPS = 40;

        private static final int DO_NOTHING_TASK = 0;
        private static final int GET_READLOCK_TASK = 1;
        private static final int GET_WRITELOCK_TASK = 2;
        private static final int RELEASE_READLOCK_TASK = 3;
        private static final int RELEASE_WRITELOCK_TASK = 4;
        private static final int QUIT_TASK = 5;

        private String name = null;
        private int nextTask = 0;
        private boolean taskCompleted = true;
        private Object resource = null;

        HelperThread( String name )
        {
            super();
            this.name = name;
        }

        public void run()
        {
            try
            {
                while ( nextTask != QUIT_TASK )
                {
                    switch ( nextTask )
                    {
                        case DO_NOTHING_TASK:
                            synchronized ( this )
                            {
                                wait( 15 );
                            }
                            break;
                        case GET_READLOCK_TASK:
                            lm.getReadLock( resource );
                            taskCompleted = true;
                            nextTask = DO_NOTHING_TASK;
                            break;
                        case RELEASE_READLOCK_TASK:
                            lm.releaseReadLock( resource );
                            taskCompleted = true;
                            nextTask = DO_NOTHING_TASK;
                            break;
                        case GET_WRITELOCK_TASK:
                            lm.getWriteLock( resource );
                            taskCompleted = true;
                            nextTask = DO_NOTHING_TASK;
                            break;
                        case RELEASE_WRITELOCK_TASK:
                            lm.releaseWriteLock( resource );
                            taskCompleted = true;
                            nextTask = DO_NOTHING_TASK;
                            break;
                        default:
                            throw new RuntimeException( "Argh" );
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
            int count = 0;
            while ( !taskCompleted )
            {
                if ( count > MAX_WAIT_LOOPS )
                {
                    throw new RuntimeException( "Task timed out" );
                }

                try
                {
                    wait( 25 );
                }
                catch ( InterruptedException e )
                {
                }
                count++;
            }
        }

        boolean isTaskCompleted()
        {
            return taskCompleted;
        }

        synchronized void getReadLock( Object resource )
        {
            if ( !taskCompleted )
            {
                throw new RuntimeException( "Task not completed" );
            }
            taskCompleted = false;
            this.resource = resource;
            nextTask = GET_READLOCK_TASK;
        }

        synchronized void releaseReadLock( Object resource )
        {
            if ( !taskCompleted )
            {
                throw new RuntimeException( "Task not completed" );
            }
            taskCompleted = false;
            this.resource = resource;
            nextTask = RELEASE_READLOCK_TASK;
        }

        synchronized void getWriteLock( Object resource )
        {
            if ( !taskCompleted )
            {
                throw new RuntimeException( "Task not completed" );
            }
            taskCompleted = false;
            this.resource = resource;
            nextTask = GET_WRITELOCK_TASK;
        }

        synchronized void releaseWriteLock( Object resource )
        {
            if ( !taskCompleted )
            {
                throw new RuntimeException( "Task not completed" );
            }
            taskCompleted = false;
            this.resource = resource;
            nextTask = RELEASE_WRITELOCK_TASK;
        }

        void quit()
        {
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

    public void testMultipleThreads()
    {
        HelperThread t1 = new HelperThread( "T1" );
        HelperThread t2 = new HelperThread( "T2" );
        HelperThread t3 = new HelperThread( "T3" );
        HelperThread t4 = new HelperThread( "T4" );
        ResourceObject r1 = new ResourceObject( "R1" );
        try
        {
            t1.start();
            t2.start();
            t3.start();
            t4.start();

            t1.getReadLock( r1 );
            t1.waitForCompletionOfTask();
            t2.getReadLock( r1 );
            t2.waitForCompletionOfTask();
            t3.getReadLock( r1 );
            t3.waitForCompletionOfTask();
            t4.getWriteLock( r1 );
            t3.releaseReadLock( r1 );
            t3.waitForCompletionOfTask();
            t2.releaseReadLock( r1 );
            t2.waitForCompletionOfTask();
            assertTrue( !t4.isTaskCompleted() );
            t1.releaseReadLock( r1 );
            t1.waitForCompletionOfTask();
            // now we can wait for write lock since it can be acquired
            // get write lock
            t4.waitForCompletionOfTask();
            t4.getReadLock( r1 );
            t4.waitForCompletionOfTask();
            t4.getReadLock( r1 );
            t4.waitForCompletionOfTask();
            // put readlock in queue
            t1.getReadLock( r1 );
            t4.getReadLock( r1 );
            t4.waitForCompletionOfTask();
            t4.releaseReadLock( r1 );
            t4.waitForCompletionOfTask();
            t4.getWriteLock( r1 );
            t4.waitForCompletionOfTask();
            t4.releaseWriteLock( r1 );
            t4.waitForCompletionOfTask();
            assertTrue( !t1.isTaskCompleted() );
            t4.releaseWriteLock( r1 );
            t4.waitForCompletionOfTask();
            // get read lock
            t1.waitForCompletionOfTask();
            t4.releaseReadLock( r1 );
            t4.waitForCompletionOfTask();
            // t4 now has 1 readlock and t1 one readlock
            // let t1 drop readlock and t4 get write lock
            t4.getWriteLock( r1 );
            t1.releaseReadLock( r1 );
            t1.waitForCompletionOfTask();
            t4.waitForCompletionOfTask();

            t4.releaseReadLock( r1 );
            t4.waitForCompletionOfTask();
            t4.releaseWriteLock( r1 );
            t4.waitForCompletionOfTask();

            t4.getWriteLock( r1 );
            t4.waitForCompletionOfTask();
            t1.getReadLock( r1 );
            t2.getReadLock( r1 );
            t3.getReadLock( r1 );
            t4.getReadLock( r1 );
            t4.waitForCompletionOfTask();
            t4.releaseWriteLock( r1 );
            t4.waitForCompletionOfTask();
            t1.waitForCompletionOfTask();
            t2.waitForCompletionOfTask();
            t3.waitForCompletionOfTask();

            t1.getWriteLock( r1 );
            t2.releaseReadLock( r1 );
            t2.waitForCompletionOfTask();
            t4.releaseReadLock( r1 );
            t4.waitForCompletionOfTask();
            t3.releaseReadLock( r1 );
            t3.waitForCompletionOfTask();

            t1.waitForCompletionOfTask();
            t1.releaseWriteLock( r1 );
            t1.waitForCompletionOfTask();
            t2.getReadLock( r1 );
            t2.waitForCompletionOfTask();
            t1.releaseReadLock( r1 );
            t1.waitForCompletionOfTask();
            t2.getWriteLock( r1 );
            t2.waitForCompletionOfTask();
            t2.releaseWriteLock( r1 );
            t2.waitForCompletionOfTask();
            t2.releaseReadLock( r1 );
            t2.waitForCompletionOfTask();
        }
        catch ( Exception e )
        {
            lm.dumpLocksOnResource( r1 );
            e.printStackTrace();
            fail( "Multiple thread rw lock test failed, " + e );
        }
        finally
        {
            t1.quit();
            t2.quit();
            t3.quit();
            t4.quit();
        }
    }

    private static boolean go = false;

    public class StressThread extends Thread
    {
        private java.util.Random rand = new java.util.Random( System
            .currentTimeMillis() );
        private final Object READ = new Object();
        private final Object WRITE = new Object();

        private String name;
        private int numberOfIterations;
        private int depthCount;
        private float readWriteRatio;
        private Object resource;

        StressThread( String name, int numberOfIterations, int depthCount,
            float readWriteRatio, Object resource )
        {
            super();
            this.name = name;
            this.numberOfIterations = numberOfIterations;
            this.depthCount = depthCount;
            this.readWriteRatio = readWriteRatio;
            this.resource = resource;
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
                try
                {
                    for ( int i = 0; i < numberOfIterations; i++ )
                    {
                        int depth = depthCount;
                        do
                        {
                            float f = rand.nextFloat();
                            if ( f < readWriteRatio )
                            {
                                lm.getReadLock( resource );
                                lockStack.push( READ );
                            }
                            else
                            {
                                lm.getWriteLock( resource );
                                lockStack.push( WRITE );
                            }
                        }
                        while ( --depth > 0 );

                        while ( !lockStack.isEmpty() )
                        {
                            if ( lockStack.pop() == READ )
                            {
                                lm.releaseReadLock( resource );
                            }
                            else
                            {
                                lm.releaseWriteLock( resource );
                            }
                        }
                    }
                }
                catch ( DeadlockDetectedException e )
                {
                    // System.out.println( "#############Deadlock detected!" );
                }
                finally
                {
                    while ( !lockStack.isEmpty() )
                    {
                        if ( lockStack.pop() == READ )
                        {
                            lm.releaseReadLock( resource );
                        }
                        else
                        {
                            lm.releaseWriteLock( resource );
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
        ResourceObject r1 = new ResourceObject( "R1" );
        Thread stressThreads[] = new Thread[100];
        go = false;
        for ( int i = 0; i < 100; i++ )
        {
            stressThreads[i] = new StressThread( "Thread" + i, 100, 9, 0.50f,
                r1 );
        }
        for ( int i = 0; i < 100; i++ )
        {
            stressThreads[i].start();
        }
        go = true;
    }
}