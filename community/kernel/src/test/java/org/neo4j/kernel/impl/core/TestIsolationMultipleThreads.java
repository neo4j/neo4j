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
package org.neo4j.kernel.impl.core;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.test.TestGraphDatabaseFactory;

/**
 * Test atomicity of Neo4j. How to get consistent results with or without locks?
 */
@Ignore( "unstable" )
public class TestIsolationMultipleThreads
{
    GraphDatabaseService database;

    private static final int COUNT = 1000;

    @Before
    public void setup()
    {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try ( Transaction tx = database.beginTx() )
        {
            for ( int i = 0; i < COUNT; i++ )
            {
                Node node = database.createNode();
                node.setProperty( "foo", 0 );
            }

            tx.success();
        }
    }

    @After
    public void tearDown()
    {
        database.shutdown();
    }

    /**
     * This test shows what happens with no isolation, i.e. default usage of Neo4j. One thread updates
     * a property "foo" on 1000 nodes by increasing it by 1 in each round. Another thread reads the
     * first and last node and computes the difference. With perfect isolation the result should be 0.
     *
     * Here the result is that roughly 5% of the time the result is not 0, since the reading thread will
     * see changes from the other thread midway through its calculation.
     *
     * @throws Exception
     */
    @Test
    public void testIsolation()
        throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool( 1 );

        final AtomicBoolean done = new AtomicBoolean( false );
        
        executor.submit( new DataChecker( done, database ) );

        new DataChanger( database, COUNT, done ).call();
    }

    /**
     *
     * This test does the same thing, but acquires read locks on BOTH nodes before reading the value.
     * This ensures that it will wait for the write transaction to finish, and so no errors are detected.
     *
     * @throws Exception
     */
    @Test
    public void testIsolationWithLocks()
        throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool( 2 );

        final AtomicBoolean done = new AtomicBoolean( false );

        executor.submit( new DataChecker( done, database )
        {
            @Override
            protected Integer getSecondValue()
            {
                Node nodeById = database.getNodeById( 1000 );
                this.tx.acquireReadLock( nodeById );
                return (Integer) nodeById.getProperty( "foo" );
            }

            @Override
            protected Integer getFirstValue()
            {
                Node nodeById = database.getNodeById( 1 );
                this.tx.acquireReadLock( nodeById );
                return (Integer) nodeById.getProperty( "foo" );
            }
        });

        new DataChanger( database, COUNT, done ).call();
    }

    /**
     * This test does the same thing as the previous one, but acquires the nodes
     * in the reverse order. The result is a consistent deadlock for the writer, which
     * is unable to proceed, even with deadlock handling and retries.
     *
     * @throws Exception
     */
    @Test(expected = DeadlockDetectedException.class)
    public void testIsolationWithLocksReversed()
        throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool( 2 );

        final AtomicBoolean done = new AtomicBoolean( false );

        executor.submit( new DataChecker( done, database )
        {
            @Override
            protected Integer getSecondValue()
            {
                Node nodeById = database.getNodeById( 1 );
                this.tx.acquireReadLock( nodeById );
                return (Integer) nodeById.getProperty( "foo" );
            }

            @Override
            protected Integer getFirstValue()
            {
                Node nodeById = database.getNodeById( 1000 );
                this.tx.acquireReadLock( nodeById );
                return (Integer) nodeById.getProperty( "foo" );
            }
        });

        new DataChanger( database, COUNT, done ).call();

        executor.shutdownNow();
    }

    /**
     *
     * This test does the same thing, but acquires read locks on BOTH nodes before reading the value.
     * The locks are released after reading the value.
     *
     * This gives 0% errors in my tests.
     *
     * @throws Exception
     */
    @Test
    public void testIsolationWithShortLocks()
        throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool( 1 );

        final AtomicBoolean done = new AtomicBoolean( false );

        executor.submit( new DataChecker( done, database )
        {
            @Override
            protected Integer getSecondValue()
            {
                Node nodeById = database.getNodeById( 1000 );
                Lock lock = this.tx.acquireReadLock( nodeById );
                try
                {
                    return (Integer) nodeById.getProperty( "foo" );
                }
                finally
                {
                    lock.release();
                }
            }

            @Override
            protected Integer getFirstValue()
            {
                Node nodeById = database.getNodeById( 1 );
                Lock lock = this.tx.acquireReadLock( nodeById );
                try
                {
                    return (Integer) nodeById.getProperty( "foo" );
                }
                finally
                {
                    lock.release();
                }
            }
        });

        new DataChanger( database, COUNT, done ).call();

        executor.shutdownNow();
    }

    /**
     *
     * This test does the same thing, but acquires read locks on BOTH nodes before reading the value, in reverse.
     * The locks are released after reading the value.
     *
     * This gives roughly 60-90%+ errors in my tests.
     *
     * @throws Exception
     */
    @Test
    public void testIsolationWithShortLocksReversed()
        throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool( 2 );

        final AtomicBoolean done = new AtomicBoolean( false );

        executor.submit( new DataChecker( done, database )
        {
            @Override
            protected Integer getSecondValue()
            {
                Node nodeById = database.getNodeById( 1 );
                Lock lock = this.tx.acquireReadLock( nodeById );
                try
                {
                    return (Integer) nodeById.getProperty( "foo" );
                }
                finally
                {
                    lock.release();
                }
            }

            @Override
            protected Integer getFirstValue()
            {
                Node nodeById = database.getNodeById( 1000 );
                Lock lock = this.tx.acquireReadLock( nodeById );
                try
                {
                    return (Integer) nodeById.getProperty( "foo" );
                }
                finally
                {
                    lock.release();
                }
            }
        });

        new DataChanger( database, COUNT, done ).call();

        executor.shutdownNow();
    }

    /**
     * This test shows what happens with no isolation, i.e. default usage of Neo4j. One thread updates
     * a property "foo" on 1000 nodes by increasing it by 1 in each round. Another thread reads the
     * property on all nodes and computes the total difference from expected value. With perfect isolation the result should be 0.
     *
     * This will always yield a result different from 0.
     *
     * @throws Exception
     */
    @Test
    public void testIsolationAll()
        throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool( 1 );

        final AtomicBoolean done = new AtomicBoolean( false );

        executor.submit( new DataChecker2( COUNT, done, database ) );

        new DataChanger( database, COUNT, done ).call();

        executor.shutdownNow();
    }

    /**
     * This test does the same as above, but now read locks nodes before calculating the diff.
     *
     * This will always yield a result of 0, i.e. correct.
     *
     * @throws Exception
     */
    @Test
    public void testIsolationAllWithLocks()
        throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool( 1 );

        final AtomicBoolean done = new AtomicBoolean( false );

        executor.submit( new DataChecker2( COUNT, done, database )
        {
            @Override
            protected int getNodeValue( int i )
            {
                Node node = database.getNodeById( i+1 );
                this.tx.acquireReadLock( node );
                return (Integer) node.getProperty( "foo" );
            }
        });

        new DataChanger( database, COUNT, done ).call();

        executor.shutdownNow();
    }

    /**
     * This test does the same as above, but now locks the nodes in the opposite order.
     *
     * This will always yield a DeadlockDetectedException. Retries does not help.
     *
     * @throws Exception
     */
    @Test(expected = DeadlockDetectedException.class)
    public void testIsolationAllWithLocksReverse()
        throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool( 1 );

        final AtomicBoolean done = new AtomicBoolean( false );

        executor.submit( new DataChecker2( COUNT, done, database )
        {
            @Override
            protected int getNodeValue( int i )
            {
                Node node = database.getNodeById( 1000-i );
                this.tx.acquireReadLock( node );
                return (Integer) node.getProperty( "foo" );
            }
        });

        new DataChanger( database, COUNT, done ).call();

        executor.shutdownNow();
    }

    private static class DataChanger
        implements Callable
    {
        private final GraphDatabaseService database;
        private final int count;
        private final AtomicBoolean done;

        public DataChanger( GraphDatabaseService database, int count, AtomicBoolean done )
        {
            this.database = database;
            this.count = count;
            this.done = done;
        }

        @Override
        public Object call()
            throws Exception
        {
            System.out.println( "Start changing data" );
            int totalDeadlocks = 0;
            try
            {
                for (int round = 0; round < 100; round++)
                {
                    int deadLocks = 0;
                    DeadlockDetectedException ex = null;
                    do
                    {
                        ex = null;
                        try ( Transaction tx = database.beginTx() )
                        {
                            for (int i = 0; i < count; i++)
                            {
                                Node node = database.getNodeById( i+1 );
                                int foo = (Integer) node.getProperty( "foo" );
                                node.setProperty( "foo", foo+1 );
                            }

                            tx.success();
                        }
                        catch( DeadlockDetectedException e )
                        {
                            System.out.println("Deadlock detected");
                            deadLocks = deadLocks+1;
                            ex = e;



                            if (deadLocks > 100)
                            {
                                totalDeadlocks += deadLocks;
                                throw e;
                            }
                        }
                    } while (ex != null);

                    totalDeadlocks += deadLocks;
                }
            }
            catch( Exception e )
            {
                e.printStackTrace();
                throw e;
            } finally
            {
                done.set( true );
                System.out.printf( "Done changing data. Detected %d deadlocks\n", totalDeadlocks );
            }

            return null;
        }
    }

    private static class DataChecker
        implements Runnable
    {
        private final AtomicBoolean done;
        private final GraphDatabaseService database;
        protected Transaction tx;

        public DataChecker( AtomicBoolean done, GraphDatabaseService database )
        {
            this.done = done;
            this.database = database;
        }

        @Override
        public void run()
        {
                System.out.println( "Start checking data" );
                double errors = 0;
                double total = 0;
                while(!done.get())
                {
                    try ( Transaction transaction = database.beginTx() )
                    {
                        int firstNode = getFirstValue();
                        int lastNode = getSecondValue();
                        if (firstNode - lastNode != 0)
                        {
                            errors++;
                        }
                        total++;

                        tx.success();
                    }
                }
                double percentage = (errors/total)*100.0;
                System.out.printf( "Done checking data, %1.0f errors found(%1.3f%%)\n", errors, percentage );
        }

        protected Integer getSecondValue()
        {
            return (Integer) database.getNodeById( 1000 ).getProperty( "foo" );
        }

        protected Integer getFirstValue()
        {
            return (Integer) database.getNodeById( 1 ).getProperty( "foo" );
        }
    }

    private static class DataChecker2
        implements Runnable
    {
        private final int count;
        private final AtomicBoolean done;
        private final GraphDatabaseService database;
        protected Transaction tx;

        public DataChecker2( int count, AtomicBoolean done, GraphDatabaseService database )
        {
            this.count = count;
            this.done = done;
            this.database = database;
        }

        @Override
        public void run()
        {
                System.out.println( "Start checking data" );
                int totalDiff = 0;
                while(!done.get())
                {
                    try ( Transaction tx = database.beginTx() )
                    {
                        int correctValue = -1;
                        int diff = 0;

                        for (int i = 0; i < count; i++)
                        {
                            int foo = getNodeValue( i );

                            if (correctValue == -1)
                                correctValue = foo;

                            diff = diff + foo - correctValue;
                        }

                        totalDiff += diff;
                        tx.success();
                    }
                    catch( Exception e )
                    {
                        e.printStackTrace();
                        tx.failure();
                    }

                }
                System.out.printf( "Done checking data, %d diff\n", totalDiff );
        }

        protected int getNodeValue( int i )
        {
            Node node = database.getNodeById( i+1 );

            return (Integer) node.getProperty( "foo" );
        }
    }
}
