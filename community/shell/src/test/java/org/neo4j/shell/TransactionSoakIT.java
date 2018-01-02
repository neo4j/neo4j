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
package org.neo4j.shell;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.impl.CollectingOutput;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterables.count;

/**
 * This exposes a two layered issue. In both cases, it crashes because:
 *
 * Thread A: Create node 1
 * Thread A: Create relationship from node 1 to another node (reads node 1)
 * Thread B: Read node 1
 * Thread A: Rollback
 * Thread B: Read node 1 properties -> Crash
 *
 * The first layer is that create node adds the node to the global cache. This should be easy to fix. However,
 * that does not resolve the issue.
 *
 * The second layer is that when an item is not found in cache, it is loaded from disk. The disk loading code
 * takes into account any records changed in the current transaction. Because of that, when Thread A creates a
 * relationship, it will load node 1 from disk, which will find the record for node 1 in the list of records created
 * or changed by Thread A, and thus find the data needed to put the (yet-to-be-committed) node 1 in the global cache.
 * See WriteTransaction#nodeLoadLight for details.
 *
 * The fix for this is to stop puttin nodes in the global cache in transactions and;
 * To move createNode et cetera into the kernel api and use *only* in-memory transaction state to work with these
 * un-created nodes (and relationships, for that matter).
 */
@Ignore("Jake 2013-09-13: Exposes bug that will be fixed by kernel API")
public class TransactionSoakIT
{
    protected GraphDatabaseAPI db;
    private ShellServer server;
    private final Random r = new Random( System.currentTimeMillis() );

    @Before
    public void doBefore() throws Exception
    {
        db = (GraphDatabaseAPI)new TestGraphDatabaseFactory().newImpermanentDatabase();
        server = new GraphDatabaseShellServer( db );
    }

    @After
    public void doAfter() throws Exception
    {
        server.shutdown();
        db.shutdown();
    }

    @Test
    public void multiThreads() throws Exception
    {
        List<Tester> testers = createTesters();
        List<Thread> threads = startTesters( testers );

        Thread.sleep( 10000 );

        stopTesters( testers );
        waitForThreadsToFinish( threads );

        try ( Transaction tx = db.beginTx() )
        {
            long relationshipCount = count( GlobalGraphOperations.at( db ).getAllRelationships() );
            int expected = committerCount( testers );
    
            assertEquals( expected, relationshipCount );
        }
    }

    private List<Tester> createTesters() throws Exception
    {
        List<Tester> testers = new ArrayList<>( 20 );
        for ( int i = 0; i < 20; i++ )
        {
            int x = r.nextInt( 3 );

            Tester t;
            if ( x == 0 )
            {
                t = new Reader("Reader-" + i);
            } else if ( x == 1 )
            {
                t = new Committer("Committer-" + i);
            } else if ( x == 2 )
            {
                t = new Rollbacker("Rollbacker-" + i);
            } else
            {
                throw new Exception( "oh noes" );
            }

            testers.add( t );
        }

        return testers;
    }

    private int committerCount( List<Tester> testers )
    {
        int count = 0;
        for ( Tester t : testers )
        {
            if ( t instanceof Committer )
            {
                Committer c = (Committer) t;
                count = count + c.count;
            }
        }
        return count;
    }

    private void waitForThreadsToFinish( List<Thread> threads ) throws InterruptedException
    {
        for ( Thread t : threads )
        {
            t.join();
        }
    }

    private void stopTesters( List<Tester> testers ) throws Exception
    {
        for ( Tester t : testers )
        {
            t.die();
        }
    }

    private List<Thread> startTesters( List<Tester> testers )
    {
        List<Thread> threads = new ArrayList<Thread>();

        for ( Tester t : testers )
        {
            Thread thread = new Thread( t, t.name() );
            thread.start();
            threads.add( thread );
        }
        return threads;
    }


    private class Reader extends Tester
    {
        public Reader( String name )
        {
            super(name);
        }

        @Override
        protected void doStuff() throws Exception
        {
            execute( "match n return count(n.name);" );
        }
    }

    private class Committer extends Tester
    {
        private int count = 0;

        private Committer( String name )
        {
            super( name );
        }

        @Override
        protected void doStuff() throws Exception
        {
            execute( "begin transaction" );
            execute( "create (a {name:'a'}), (b {name:'b'}), a-[:LIKES]->b;" );
            execute( "commit" );
            count = count + 1;
        }
    }

    private class Rollbacker extends Tester
    {
        private Rollbacker( String name )
        {
            super( name );
        }

        @Override
        protected void doStuff() throws Exception
        {
            execute( "begin transaction" );
            execute( "create (a {name:'a'}), (b {name:'b'}), a-[:LIKES]->b;" );
            execute( "rollback" );
        }
    }

    private abstract class Tester implements Runnable
    {
        private final String name;
        ShellClient client;
        private boolean alive = true;
        private Exception exception = null;

        protected Tester( String name )
        {
            this.name = name;
            try
            {
                client = new SameJvmClient( new HashMap<String, Serializable>(), server,
                                            new SilentLocalOutput(), InterruptSignalHandler.getHandler() );
            }
            catch ( ShellException e )
            {
                throw new RuntimeException( "Error starting client", e );
            }
        }

        protected void execute( String cmd ) throws Exception
        {
            executeCommand( server, client, cmd );
            Thread.sleep( r.nextInt( 10 ) );
        }

        @Override
        public void run()
        {
            try
            {
                while ( alive )
                {
                    doStuff();
                }
            } catch ( Exception e )
            {
                alive = false;
                exception = e;
            }
        }

        public void die() throws Exception
        {
            if ( exception != null )
            {
                throw exception;
            }
            alive = false;
        }

        protected abstract void doStuff() throws Exception;

        private String name()
        {
            return name;
        }
    }


    public void executeCommand( ShellServer server, ShellClient client, String command ) throws Exception
    {
        CollectingOutput output = new CollectingOutput();
        server.interpretLine( client.getId(), command, output );
    }
}
