/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ExecutorService;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Pair;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Thread.interrupted;
import static java.lang.Thread.sleep;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.neo4j.test.TargetDirectory.forTest;

@Ignore( "Good for driving out problems with loading/heaviness of property records" )
public class ConcurrentPropertyAccessIT
{
    @Test
    public void tryTriggerIssueWithConcurrentlySettingAndReadingProperties() throws Exception
    {
        // GIVEN
        ExecutorService executor = newCachedThreadPool();
        executor.submit( new SetPropertyWorker() );
        executor.submit( new RemovePropertyWorker() );
        executor.submit( new GetPropertyWorker() );
        executor.submit( new ReplaceNodeWorker() );

        waitIndefinitely();

        // THEN
    }
    
    private void waitIndefinitely()
    {
        while ( true )
        {
            try
            {
                sleep( MAX_VALUE );
            }
            catch ( InterruptedException e )
            {
                interrupted();
                // meh
            }
        }
    }

    private GraphDatabaseService db;
    private Node[] nodes;

    protected Pair<Integer, Node> getNode( Random random, boolean takeOut )
    {
        synchronized ( nodes )
        {
            while ( true )
            {
                int index = random.nextInt( nodes.length );
                Node node = nodes[index];
                if ( null != node )
                {
                    if ( takeOut )
                        nodes[index] = null;
                    return Pair.of( index, node );
                }
            }
        }
    }

    protected void setNode( int i, Node node )
    {
        synchronized ( nodes )
        {
            nodes[i] = node;
        }
    }

    private abstract class Worker implements Runnable
    {
        protected final Random random = new Random();
        
        @Override
        public void run()
        {
            while ( true )
            {
                Transaction tx = db.beginTx();
                try
                {
                    doSomething();
                    tx.success();
                }
                catch ( Throwable t )
                {
                    t.printStackTrace(System.err);
                    System.err.flush();
                    // throw Exceptions.launderedException( t );
                }
                finally
                {
                    tx.finish();
                }
            }
        }

        protected abstract void doSomething() throws Throwable;
    }
    
    private class SetPropertyWorker extends Worker
    {
        @Override
        protected void doSomething() throws Throwable
        {
            Pair<Integer, Node> pair = getNode( random, false );
            Node node = pair.other();
            node.setProperty( randomPropertyKey( random ), randomLongPropertyValue( random.nextInt( 8 ) + 2,  random ) );
        }
    }
    
    private class GetPropertyWorker extends Worker
    {
        @Override
        protected void doSomething() throws Throwable
        {
            Pair<Integer, Node> pair = getNode( random, false );
            Node node = pair.other();
            node.getProperty( randomPropertyKey( random ), null );
        }
    }
    
    private class RemovePropertyWorker extends Worker
    {
        @Override
        protected void doSomething() throws Throwable
        {
            Pair<Integer, Node> pair = getNode( random, false );
            Node node = pair.other();
            node.removeProperty( randomPropertyKey( random ) );
        }
    }

    private class ReplaceNodeWorker extends Worker
    {

        @Override
        protected void doSomething() throws Throwable
        {
            Pair<Integer, Node> pair = getNode( random, true );
            int index = pair.first();
            Node node = pair.other();
            node.delete();
            setNode( index, db.createNode() );
        }
    }
    
    private Object randomLongPropertyValue( int length, Random random )
    {
        String[] parts = new String[] { "bozo", "bimbo", "basil", "bongo" };
        StringBuilder result = new StringBuilder( 4 * length );
        for ( int i = 0; i < length; i ++ )
        {
            result.append( parts[ random.nextInt( parts.length )] );
        }
        return result.toString();
    }
    
    private String randomPropertyKey( Random random )
    {
        return random.nextBoolean() ? "name" : "animals";
    }

    @Before
    public void before() throws Exception
    {
        String storeDir = forTest( getClass() ).makeGraphDbDir().getAbsolutePath();
        db = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        nodes = createInitialNodes();
    }

    private Node[] createInitialNodes()
    {
        Node[] nodes = new Node[100];
        Transaction tx = db.beginTx();
        try
        {
            for ( int i = 0; i < nodes.length; i++ )
            {
                nodes[i] = db.createNode();
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        return nodes;
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }
}
