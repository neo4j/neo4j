/**
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.UniqueFactory;

public class GetOrCreateTest extends AbstractJavaDocTestbase
{
    interface GetOrCreate
    {
        Node getOrCreateUser( String username, GraphDatabaseService graphDb, Node lockNode );
    }

    class PessimisticGetOrCreate implements GetOrCreate
    {
        @Override
        public Node getOrCreateUser( String username, GraphDatabaseService graphDb, Node lockNode )
        {
            return getOrCreateUserPessimistically( username, graphDb, lockNode );
        }
    }

    class UniqueFactoryGetOrCreate implements GetOrCreate
    {
        @Override
        public Node getOrCreateUser( String username, GraphDatabaseService graphDb, Node lockNode )
        {
            return getOrCreateUserWithUniqueFactory( username, graphDb );
        }
    }

    class ThreadRunner implements Runnable
    {
        public static final int NUM_USERS = 1000;
        final GetOrCreate impl;

        ThreadRunner( GetOrCreate impl )
        {
            this.impl = impl;
        }

        private Node createNode()
        {
            Transaction tx = graphdb().beginTx();
            try
            {
                final Node node = graphdb().createNode();
                tx.success();
                return node;
            }
            finally
            {
                tx.finish();
            }
        }

        @Override
        public void run()
        {
            final Node lockNode = createNode();
            final List<List<Node>> results = new ArrayList<List<Node>>();
            final List<Thread> threads = new ArrayList<Thread>();
            final AtomicReference<RuntimeException> failure = new AtomicReference<RuntimeException>();
            for ( int i = 0; i < Runtime.getRuntime().availableProcessors()*2; i++ )
            {
                threads.add( new Thread( GetOrCreateTest.class.getSimpleName() + " thread " + i )
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            List<Node> subresult = new ArrayList<Node>();
                            for ( int j = 0; j < NUM_USERS; j++ )
                            {
                                subresult.add( impl.getOrCreateUser( getUsername( j ), graphdb(), lockNode ) );
                            }
                            results.add( subresult );
                        }
                        catch ( RuntimeException e )
                        {
                            failure.compareAndSet( null, e );
                            throw e;
                        }
                    }
                } );
            }
            for ( Thread thread : threads )
            {
                thread.start();
            }
            for ( Thread thread : threads )
            {
                try
                {
                    thread.join();
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }
            }

            if ( failure.get() != null )
                throw failure.get();

            List<Node> first = results.remove( 0 );
            for ( List<Node> subresult : results )
            {
                assertEquals( first, subresult );
            }
            for ( int i = 0; i < NUM_USERS; i++ )
            {
                final String username = getUsername( i );
                impl.getOrCreateUser( username, graphdb(), lockNode );
                assertUserExistsUniquely( username );
            }
        }

        private String getUsername( int j )
        {
            return "User" + j;
        }

        private void assertUserExistsUniquely( String username )
        {
            try
            {
                assertNotNull( "User '" + username + "' not created.",
                    graphdb().index().forNodes( "users" ).get( "name", username ).getSingle() );
            }
            catch ( NoSuchElementException e )
            {
                throw new RuntimeException( "User '" + username + "' not created uniquely.", e );
            }
        }
    }

    @Test
    public void testPessimisticLocking() throws InterruptedException
    {
        new ThreadRunner( new PessimisticGetOrCreate() ).run();
    }

    // START SNIPPET: pessimisticLocking
    public Node getOrCreateUserPessimistically( String username, GraphDatabaseService graphDb, Node lockNode )
    {
        Index<Node> usersIndex = graphDb.index().forNodes( "users" );
        Node userNode = usersIndex.get( "name", username ).getSingle();
        if ( userNode != null ) return userNode;
        Transaction tx = graphDb.beginTx();
        try
        {
            tx.acquireWriteLock( lockNode );
            userNode = usersIndex.get( "name", username ).getSingle();
            if ( userNode == null )
            {
                userNode = graphDb.createNode();
                userNode.setProperty( "name", username );
                usersIndex.add( userNode, "name", username );
            }
            tx.success();
            return userNode;
        }
        finally
        {
            tx.finish();
        }
    }
    // END SNIPPET: pessimisticLocking

    @Test
    public void getOrCreateWithUniqueFactory() throws Exception
    {
        new ThreadRunner( new UniqueFactoryGetOrCreate() ).run();
    }

    // START SNIPPET: getOrCreate
    public Node getOrCreateUserWithUniqueFactory( String username, GraphDatabaseService graphDb )
    {
        UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory( graphDb, "users" )
        {
            @Override
            protected void initialize( Node created, Map<String, Object> properties )
            {
                created.setProperty( "name", properties.get( "name" ) );
            }
        };

        return factory.getOrCreate( "name", username );
    }
    // END SNIPPET: getOrCreate
}
