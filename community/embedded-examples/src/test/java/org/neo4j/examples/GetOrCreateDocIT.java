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

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.UniqueFactory;

public class GetOrCreateDocIT extends AbstractJavaDocTestbase
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
        static final int NUM_USERS = 10;
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
            final List<GetOrCreateTask> threads = new ArrayList<GetOrCreateTask>();
            int numThreads = Runtime.getRuntime().availableProcessors()*2;
            for ( int i = 0; i < numThreads; i++ )
            {
                threads.add( new GetOrCreateTask( db, lockNode, NUM_USERS, impl,
                        GetOrCreateDocIT.class.getSimpleName() + " thread " + i ) );
            }
            for ( Thread thread : threads )
            {
                thread.start();
            }
            
            RuntimeException failure = null;
            List<List<Node>> results = new ArrayList<List<Node>>();
            for ( GetOrCreateTask thread : threads )
            {
                try
                {
                    thread.join();
                    if ( failure == null )
                        failure = thread.failure;
                    
                    results.add( thread.result );
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }
            }
            
            if ( failure != null )
                throw failure;

            assertEquals( numThreads, results.size() );
            List<Node> firstResult = results.remove( 0 );
            for ( List<Node> subresult : results )
            {
                assertEquals( firstResult, subresult );
            }
            for ( int i = 0; i < NUM_USERS; i++ )
            {
                final String username = getUsername( i );
                impl.getOrCreateUser( username, graphdb(), lockNode );
                assertUserExistsUniquely( username );
            }
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
    
    private static String getUsername( int j )
    {
        return "User" + j;
    }
    
    private static class GetOrCreateTask extends Thread
    {
        private final GraphDatabaseService db;
        private final Node lockNode;
        private final int numUsers;
        private final GetOrCreate impl;
        volatile List<Node> result;
        volatile RuntimeException failure;
        
        GetOrCreateTask( GraphDatabaseService db, Node lockNode, int numUsers, GetOrCreate impl, String name )
        {
            super( name );
            this.db = db;
            this.lockNode = lockNode;
            this.numUsers = numUsers;
            this.impl = impl;
        }
        
        @Override
        public void run()
        {
            try
            {
                List<Node> subresult = new ArrayList<Node>();
                for ( int j = 0; j < numUsers; j++ )
                {
                    subresult.add( impl.getOrCreateUser( getUsername( j ), db, lockNode ) );
                }
                this.result = subresult;
            }
            catch ( RuntimeException e )
            {
                failure = e;
                throw e;
            }
        }
    }

    @Test
    public void testPessimisticLocking() throws InterruptedException
    {
        new ThreadRunner( new PessimisticGetOrCreate() ).run();
    }

    // START SNIPPET: pessimisticLocking
    public Node getOrCreateUserPessimistically( String username, GraphDatabaseService graphDb, 
                                                Node lockNode )
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
