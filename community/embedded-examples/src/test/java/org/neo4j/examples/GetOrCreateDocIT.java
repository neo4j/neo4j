/*
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

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GetOrCreateDocIT extends AbstractJavaDocTestBase
{
    @BeforeClass
    public static void init()
    {
        db = new TestGraphDatabaseFactory().newEmbeddedDatabase(
                TargetDirectory.forTest( GetOrCreateDocIT.class ).makeGraphDbDir().getAbsolutePath()
             );
    }

    abstract class GetOrCreate<D>
    {
        abstract Node getOrCreateUser( String username, GraphDatabaseService graphDb, D dependency );

        void assertUserExistsUniquely( GraphDatabaseService graphDb, Transaction tx, String username )
        {
            assertUserExistsUniquelyInGraphDb( graphDb, tx, username );
        }
    }

    class PessimisticGetOrCreate extends GetOrCreate<Node>
    {
        @Override
        public Node getOrCreateUser( String username, GraphDatabaseService graphDb, Node lockNode )
        {
            return getOrCreateUserPessimistically( username, graphDb, lockNode );
        }
    }

    class UniqueFactoryGetOrCreate extends GetOrCreate<UniqueFactory<Node>>
    {
        @Override
        public Node getOrCreateUser( String username, GraphDatabaseService graphDb, UniqueFactory<Node> uniqueFactory )
        {
            return getOrCreateUserWithUniqueFactory( username, graphDb, uniqueFactory );
        }

        @Override
        void assertUserExistsUniquely( GraphDatabaseService graphDb, Transaction tx, String username )
        {
            super.assertUserExistsUniquely( graphDb, tx, username );    //To change body of overridden methods use
            assertUserExistsUniquelyInIndex( graphDb, tx, username );
        }
    }

    class CypherGetOrCreate extends GetOrCreate<GraphDatabaseService>
    {
        @Override
        public Node getOrCreateUser( String username, GraphDatabaseService graphDb, GraphDatabaseService engine )
        {
            return getOrCreateWithCypher( username, graphDb );
        }
    }

    abstract class ThreadRunner<D> implements Runnable
    {
        static final int NUM_USERS = 100;
        final GetOrCreate<D> impl;
        private final String base;

        ThreadRunner( GetOrCreate<D> impl, String base )
        {
            this.impl = impl;
            this.base = base;
        }

        abstract D createDependency();

        @Override
        public void run()
        {
            final D dependency = createDependency();
            final List<GetOrCreateTask<D>> threads = new ArrayList<>();

            int numThreads = Runtime.getRuntime().availableProcessors() * 2;
            for ( int i = 0; i < numThreads; i++ )
            {
                String threadName = format( "%s thread %d", GetOrCreateDocIT.class.getSimpleName(), i );
                threads.add( new GetOrCreateTask<>( db,  NUM_USERS, impl, threadName, dependency, base ) );
            }
            for ( Thread thread : threads )
            {
                thread.start();
            }

            RuntimeException failure = null;
            List<List<Node>> results = new ArrayList<>();
            for ( GetOrCreateTask<D> thread : threads )
            {
                try
                {
                    thread.join();
                    if ( failure == null )
                    {
                        failure = thread.failure;
                    }

                    results.add( thread.result );
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }
            }

            if ( failure != null )
            {
                throw failure;
            }

            assertEquals( numThreads, results.size() );
            List<Node> firstResult = results.remove( 0 );
            for ( List<Node> subresult : results )
            {
                assertEquals( firstResult, subresult );
            }

            for ( int i = 0; i < NUM_USERS; i++ )
            {
                final String username = getUsername( base, i );
                GraphDatabaseService graphdb = graphdb();
                impl.getOrCreateUser( username, graphdb, dependency );

                try ( Transaction tx = graphdb.beginTx() )
                {
                    impl.assertUserExistsUniquely( graphdb, tx, username );
                }
                catch ( NoSuchElementException e )
                {
                    throw new RuntimeException( format( "User '%s' not created uniquely.", username ), e );
                }
            }
        }
    }

    private static String getUsername( String base, int j )
    {
        return format( "%s%d", base, j );
    }

    private static class GetOrCreateTask<D> extends Thread
    {
        private final GraphDatabaseService db;
        private final int numUsers;
        private final GetOrCreate<D> impl;
        private final D dependency;
        private final String base;

        volatile List<Node> result;
        volatile RuntimeException failure;

        GetOrCreateTask( GraphDatabaseService db, int numUsers, GetOrCreate<D> impl, String name, D dependency, String base )
        {
            super( name );
            this.db = db;
            this.numUsers = numUsers;
            this.impl = impl;
            this.dependency = dependency;
            this.base = base;
        }

        @Override
        public void run()
        {
            try
            {
                List<Node> subresult = new ArrayList<>();
                for ( int j = 0; j < numUsers; j++ )
                {
                    subresult.add( impl.getOrCreateUser( getUsername( base, j ), db, dependency) );
                }
                this.result = subresult;
            }
            catch ( RuntimeException e )
            {
                failure = e;
            }
        }
    }

    @Test
    public void testPessimisticLocking()
    {
        new ThreadRunner<Node>( new PessimisticGetOrCreate(), "chris" )
        {
            @Override
            Node createDependency()
            {
                return createLockNode( graphdb() );
            }
        }.run();
    }

    @Test
    public void getOrCreateWithUniqueFactory() throws Exception
    {
        new ThreadRunner<UniqueFactory<Node>>( new UniqueFactoryGetOrCreate(), "davide" ) {

            @Override
            UniqueFactory<Node> createDependency()
            {
                return createUniqueFactory( graphdb() );
            }
        }.run();
    }

    @Test
    public void getOrCreateUsingCypher() throws Exception
    {
        new ThreadRunner<GraphDatabaseService>( new CypherGetOrCreate(), "cypher") {
            @Override
            GraphDatabaseService createDependency()
            {
                return createConstraint( graphdb() );
            }
        }.run();
    }

    public Node getOrCreateUserPessimistically( String username, GraphDatabaseService graphDb, Node lockNode )
    {
        // START SNIPPET: pessimisticLocking
        try ( Transaction tx = graphDb.beginTx() )
        {
            Index<Node> usersIndex = graphDb.index().forNodes( "users" );
            Node userNode = usersIndex.get( "name", username ).getSingle();
            if ( userNode != null )
            {
                return userNode;
            }

            tx.acquireWriteLock( lockNode );
            userNode = usersIndex.get( "name", username ).getSingle();
            if ( userNode == null )
            {
                userNode = graphDb.createNode( DynamicLabel.label( "User" ) );
                usersIndex.add( userNode, "name", username );
                userNode.setProperty( "name", username );
            }
            tx.success();
            return userNode;
        }
        // END SNIPPET: pessimisticLocking
    }

    public static Node createLockNode( GraphDatabaseService graphDb )
    {
        // START SNIPPET: prepareLockNode
        try ( Transaction tx = graphDb.beginTx() )
        {
            final Node lockNode = graphDb.createNode();
            tx.success();
            return lockNode;
        }
        // END SNIPPET: prepareLockNode
    }

    private UniqueFactory<Node> createUniqueFactory( GraphDatabaseService graphDb )
    {
        // START SNIPPET: prepareUniqueFactory
        try ( Transaction tx = graphDb.beginTx() )
        {
            UniqueFactory.UniqueNodeFactory result = new UniqueFactory.UniqueNodeFactory( graphDb, "users" )
            {
                @Override
                protected void initialize( Node created, Map<String, Object> properties )
                {
                    created.addLabel( DynamicLabel.label( "User" ) );
                    created.setProperty( "name", properties.get( "name" ) );
                }
            };
            tx.success();
            return result;
        }
        // END SNIPPET: prepareUniqueFactory
    }

    public Node getOrCreateUserWithUniqueFactory( String username, GraphDatabaseService graphDb,
                                                  UniqueFactory<Node> factory )
    {
        // START SNIPPET: getOrCreateWithFactory
        try ( Transaction tx = graphDb.beginTx() )
        {
            Node node = factory.getOrCreate( "name", username );
            tx.success();
            return node;
        }
        // END SNIPPET: getOrCreateWithFactory
    }

    private Node getOrCreateWithCypher( String username, GraphDatabaseService graphDb )
    {
        // START SNIPPET: getOrCreateWithCypher
        Node result = null;
        ResourceIterator<Node> resultIterator = null;
        try ( Transaction tx = graphDb.beginTx() )
        {
            String queryString = "MERGE (n:User {name: {name}}) RETURN n";
            Map<String, Object> parameters = new HashMap<>();
            parameters.put( "name", username );
            resultIterator = graphDb.execute( queryString, parameters ).columnAs( "n" );
            result = resultIterator.next();
            tx.success();
            return result;
        }
        // END SNIPPET: getOrCreateWithCypher
        finally
        {
            if ( resultIterator != null )
            {
                if ( resultIterator.hasNext() )
                {
                    Node other = resultIterator.next();
                    //noinspection ThrowFromFinallyBlock
                    throw new IllegalStateException( "Merge returned more than one node: " + result + " and " + other );
                }
            }
        }
    }

    private GraphDatabaseService createConstraint( GraphDatabaseService graphdb )
    {
        // START SNIPPET: prepareConstraint
        try ( Transaction tx = graphdb.beginTx() )
        {
            graphdb.schema()
                    .constraintFor( DynamicLabel.label( "User" ) )
                    .assertPropertyIsUnique( "name" )
                    .create();
            tx.success();
        }
        // END SNIPPET: prepareConstraint
        return graphdb;
    }

    private static void assertUserExistsUniquelyInIndex( GraphDatabaseService graph, Transaction tx, String username )
    {
        assertNotNull( format( "User '%s' not created.", username ), graph.index()
                .forNodes( "users" )
                .get( "name", username )
                .getSingle() );
        tx.success();
    }

    private static void assertUserExistsUniquelyInGraphDb( GraphDatabaseService graph, Transaction tx, String username )
    {
        Label label = DynamicLabel.label( "User" );
        Node result = graph.findNode( label, "name", username );
        assertNotNull( format( "User '%s' not created.", username ), result );
        tx.success();
    }
}
