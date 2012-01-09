/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.index.usage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.test.ImpermanentGraphDatabase;

public class UniqueEntityCreationTest
{
    GraphDatabaseService graphDatabaseService;
    
    @Test
    public void testOptimisticCreation() throws InterruptedException
    {
        graphDatabaseService = new ImpermanentGraphDatabase();
        new ThreadRunner().run();
    }

    class ThreadRunner implements Runnable
    {
        public static final int NUM_USERS = 10;

        @Override
        public void run()
        {
            final List<List<Node>> results = new ArrayList<List<Node>>();
            final List<Thread> threads = new ArrayList<Thread>();
            for ( int i = 0; i < 10; i++ )
            {
                threads.add( new Thread()
                {
                    @Override
                    public void run()
                    {
                        List<Node> subresult = new ArrayList<Node>();
                        for ( int j = 0; j < NUM_USERS; j++ )
                        {
                            subresult.add( getOrCreateUserOptimistically( getUsername( j ), graphDatabaseService) );
                        }
                        results.add( subresult );
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
            List<Node> first = results.remove( 0 );
            for ( List<Node> subresult : results )
            {
                assertEquals( first, subresult );
            }
            for ( int i = 0; i < NUM_USERS; i++ )
            {
                final String username = getUsername( i );
                getOrCreateUserOptimistically( username, graphDatabaseService );
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
                    graphDatabaseService.index().forNodes( "users" ).get( "name", username ).getSingle() );
            }
            catch ( NoSuchElementException e )
            {
                throw new RuntimeException( "User '" + username + "' not created uniquely.", e );
            }
        }
    }

    private Node getOrCreateUserOptimistically( String username, GraphDatabaseService graphDb )
    {
        Index<Node> usersIndex = graphDb.index().forNodes( "users" );
        Node userNode = getFirstUserNode( username, graphDb, usersIndex );
        if ( userNode == null )
        {
            addUserNode( username, graphDb, usersIndex );
            userNode = getFirstUserNode( username, graphDb, usersIndex );
        }
        return userNode;
    }

    private Node getFirstUserNode( String username, GraphDatabaseService graphDb, Index<Node> usersIndex )
    {
        final IndexHits<Node> userHits = usersIndex.get( "name", username );
        Node firstUser = null;
        Set<Node> duplicates = new HashSet<Node>();
        try
        {
            for ( Node user : userHits )
            {
                if ( firstUser == null )
                {
                    firstUser = user;
                }
                else
                {
                    duplicates.add( user );
                }
            }
        }
        finally
        {
            userHits.close();
        }
        if ( !duplicates.isEmpty() )
        {
            try
            {
                deleteNodes( username, graphDb, usersIndex, duplicates );
            }
            catch ( Exception e )
            {
                // May produce errors due to duplicate nodes already having been removed.
            }
        }
        return firstUser;
    }

    private void deleteNodes( String username, GraphDatabaseService graphDb, Index<Node> usersIndex, Set<Node> duplicates )
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            for ( Node duplicate : duplicates )
            {
                usersIndex.remove( duplicate, "name", username );
                duplicate.delete();
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private void addUserNode( String username, GraphDatabaseService graphDb, Index<Node> usersIndex )
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            Node userNode = graphDb.createNode();
            userNode.setProperty( "name", username );
            usersIndex.add( userNode, "name", username );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
}
