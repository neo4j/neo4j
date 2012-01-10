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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
    public static final int NUM_KEYS = 5;
    public static final int NUM_THREADS = 20;

    GraphDatabaseService database;

    @Test
    public void testOptimisticCreation() throws InterruptedException
    {
        database = new ImpermanentGraphDatabase();

        final List<List<Node>> results = new ArrayList<List<Node>>();
        final List<Thread> threads = new ArrayList<Thread>();
        for ( int i = 0; i < NUM_THREADS; i++ )
        {
            final int threadNumber = i;

            threads.add( new Thread()
            {
                @Override
                public void run()
                {
                    List<Node> subResult = new ArrayList<Node>();
                    for ( int j = 0; j < NUM_KEYS; j++ )
                    {
                        String key = "key" + j;
                        Index<Node> index = database.index().forNodes( "users" );
                        Node userNode = getFirstNode( threadNumber, key, database, index );
                        if ( userNode == null )
                        {
                            addNode( key, database, index );
                            userNode = getFirstNode( threadNumber, key, database, index );
                        }
                        subResult.add( userNode );
                    }
                    results.add( subResult );
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
    }

    private Node getFirstNode( int threadNumber, String key, GraphDatabaseService graphDb, Index<Node> index )
    {
        final IndexHits<Node> hits = index.get( "name", key );
        Node firstNode = null;
        Set<Node> duplicates = new HashSet<Node>();
        try
        {
            for ( Node node : hits )
            {
                if ( firstNode == null )
                {
                    firstNode = node;
                }
                else
                {
                    duplicates.add( node );
                }
            }
        }
        finally
        {
            hits.close();
        }
        if ( !duplicates.isEmpty() )
        {
            ArrayList<Node> duplicatesList = new ArrayList( duplicates );
            for ( int i = 0; i < duplicatesList.size(); i++ )
            {
                Node node = duplicatesList.get( i );
                String message = String.format( "Thread %d: For %s, found node %d, deleting node %d (duplicate %d)",
                        threadNumber, key, firstNode.getId(), node.getId(), i );
                Transaction tx = graphDb.beginTx();
                try
                {
                    index.remove( node, "name", key );
                    node.delete();
                    tx.success();
                }
                catch ( Exception e )
                {
                    message += " threw " + e.getClass().getSimpleName();
                }
                finally
                {
                    tx.finish();
                }
                System.out.println( message );
            }
        }
        return firstNode;
    }

    private void addNode( String key, GraphDatabaseService graphDb, Index<Node> usersIndex )
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            Node userNode = graphDb.createNode();
            userNode.setProperty( "name", key );
            usersIndex.add( userNode, "name", key );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
}
