/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.index;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

public class IndexConstraintsTest
{
    private GraphDatabaseService graphDb;

    @Before
    public void setup() throws IOException
    {
        this.graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @After
    public void shutdown() throws IOException
    {
        this.graphDb.shutdown();
    }

    @Test
    public void testMultipleCreate() throws InterruptedException
    {
        final int numThreads = 25;
        final String uuid = UUID.randomUUID().toString();
        ExecutorCompletionService<Node> ecs = new ExecutorCompletionService<Node>(
                Executors.newFixedThreadPool( numThreads ) );
        for ( int i = 0; i < numThreads; i++ )
        {
            ecs.submit( new Callable<Node>()
            {
                public Node call() throws Exception
                {
                    Transaction tx = graphDb.beginTx();
                    try
                    {
                        final Node node = graphDb.createNode();
                        // Acquire write lock on common node
                        graphDb.getReferenceNode().removeProperty(
                                "NOT_EXISTING" );
                        Index<Node> index = graphDb.index().forNodes( "uuids" );
                        final Node existing = index.get( "uuid", uuid ).getSingle();
                        if ( existing != null )
                        {
                            throw new RuntimeException( "Node already exists" );
                        }
                        node.setProperty( "uuid", uuid );
                        index.add( node, "uuid", uuid );
                        tx.success();
                        return node;
                    }
                    finally
                    {
                        tx.finish();
                    }
                }
            } );
        }
        int numSucceeded = 0;
        for ( int i = 0; i < numThreads; i++ )
        {
            try
            {
                ecs.take().get();
                ++numSucceeded;
            }
            catch ( ExecutionException e )
            {
            }
        }
        assertEquals( 1, numSucceeded );
    }
}