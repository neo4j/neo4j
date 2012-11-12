/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.rest.paging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.EphemeralDatabase;

public class PagedTraverserTest
{
    private static final int LIST_LENGTH = 100;
    private Database database;
    private Node startNode;

    @Before
    public void clearDb() throws Throwable
    {
        database = new EphemeralDatabase();
        database.start();
        createLinkedList( LIST_LENGTH, database );
    }

    @After
    public void shutdownDatabase() throws Throwable
    {
        database.getGraph().shutdown();
    }

    private void createLinkedList( int listLength, Database db )
    {
        Transaction tx = db.getGraph().beginTx();
        try
        {
            Node previous = null;
            for ( int i = 0; i < listLength; i++ )
            {
                Node current = db.getGraph().createNode();

                if ( previous != null )
                {
                    previous.createRelationshipTo( current, DynamicRelationshipType.withName( "NEXT" ) );
                }
                else
                {
                    startNode = current;
                }

                previous = current;
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    @Test
    public void shouldPageThroughResultsForWhollyDivisiblePageSize()
    {
        Traverser myTraverser = simpleListTraverser();
        PagedTraverser traversalPager = new PagedTraverser( myTraverser, LIST_LENGTH / 10 );

        int iterations = iterateThroughPagedTraverser( traversalPager );

        assertEquals( 10, iterations );
        assertNull( traversalPager.next() );

    }

    @SuppressWarnings( "unused" )
    private int iterateThroughPagedTraverser( PagedTraverser traversalPager )
    {
        int count = 0;
        for ( List<Path> paths : traversalPager )
        {
            count++;
        }
        return count;
    }

    @Test
    public void shouldPageThroughResultsForNonWhollyDivisiblePageSize()
    {
        int awkwardPageSize = 7;
        Traverser myTraverser = simpleListTraverser();
        PagedTraverser traversalPager = new PagedTraverser( myTraverser, awkwardPageSize );

        int iterations = iterateThroughPagedTraverser( traversalPager );

        assertEquals( 15, iterations );
        assertNull( traversalPager.next() );
    }

    private Traverser simpleListTraverser()
    {
        return Traversal.description()
                .expand( Traversal.expanderForTypes( DynamicRelationshipType.withName( "NEXT" ), Direction.OUTGOING ) )
                .depthFirst()
                .uniqueness( Uniqueness.NODE_GLOBAL )
                .traverse( startNode );
    }
}
