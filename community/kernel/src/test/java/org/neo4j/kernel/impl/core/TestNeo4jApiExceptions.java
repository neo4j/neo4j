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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class TestNeo4jApiExceptions
{
    @Test
    public void testNotInTransactionException()
    {
        Node node1 = graph.createNode();
        node1.setProperty( "test", 1 );
        Node node2 = graph.createNode();
        Node node3 = graph.createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        rel.setProperty( "test", 11 );
        commit();
        try
        {
            graph.createNode();
            fail( "Create node with no transaction should throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }
        try
        {
            node1.createRelationshipTo( node2, MyRelTypes.TEST );
            fail( "Create relationship with no transaction should " +
                    "throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }
        try
        {
            node1.setProperty( "test", 2 );
            fail( "Set property with no transaction should throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }
        try
        {
            rel.setProperty( "test", 22 );
            fail( "Set property with no transaction should throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }
        try
        {
            node3.delete();
            fail( "Delete node with no transaction should " +
                    "throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }
        try
        {
            rel.delete();
            fail( "Delete relationship with no transaction should " +
                    "throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }
        newTransaction();
        assertEquals( node1.getProperty( "test" ), 1 );
        assertEquals( rel.getProperty( "test" ), 11 );
        assertEquals( rel, node1.getSingleRelationship( MyRelTypes.TEST,
                Direction.OUTGOING ) );
        node1.delete();
        node2.delete();
        rel.delete();
        node3.delete();

        // Finally
        rollback();
    }

    @Test
    public void testNotFoundException()
    {
        Node node1 = graph.createNode();
        Node node2 = graph.createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        long nodeId = node1.getId();
        long relId = rel.getId();
        rel.delete();
        node2.delete();
        node1.delete();
        newTransaction();
        try
        {
            graph.getNodeById( nodeId );
            fail( "Get node by id on deleted node should throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }
        try
        {
            graph.getRelationshipById( relId );
            fail( "Get relationship by id on deleted node should " +
                    "throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }

        // Finally
        rollback();
    }


    @Test
    public void shouldGiveNiceErrorWhenShutdownKernelApi()
    {
        GraphDatabaseService graphDb = graph;
        Node node = graphDb.createNode();
        commit();
        graphDb.shutdown();

        try
        {
            asList( node.getLabels().iterator() );
            fail( "Did not get a nice exception" );
        }
        catch ( DatabaseShutdownException e )
        { // good
        }
    }

    @Test
    public void shouldGiveNiceErrorWhenShutdownLegacy()
    {
        GraphDatabaseService graphDb = graph;
        Node node = graphDb.createNode();
        commit();
        graphDb.shutdown();

        try
        {
            node.getRelationships();
            fail( "Did not get a nice exception" );
        }
        catch ( DatabaseShutdownException e )
        { // good
        }
        try
        {
            graphDb.createNode();
            fail( "Create node did not produce expected error" );
        }
        catch ( DatabaseShutdownException e )
        { // good
        }
    }

    private Transaction tx;
    private GraphDatabaseService graph;


    private void newTransaction()
    {
        if ( tx != null )
        {
            tx.success();
            tx.close();
        }
        tx = graph.beginTx();
    }

    public void commit()
    {
        if ( tx != null )
        {
            tx.success();
            tx.close();
            tx = null;
        }
    }

    public void rollback()
    {
        if ( tx != null )
        {
            tx.close();
            tx = null;
        }
    }

    @Before
    public void init()
    {
        graph = new TestGraphDatabaseFactory().newImpermanentDatabase();
        newTransaction();
    }

    @After
    public void cleanUp()
    {
        rollback();
        graph.shutdown();
    }

}
