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

import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class Neo4jConstraintsTest extends AbstractNeo4jTestCase
{
    private final String key = "testproperty";

    @Test
    public void testDeleteReferenceNodeOrLastNodeIsOk()
    {
        Transaction tx = getTransaction();
        for ( int i = 0; i < 10; i++ )
        {
            getGraphDb().createNode();
        }
        // long numNodesPre = getNodeManager().getNumberOfIdsInUse( Node.class
        // );
        // empty the DB instance
        for ( Node node : GlobalGraphOperations.at( getGraphDb() ).getAllNodes() )
        {
            for ( Relationship rel : node.getRelationships() )
            {
                rel.delete();
            }
            node.delete();
        }
        tx.success();
        tx.close();
        tx = getGraphDb().beginTx();
        assertFalse( GlobalGraphOperations.at( getGraphDb() ).getAllNodes().iterator().hasNext() );
        // TODO: this should be valid, fails right now!
        // assertEquals( 0, numNodesPost );
        tx.success();
        tx.close();
    }

    @Test
    public void testDeleteNodeWithRel1()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        node1.createRelationshipTo( node2, MyRelTypes.TEST );
        node1.delete();
        try
        {
            Transaction tx = getTransaction();
            tx.success();
            tx.close();
            fail( "Should not validate" );
        }
        catch ( Exception e )
        {
            // good
        }
        setTransaction( getGraphDb().beginTx() );
    }

    @Test
    public void testDeleteNodeWithRel2()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        node1.createRelationshipTo( node2, MyRelTypes.TEST );
        node2.delete();
        node1.delete();
        try
        {
            Transaction tx = getTransaction();
            tx.success();
            tx.close();
            fail( "Should not validate" );
        }
        catch ( Exception e )
        {
            // good
        }
        setTransaction( getGraphDb().beginTx() );
    }

    @Test
    public void testDeleteNodeWithRel3()
    {
        // make sure we can delete in wrong order
        Node node0 = getGraphDb().createNode();
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel0 = node0.createRelationshipTo( node1, MyRelTypes.TEST );
        Relationship rel1 = node0.createRelationshipTo( node2, MyRelTypes.TEST );
        node1.delete();
        rel0.delete();
        Transaction tx = getTransaction();
        tx.success();
        tx.close();
        setTransaction( getGraphDb().beginTx() );
        node2.delete();
        rel1.delete();
        node0.delete();
    }

    @Test
    public void testCreateRelOnDeletedNode()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Transaction tx = getTransaction();
        tx.success();
        tx.close();
        tx = getGraphDb().beginTx();
        node1.delete();
        try
        {
            node1.createRelationshipTo( node2, MyRelTypes.TEST );
            fail( "Create of rel on deleted node should fail fast" );
        }
        catch ( Exception e )
        { // ok
        }
        try
        {
            tx.failure();
            tx.close();
            // fail( "Transaction should be marked rollback" );
        }
        catch ( Exception e )
        { // good
        }
        setTransaction( getGraphDb().beginTx() );
        node2.delete();
        node1.delete();
    }

    @Test
    public void testAddPropertyDeletedNode()
    {
        Node node = getGraphDb().createNode();
        node.delete();
        try
        {
            node.setProperty( key, 1 );
            fail( "Add property on deleted node should not validate" );
        }
        catch ( Exception e )
        {
            // good
        }
    }

    @Test
    public void testRemovePropertyDeletedNode()
    {
        Node node = getGraphDb().createNode();
        node.setProperty( key, 1 );
        node.delete();
        try
        {
            node.removeProperty( key );
            Transaction tx = getTransaction();
            tx.success();
            tx.close();
            fail( "Change property on deleted node should not validate" );
        }
        catch ( Exception e )
        {
            // ok
        }
    }

    @Test
    public void testChangePropertyDeletedNode()
    {
        Node node = getGraphDb().createNode();
        node.setProperty( key, 1 );
        node.delete();
        try
        {
            node.setProperty( key, 2 );
            Transaction tx = getTransaction();
            tx.success();
            tx.close();
            fail( "Change property on deleted node should not validate" );
        }
        catch ( Exception e )
        {
            // ok
        }
    }

    @Test
    public void testAddPropertyDeletedRelationship()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        rel.delete();
        try
        {
            rel.setProperty( key, 1 );
            Transaction tx = getTransaction();
            tx.success();
            tx.close();
            fail( "Add property on deleted rel should not validate" );
        }
        catch ( Exception e )
        { // good
        }
        node1.delete();
        node2.delete();
    }

    @Test
    public void testRemovePropertyDeletedRelationship()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        rel.setProperty( key, 1 );
        rel.delete();
        try
        {
            rel.removeProperty( key );
            Transaction tx = getTransaction();
            tx.success();
            tx.close();
            fail( "Remove property on deleted rel should not validate" );
        }
        catch ( Exception e )
        {
            // ok
        }
        node1.delete();
        node2.delete();
    }

    @Test
    public void testChangePropertyDeletedRelationship()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        rel.setProperty( key, 1 );
        rel.delete();
        try
        {
            rel.setProperty( key, 2 );
            Transaction tx = getTransaction();
            tx.success();
            tx.close();
            fail( "Change property on deleted rel should not validate" );
        }
        catch ( Exception e )
        {
            // ok
        }
        node1.delete();
        node2.delete();
    }

    @Test
    public void testMultipleDeleteNode()
    {
        Node node1 = getGraphDb().createNode();
        node1.delete();
        try
        {
            node1.delete();
            Transaction tx = getTransaction();
            tx.success();
            tx.close();
            fail( "Should not validate" );
        }
        catch ( Exception e )
        {
            // ok
        }
    }

    @Test
    public void testMultipleDeleteRelationship()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        rel.delete();
        node1.delete();
        node2.delete();
        try
        {
            rel.delete();
            Transaction tx = getTransaction();
            tx.success();
            tx.close();
            fail( "Should not validate" );
        }
        catch ( Exception e )
        {
            // ok
        }
    }

    @Test
    public void testIllegalPropertyType()
    {
        Node node1 = getGraphDb().createNode();
        try
        {
            node1.setProperty( key, new Object() );
            fail( "Shouldn't validate" );
        }
        catch ( Exception e )
        { // good
        }
        {
            Transaction tx = getTransaction();
            tx.failure();
            tx.close();
        }
        setTransaction( getGraphDb().beginTx() );
        try
        {
            getGraphDb().getNodeById( node1.getId() );
            fail( "Node should not exist, previous tx didn't rollback" );
        }
        catch ( NotFoundException e )
        {
            // good
        }
        node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel = node1.createRelationshipTo( node2,
                MyRelTypes.TEST );
        try
        {
            rel.setProperty( key, new Object() );
            fail( "Shouldn't validate" );
        }
        catch ( Exception e )
        { // good
        }
        try
        {
            Transaction tx = getTransaction();
            tx.success();
            tx.close();
            fail( "Shouldn't validate" );
        }
        catch ( Exception e )
        {
        } // good
        setTransaction( getGraphDb().beginTx() );
        try
        {
            getGraphDb().getNodeById( node1.getId() );
            fail( "Node should not exist, previous tx didn't rollback" );
        }
        catch ( NotFoundException e )
        {
            // good
        }
        try
        {
            getGraphDb().getNodeById( node2.getId() );
            fail( "Node should not exist, previous tx didn't rollback" );
        }
        catch ( NotFoundException e )
        {
            // good
        }
    }

    @Test
    public void testNodeRelDeleteSemantics()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel1 = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        Relationship rel2 = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        node1.setProperty( "key1", "value1" );
        rel1.setProperty( "key1", "value1" );

        newTransaction();
        node1.delete();
        try
        {
            node1.getProperty( "key1" );
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }
        try
        {
            node1.setProperty( "key1", "value2" );
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }
        try
        {
            node1.removeProperty( "key1" );
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }
        node2.delete();
        try
        {
            node2.delete();
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }
        try
        {
            node1.getProperty( "key1" );
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }
        try
        {
            node1.setProperty( "key1", "value2" );
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }
        try
        {
            node1.removeProperty( "key1" );
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }
        assertEquals( "value1", rel1.getProperty( "key1" ) );
        rel1.delete();
        try
        {
            rel1.delete();
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }
        try
        {
            rel1.getProperty( "key1" );
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }
        try
        {
            rel1.setProperty( "key1", "value2" );
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }
        try
        {
            rel1.removeProperty( "key1" );
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }
        try
        {
            rel1.getProperty( "key1" );
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }
        try
        {
            rel1.setProperty( "key1", "value2" );
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }
        try
        {
            rel1.removeProperty( "key1" );
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }
        try
        {
            node2.createRelationshipTo( node1, MyRelTypes.TEST );
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }
        try
        {
            node2.createRelationshipTo( node1, MyRelTypes.TEST );
            fail( "Should throw exception" );
        }
        catch ( NotFoundException e )
        { // good
        }

        assertEquals( node1, rel1.getStartNode() );
        assertEquals( node2, rel2.getEndNode() );
        Node[] nodes = rel1.getNodes();
        assertEquals( node1, nodes[0] );
        assertEquals( node2, nodes[1] );
        assertEquals( node2, rel1.getOtherNode( node1 ) );
        rel2.delete();
        // will be marked for rollback so commit will throw exception
        rollback();
    }
}
