/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.core;

import java.util.Random;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.impl.AbstractNeoTestCase;
import org.neo4j.impl.MyRelTypes;

public class TestNeo extends AbstractNeoTestCase
{
    public TestNeo( String testName )
    {
        super( testName );
    }

    public void testReferenceNode()
    {
        // fix this test when we can set reference node again
        Node oldReferenceNode = null;
        try
        {
            // get old reference node if one is set
            oldReferenceNode = getNeo().getReferenceNode();
        }
        catch ( RuntimeException e )
        {
            // ok no one set, oldReferenceNode is null then
        }
        try
        {
            NeoModule neoModule = ((EmbeddedNeo) getNeo()).getConfig()
                .getNeoModule();

            Node newReferenceNode = getNeo().createNode();
            neoModule.setReferenceNodeId( (int) newReferenceNode.getId() );
            assertEquals( newReferenceNode, getNeo().getReferenceNode() );
            newReferenceNode.delete();
            if ( oldReferenceNode != null )
            {
                neoModule.setReferenceNodeId( (int) oldReferenceNode.getId() );
                assertEquals( oldReferenceNode, getNeo().getReferenceNode() );
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail( "" + e );
        }
    }

    public void testBasicNodeRelationships()
    {
        Node firstNode = null;
        Node secondNode = null;
        Relationship rel = null;
        // Create nodes and a relationship between them
        firstNode = getNeo().createNode();
        assertNotNull( "Failure creating first node", firstNode );
        secondNode = getNeo().createNode();
        assertNotNull( "Failure creating second node", secondNode );
        rel = firstNode.createRelationshipTo( secondNode, MyRelTypes.TEST );
        assertNotNull( "Relationship is null", rel );
        RelationshipType relType = rel.getType();
        assertNotNull( "Relationship's type is is null", relType );

        // Verify that the node reports that it has a relationship of
        // the type we created above
        assertTrue( firstNode.getRelationships( relType ).iterator().hasNext() );
        assertTrue( secondNode.getRelationships( relType ).iterator().hasNext() );

        Iterable<Relationship> allRels = null;

        // Verify that both nodes return the relationship we created above
        allRels = firstNode.getRelationships();
        assertTrue( this.objectExistsInIterable( rel, allRels ) );
        allRels = firstNode.getRelationships( relType );
        assertTrue( this.objectExistsInIterable( rel, allRels ) );

        allRels = secondNode.getRelationships();
        assertTrue( this.objectExistsInIterable( rel, allRels ) );
        allRels = secondNode.getRelationships( relType );
        assertTrue( this.objectExistsInIterable( rel, allRels ) );

        // Verify that the relationship reports that it is associated with
        // firstNode and secondNode
        Node[] relNodes = rel.getNodes();
        assertEquals( "A relationship should always be connected to exactly "
            + "two nodes", relNodes.length, 2 );
        assertTrue( "Relationship says that it isn't connected to firstNode",
            this.objectExistsInArray( firstNode, relNodes ) );
        assertTrue( "Relationship says that it isn't connected to secondNode",
            this.objectExistsInArray( secondNode, relNodes ) );
        assertTrue( "The other node should be secondNode but it isn't", rel
            .getOtherNode( firstNode ).equals( secondNode ) );
        assertTrue( "The other node should be firstNode but it isn't", rel
            .getOtherNode( secondNode ).equals( firstNode ) );
        rel.delete();
        secondNode.delete();
        firstNode.delete();
    }

    private boolean objectExistsInIterable( Relationship rel,
        Iterable<Relationship> allRels )
    {
        for ( Relationship iteratedRel : allRels )
        {
            if ( rel.equals( iteratedRel ) )
            {
                return true;
            }
        }
        return false;
    }

    private boolean objectExistsInArray( Object obj, Object[] objArray )
    {
        for ( int i = 0; i < objArray.length; i++ )
        {
            if ( objArray[i].equals( obj ) )
            {
                return true;
            }
        }
        return false;
    }

    private static enum RelTypes implements RelationshipType
    {
        ONE_MORE_RELATIONSHIP;
    }

    // TODO: fix this testcase
    public void testIdUsageInfo()
    {
        NeoModule neoModule = ((EmbeddedNeo) getNeo()).getConfig()
            .getNeoModule();
        NodeManager nm = neoModule.getNodeManager();
        int nodeCount = nm.getNumberOfIdsInUse( Node.class );
        int relCount = nm.getNumberOfIdsInUse( Relationship.class );
        if ( nodeCount > nm.getHighestPossibleIdInUse( Node.class ) )
        {
            // fail( "Node count greater than highest id " + nodeCount );
        }
        if ( relCount > nm.getHighestPossibleIdInUse( Relationship.class ) )
        {
            // fail( "Rel count greater than highest id " + relCount );
        }
        // assertTrue( nodeCount <= nm.getHighestPossibleIdInUse( Node.class )
        // );
        // assertTrue( relCount <= nm.getHighestPossibleIdInUse(
        // Relationship.class ) );
        Node n1 = nm.createNode();
        Node n2 = nm.createNode();
        Relationship r1 = n1.createRelationshipTo( n2, MyRelTypes.TEST );
        // assertEquals( nodeCount + 2, nm.getNumberOfIdsInUse( Node.class ) );
        // assertEquals( relCount + 1, nm.getNumberOfIdsInUse(
        // Relationship.class ) );
        r1.delete();
        n1.delete();
        n2.delete();
        // must commit for ids to be reused
        try
        {
            getTransaction().success();
            getTransaction().finish();
        }
        catch ( Exception e )
        {
            fail( "" + e );
        }
        // assertEquals( nodeCount, nm.getNumberOfIdsInUse( Node.class ) );
        // assertEquals( relCount, nm.getNumberOfIdsInUse( Relationship.class )
        // );
        setTransaction( getNeo().beginTx() );
    }

    public void testRandomPropertyName()
    {
        Node node1 = getNeo().createNode();
        String key = "random_"
            + new Random( System.currentTimeMillis() ).nextLong();
        node1.setProperty( key, "value" );
        assertEquals( "value", node1.getProperty( key ) );
        node1.delete();
    }

    public void testNodeChangePropertyArray() throws Exception
    {
        Transaction tx = getTransaction();
        tx.finish();
        tx = getNeo().beginTx();
        Node node;
        try
        {
            node = getNeo().createNode();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        tx = getNeo().beginTx();
        try
        {
            node.setProperty( "test", new String[] { "value1" } );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        tx = getNeo().beginTx();
        try
        {
            node.setProperty( "test", new String[] { "value1", "value2" } );
            // no success, we wanna test rollback on this operation
        }
        finally
        {
            tx.finish();
        }
        tx = getNeo().beginTx();
        try
        {
            String[] value = (String[]) node.getProperty( "test" );
            assertEquals( 1, value.length );
            assertEquals( "value1", value[0] );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        setTransaction( getNeo().beginTx() );
    }

    public void testMultipleNeos()
    {
        NeoService neo2 = new EmbeddedNeo( "var/test-neo2" );
        Transaction tx2 = neo2.beginTx();
        getNeo().createNode();
        neo2.createNode();
        tx2.success();
        tx2.finish();
        neo2.shutdown();
    }
    
    public void testGetAllNode()
    {
        long highId = getNodeManager().getHighestPossibleIdInUse( Node.class );
        if ( highId >= 0 && highId < 10000 )
        {
            int count = 0;
            for ( Node node : getEmbeddedNeo().getAllNodes() )
            {
                count++;
            }
            boolean found = false;
            Node newNode = getNeo().createNode();
            newTransaction();
            int oldCount = count;
            count = 0;
            for ( Node node : getEmbeddedNeo().getAllNodes() )
            {
                count++;
                if ( node.equals( newNode ) )
                {
                    found = true;
                }
            }
            assertTrue( found );
            assertEquals( count, oldCount + 1 );
            newNode.delete();
            newTransaction();
            found = false;
            count = 0;
            for ( Node node : getEmbeddedNeo().getAllNodes() )
            {
                count++;
                if ( node.equals( newNode ) )
                {
                    found = true;
                }
            }
            assertTrue( !found );
            assertEquals( count, oldCount );
        }
        // else we skip test, takes to long
    }
}