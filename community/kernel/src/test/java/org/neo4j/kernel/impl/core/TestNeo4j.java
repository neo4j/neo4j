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
package org.neo4j.kernel.impl.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class TestNeo4j extends AbstractNeo4jTestCase
{
    @Test
    public void testReferenceNode()
    {
        // fix this test when we can set reference node again
        Node oldReferenceNode = null;
        try
        {
            // get old reference node if one is set
            oldReferenceNode = getGraphDb().getReferenceNode();
        }
        catch ( RuntimeException e )
        {
            // ok no one set, oldReferenceNode is null then
        }
        try
        {
            GraphDbModule graphDbModule = ((EmbeddedGraphDatabase) getGraphDb()).getConfig()
                .getGraphDbModule();

            Node newReferenceNode = getGraphDb().createNode();
            graphDbModule.setReferenceNodeId( newReferenceNode.getId() );
            assertEquals( newReferenceNode, getGraphDb().getReferenceNode() );
            newReferenceNode.delete();
            if ( oldReferenceNode != null )
            {
                graphDbModule.setReferenceNodeId( oldReferenceNode.getId() );
                assertEquals( oldReferenceNode, getGraphDb().getReferenceNode() );
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail( "" + e );
        }
    }

    @Test
    public void testBasicNodeRelationships()
    {
        Node firstNode = null;
        Node secondNode = null;
        Relationship rel = null;
        // Create nodes and a relationship between them
        firstNode = getGraphDb().createNode();
        assertNotNull( "Failure creating first node", firstNode );
        secondNode = getGraphDb().createNode();
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

//    private static enum RelTypes implements RelationshipType
//    {
//        ONE_MORE_RELATIONSHIP;
//    }

    // TODO: fix this testcase
    @Test
    public void testIdUsageInfo()
    {
        GraphDbModule graphDbModule = ((EmbeddedGraphDatabase) getGraphDb()).getConfig()
            .getGraphDbModule();
        NodeManager nm = graphDbModule.getNodeManager();
        long nodeCount = nm.getNumberOfIdsInUse( Node.class );
        long relCount = nm.getNumberOfIdsInUse( Relationship.class );
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
        setTransaction( getGraphDb().beginTx() );
    }

    @Test
    public void testRandomPropertyName()
    {
        Node node1 = getGraphDb().createNode();
        String key = "random_"
            + new Random( System.currentTimeMillis() ).nextLong();
        node1.setProperty( key, "value" );
        assertEquals( "value", node1.getProperty( key ) );
        node1.delete();
    }

    @Test
    public void testNodeChangePropertyArray() throws Exception
    {
        Transaction tx = getTransaction();
        tx.finish();
        tx = getGraphDb().beginTx();
        Node node;
        try
        {
            node = getGraphDb().createNode();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        tx = getGraphDb().beginTx();
        try
        {
            node.setProperty( "test", new String[] { "value1" } );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        tx = getGraphDb().beginTx();
        try
        {
            node.setProperty( "test", new String[] { "value1", "value2" } );
            // no success, we wanna test rollback on this operation
        }
        finally
        {
            tx.finish();
        }
        tx = getGraphDb().beginTx();
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
        setTransaction( getGraphDb().beginTx() );
    }

    @Test
    public void testMultipleNeos()
    {
        String storePath = getStorePath( "test-neo2" );
        deleteFileOrDirectory( storePath );
        GraphDatabaseService graphDb2 = new EmbeddedGraphDatabase( storePath );
        Transaction tx2 = graphDb2.beginTx();
        getGraphDb().createNode();
        graphDb2.createNode();
        tx2.success();
        tx2.finish();
        graphDb2.shutdown();
    }
    
    @Test
    public void testGetAllNode()
    {
        long highId = getNodeManager().getHighestPossibleIdInUse( Node.class );
        if ( highId >= 0 && highId < 10000 )
        {
            int count = 0;
            for ( Node node : getEmbeddedGraphDb().getAllNodes() )
            {
                count++;
            }
            boolean found = false;
            Node newNode = getGraphDb().createNode();
            newTransaction();
            int oldCount = count;
            count = 0;
            for ( Node node : getEmbeddedGraphDb().getAllNodes() )
            {
                count++;
                if ( node.equals( newNode ) )
                {
                    found = true;
                }
            }
            assertTrue( found );
            assertEquals( count, oldCount + 1 );
            
            // Tests a bug in the "all nodes" iterator
            Iterator<Node> allNodesIterator =
                getEmbeddedGraphDb().getAllNodes().iterator();
            assertNotNull( allNodesIterator.next() );
            
            newNode.delete();
            newTransaction();
            found = false;
            count = 0;
            for ( Node node : getEmbeddedGraphDb().getAllNodes() )
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
        // else we skip test, takes too long
    }
    
    @Test
    public void testMultipleShutdown()
    {
        getGraphDb().shutdown();
        getGraphDb().shutdown();
    }
    
    @Test
    public void testKeepLogsConfig()
    {
        Map<String,String> config = new HashMap<String,String>();
        config.put( Config.KEEP_LOGICAL_LOGS, Config.DEFAULT_DATA_SOURCE_NAME );
        String storeDir = "target/configdb";
        deleteFileOrDirectory( storeDir );
        EmbeddedGraphDatabase db = new EmbeddedGraphDatabase( 
                storeDir, config );
        XaDataSourceManager xaDsMgr = 
                db.getConfig().getTxModule().getXaDataSourceManager();
        XaDataSource xaDs = xaDsMgr.getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
        assertTrue( xaDs.isLogicalLogKept() );
        db.shutdown();
        
        config.remove( Config.KEEP_LOGICAL_LOGS );
        db = new EmbeddedGraphDatabase( storeDir, config );
        xaDsMgr = db.getConfig().getTxModule().getXaDataSourceManager();
        xaDs = xaDsMgr.getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
        // Here we rely on the default value being set to true due to the existence
        // of previous log files.
        assertTrue( xaDs.isLogicalLogKept() );
        db.shutdown();

        config.put( Config.KEEP_LOGICAL_LOGS, "false" );
        db = new EmbeddedGraphDatabase( storeDir, config );
        xaDsMgr = db.getConfig().getTxModule().getXaDataSourceManager();
        xaDs = xaDsMgr.getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
        // Here we explicitly turn off the keeping of logical logs so it should be
        // false even if there are previous existing log files.
        assertFalse( xaDs.isLogicalLogKept() );
        db.shutdown();
        
        config.put( Config.KEEP_LOGICAL_LOGS, Config.DEFAULT_DATA_SOURCE_NAME + "=false" );
        db = new EmbeddedGraphDatabase( storeDir, config );
        xaDsMgr = db.getConfig().getTxModule().getXaDataSourceManager();
        xaDs = xaDsMgr.getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
        // Here we explicitly turn off the keeping of logical logs so it should be
        // false even if there are previous existing log files.
        assertFalse( xaDs.isLogicalLogKept() );
        db.shutdown();
        
        config.put( Config.KEEP_LOGICAL_LOGS, Config.DEFAULT_DATA_SOURCE_NAME + "=true" );
        db = new EmbeddedGraphDatabase( storeDir, config );
        xaDsMgr = db.getConfig().getTxModule().getXaDataSourceManager();
        xaDs = xaDsMgr.getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
        assertTrue( xaDs.isLogicalLogKept() );
        db.shutdown();

        config.put( Config.KEEP_LOGICAL_LOGS, "true" );
        db = new EmbeddedGraphDatabase( storeDir, config );
        xaDsMgr = db.getConfig().getTxModule().getXaDataSourceManager();
        xaDs = xaDsMgr.getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
        assertTrue( xaDs.isLogicalLogKept() );
    }
}