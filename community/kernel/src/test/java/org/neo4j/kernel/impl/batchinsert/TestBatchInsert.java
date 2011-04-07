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
package org.neo4j.kernel.impl.batchinsert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.kernel.impl.batchinsert.SimpleRelationship;

public class TestBatchInsert
{
    private static Map<String,Object> properties = new HashMap<String,Object>();

    private static enum RelTypes implements RelationshipType
    {
        BATCH_TEST,
        REL_TYPE1,
        REL_TYPE2,
        REL_TYPE3,
        REL_TYPE4,
        REL_TYPE5
    }
    
    private static RelationshipType[] relTypeArray = { 
        RelTypes.REL_TYPE1, RelTypes.REL_TYPE2, RelTypes.REL_TYPE3,
        RelTypes.REL_TYPE4, RelTypes.REL_TYPE5 };
    
    static
    {
        properties.put( "key0", "SDSDASSDLKSDSAKLSLDAKSLKDLSDAKLDSLA" );
        properties.put( "key1", 1 );
        properties.put( "key2", (short) 2 );
        properties.put( "key3", 3L );
        properties.put( "key4", 4.0f );
        properties.put( "key5", 5.0d );
        properties.put( "key6", (byte) 6 );
        properties.put( "key7", true );
        properties.put( "key8", (char) 8 );
        properties.put( "key10", new String[] {
            "SDSDASSDLKSDSAKLSLDAKSLKDLSDAKLDSLA", "dsasda", "dssadsad" 
        } );
        properties.put( "key11", new int[] {1,2,3,4,5,6,7,8,9 } );
        properties.put( "key12", new short[] {1,2,3,4,5,6,7,8,9} );
        properties.put( "key13", new long[] {1,2,3,4,5,6,7,8,9 } );
        properties.put( "key14", new float[] {1,2,3,4,5,6,7,8,9} );
        properties.put( "key15", new double[] {1,2,3,4,5,6,7,8,9} );
        properties.put( "key16", new byte[] {1,2,3,4,5,6,7,8,9} );
        properties.put( "key17", new boolean[] {true,false,true,false} );
        properties.put( "key18", new char[] {1,2,3,4,5,6,7,8,9} );
    }
    
    private BatchInserter newBatchInserter()
    {
        String storePath = AbstractNeo4jTestCase.getStorePath( "neo-batch" );
        AbstractNeo4jTestCase.deleteFileOrDirectory( new File( storePath ) );
        return new BatchInserterImpl( storePath );
    }
    
    @Test
    public void testSimple()
    {
        BatchInserter graphDb = newBatchInserter();
        long node1 = graphDb.createNode( null );
        long node2 = graphDb.createNode( null );
        long rel1 = graphDb.createRelationship( node1, node2, RelTypes.BATCH_TEST, 
            null );
        SimpleRelationship rel = graphDb.getRelationshipById( rel1 );
        assertEquals( rel.getStartNode(), node1 );
        assertEquals( rel.getEndNode(), node2 );
        assertEquals( RelTypes.BATCH_TEST.name(), rel.getType().name() );
        graphDb.shutdown();
    }
    
    @Test
    public void testMore()
    {
        BatchInserter graphDb = newBatchInserter();
        long startNode = graphDb.createNode( properties );
        long endNodes[] = new long[25];
        Set<Long> rels = new HashSet<Long>();
        for ( int i = 0; i < 25; i++ )
        {
            endNodes[i] = graphDb.createNode( properties );
            rels.add( graphDb.createRelationship( startNode, endNodes[i], 
                relTypeArray[i % 5], properties ) );
        }
        for ( SimpleRelationship rel : graphDb.getRelationships( startNode ) )
        {
            assertTrue( rels.contains( rel.getId() ) );
            assertEquals( rel.getStartNode(), startNode );
        }
        graphDb.setNodeProperties( startNode, properties );
        graphDb.shutdown();
    }
    
    @Test
    public void testBadStuff()
    {
        BatchInserter graphDb = newBatchInserter();
        long startNode = graphDb.createNode( properties );
        try
        {
            graphDb.createRelationship( startNode, startNode, relTypeArray[0], 
                    properties );
            fail( "Could create relationship with same start and end node" );
        }
        catch ( IllegalArgumentException e )
        {
            // good
        }
        finally
        {
            graphDb.shutdown();
        }
    }
    
    private void setProperties( Node node )
    {
        for ( String key : properties.keySet() )
        {
            node.setProperty( key, properties.get( key ) );
        }
    }

    private void setProperties( Relationship rel )
    {
        for ( String key : properties.keySet() )
        {
            rel.setProperty( key, properties.get( key ) );
        }
    }
    
    @Test
    public void testWithGraphDbService()
    {
        BatchInserter batchInserter = newBatchInserter();
        GraphDatabaseService graphDb = batchInserter.getGraphDbService();
        Node startNode = graphDb.createNode();
        setProperties( startNode );
        Node endNodes[] = new Node[25];
        Set<Relationship> rels = new HashSet<Relationship>();
        for ( int i = 0; i < 25; i++ )
        {
            endNodes[i] = graphDb.createNode();
            setProperties( endNodes[i] );
            Relationship rel = startNode.createRelationshipTo( endNodes[i], 
                relTypeArray[i % 5] ); 
            rels.add( rel );
            setProperties( rel ); 
        }
        for ( Relationship rel : startNode.getRelationships() )
        {
            assertTrue( rels.contains( rel ) );
            assertEquals( rel.getStartNode(), startNode );
        }
        setProperties( startNode );
        graphDb.shutdown();
    }
    
    @Test
    public void testGraphDbServiceGetRelationships()
    {
        BatchInserter batchInserter = newBatchInserter();
        GraphDatabaseService graphDb = batchInserter.getGraphDbService();
        Node startNode = graphDb.createNode();
        for ( int i = 0; i < 5; i++ )
        {
            Node endNode = graphDb.createNode();
            startNode.createRelationshipTo( endNode, relTypeArray[i] ); 
        }
        try
        {
            startNode.createRelationshipTo( startNode, relTypeArray[0] );
            fail( "Could create relationship with same start and end node" );
        }
        catch ( IllegalArgumentException e )
        {
            // ok good
        }
        for ( int i = 0; i < 5; i++ )
        {
            assertTrue( startNode.getSingleRelationship( 
                relTypeArray[i], Direction.OUTGOING ) != null );
        }
        for ( int i = 0; i < 5; i++ )
        {
            Iterator<Relationship> relItr = 
                startNode.getRelationships( relTypeArray[i], 
                    Direction.OUTGOING ).iterator();
            relItr.next();
            assertTrue( !relItr.hasNext() );
        }
        for ( int i = 0; i < 5; i++ )
        {
            Iterator<Relationship> relItr = 
                startNode.getRelationships( relTypeArray[i] ).iterator();
            relItr.next();
            assertTrue( !relItr.hasNext() );
        }
        graphDb.shutdown();
    }
}
