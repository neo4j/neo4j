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

import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.test.EmbeddedDatabaseRule;

import static java.lang.Math.pow;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.AbstractNeo4jTestCase.deleteFileOrDirectory;

public class BigStoreIT implements RelationshipType
{
    private static final RelationshipType OTHER_TYPE = DynamicRelationshipType.withName( "OTHER" );
    
    private static final String PATH = "target/var/big";
    private GraphDatabaseService db;
    @Rule
    public TestName testName = new TestName()
    {
        @Override
        public String getMethodName()
        {
            return BigStoreIT.this.getClass().getSimpleName() + "#" + super.getMethodName();
        }
    };
    @Rule
    public EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule( BigStoreIT.class );

    
    @Before
    public void doBefore()
    {
        // Delete before just to be sure
        deleteFileOrDirectory( new File( PATH ) );
        db = dbRule.getGraphDatabaseService();
    }
    
    @Override
    public String name()
    {
        return "BIG_TYPE";
    }
    
    @Test
    public void create4BPlusStuff() throws Exception
    {
        testHighIds( (long) pow( 2, 32 ), 2, 400 );
    }
    
    @Test
    public void create8BPlusStuff() throws Exception
    {
        testHighIds( (long) pow( 2, 33 ), 1, 1000 );
    }
    
    @Test
    public void createAndVerify32BitGraph() throws Exception
    {
        createAndVerifyGraphStartingWithId( (long) pow( 2, 32 ), 400 );
    }
    
    @Test
    public void createAndVerify33BitGraph() throws Exception
    {
        createAndVerifyGraphStartingWithId( (long) pow( 2, 33 ), 1000 );
    }

    @Ignore("Blows up with a FileTooLarge error")
    @Test
    public void createAndVerify34BitGraph() throws Exception
    {
        createAndVerifyGraphStartingWithId( (long) pow( 2, 34 ), 1600 );
    }
    
    private void createAndVerifyGraphStartingWithId( long startId, int requiredHeapMb ) throws Exception
    {
        assumeTrue( machineIsOkToRunThisTest( requiredHeapMb ) );
        
        /*
         * Will create a layout like this:
         * 
         * (refNode) --> (node) --> (highNode)
         *           ...
         *           ...
         *           
         * Each node/relationship will have a bunch of different properties on them.
         */
        Node refNode = createReferenceNode( db );
        setHighIds( startId-1000 );
        
        byte[] bytes = new byte[45];
        bytes[2] = 5;
        bytes[10] = 42;
        Map<String, Object> properties = map( "number", 11, "short string", "test",
                "long string", "This is a long value, long enough", "array", bytes );
        Transaction tx = db.beginTx();
        int count = 10000;
        for ( int i = 0; i < count; i++ )
        {
            Node node = db.createNode();
            setProperties( node, properties );
            Relationship rel1 = refNode.createRelationshipTo( node, this );
            setProperties( rel1, properties );
            Node highNode = db.createNode();
            Relationship rel2 = node.createRelationshipTo( highNode, OTHER_TYPE );
            setProperties( rel2, properties );
            setProperties( highNode, properties );
            if ( i % 100 == 0 && i > 0 )
            {
                tx.success();
                tx.close();
                tx = db.beginTx();
            }
        }
        tx.success();
        tx.close();

        db = dbRule.restartDatabase();

        // Verify the data
        int verified = 0;
        
        try ( Transaction transaction = db.beginTx() )
        {
            refNode = db.getNodeById( refNode.getId() );
	        for ( Relationship rel : refNode.getRelationships( Direction.OUTGOING ) )
	        {
	            Node node = rel.getEndNode();
	            assertProperties( properties, node );
	            assertProperties( properties, rel );
	            Node highNode = node.getSingleRelationship( OTHER_TYPE, Direction.OUTGOING ).getEndNode();
	            assertProperties( properties, highNode );
	            verified++;
	        }
	        transaction.success();
        }
        assertEquals( count, verified );
    }

    private static final Label REFERENCE = DynamicLabel.label( "Reference" );
    
    private Node createReferenceNode( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( REFERENCE );
            tx.success();
            return node;
        }
    }

    public static boolean machineIsOkToRunThisTest(int requiredHeapMb )
    {
        if ( SystemUtils.IS_OS_WINDOWS )
        {
            // This test cannot be run on Windows because it can't handle files of this size in a timely manner
            return false;
        }
        if ( SystemUtils.IS_OS_MAC_OSX )
        {
            // This test cannot be run on Mac OS X because Mac OS X doesn't support sparse files
            return false;
        }
        
        long heapMb = Runtime.getRuntime().maxMemory() / (1000*1000); // Not 1024, matches better wanted result with -Xmx
        if ( heapMb < requiredHeapMb )
        {
            // This test requires a heap of size requiredHeapMb, this heap has only heapMb
            return false;
        }
        return true;
    }

    public static void assertProperties( Map<String, Object> properties, PropertyContainer entity )
    {
        int count = 0;
        for ( String key : entity.getPropertyKeys() )
        {
            Object expectedValue = properties.get( key );
            Object entityValue = entity.getProperty( key );
            if ( expectedValue.getClass().isArray() )
            {
                assertTrue( Arrays.equals( (byte[]) expectedValue, (byte[]) entityValue ) );
            }
            else
            {
                assertEquals( expectedValue, entityValue );
            }
            count++;
        }
        assertEquals( properties.size(), count );
    }

    private void setProperties( PropertyContainer entity, Map<String, Object> properties )
    {
        for ( Map.Entry<String, Object> property : properties.entrySet() )
        {
            entity.setProperty( property.getKey(), property.getValue() );
        }
    }

    private void testHighIds( long highMark, int minus, int requiredHeapMb ) throws IOException
    {
        if ( !machineIsOkToRunThisTest( requiredHeapMb ) )
        {
            return;
        }
        
        long idBelow = highMark-minus;
        setHighIds( idBelow );
        String propertyKey = "name";
        int intPropertyValue = 123;
        String stringPropertyValue = "Long string, longer than would fit in shortstring";
        long[] arrayPropertyValue = new long[] { 1021L, 321L, 343212L };
        
        Transaction tx = db.beginTx();
        Node nodeBelowTheLine = db.createNode();
        nodeBelowTheLine.setProperty( propertyKey, intPropertyValue );
        assertEquals( idBelow, nodeBelowTheLine.getId() );
        Node nodeAboveTheLine = db.createNode();
        nodeAboveTheLine.setProperty( propertyKey, stringPropertyValue );
        Relationship relBelowTheLine = nodeBelowTheLine.createRelationshipTo( nodeAboveTheLine, this );
        relBelowTheLine.setProperty( propertyKey, arrayPropertyValue );
        assertEquals( idBelow, relBelowTheLine.getId() );
        Relationship relAboveTheLine = nodeAboveTheLine.createRelationshipTo( nodeBelowTheLine, this );
        assertEquals( highMark, relAboveTheLine.getId() );
        assertEquals( highMark, nodeAboveTheLine.getId() );
        assertEquals( intPropertyValue, nodeBelowTheLine.getProperty( propertyKey ) );
        assertEquals( stringPropertyValue, nodeAboveTheLine.getProperty( propertyKey ) );
        assertTrue( Arrays.equals( arrayPropertyValue, (long[]) relBelowTheLine.getProperty( propertyKey ) ) );
        tx.success();
        tx.close();

        for ( int i = 0; i < 2; i++ )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                assertEquals( nodeAboveTheLine, db.getNodeById( highMark ) );
                assertEquals( idBelow, nodeBelowTheLine.getId() );
                assertEquals( highMark, nodeAboveTheLine.getId() );
                assertEquals( idBelow, relBelowTheLine.getId() );
                assertEquals( highMark, relAboveTheLine.getId() );
                assertEquals( relBelowTheLine,
                        db.getNodeById( idBelow ).getSingleRelationship( this, Direction.OUTGOING ) );
                assertEquals( relAboveTheLine,
                        db.getNodeById( idBelow ).getSingleRelationship( this, Direction.INCOMING ) );
                assertEquals( idBelow, relBelowTheLine.getId() );
                assertEquals( highMark, relAboveTheLine.getId() );
                assertEquals( asSet( asList( relBelowTheLine, relAboveTheLine ) ),
                        asSet( asCollection( db.getNodeById( idBelow ).getRelationships() ) ) );
                transaction.success();
            }
            if ( i == 0 )
            {
                db = dbRule.restartDatabase();
            }
        }
    }

    private void setHighIds( long id )
    {
        setHighId( IdType.NODE, id );
        setHighId( IdType.RELATIONSHIP, id );
        setHighId( IdType.PROPERTY, id );
        setHighId( IdType.ARRAY_BLOCK, id );
        setHighId( IdType.STRING_BLOCK, id );
    }
    
    private static <T> Collection<T> asSet( Collection<T> collection )
    {
        return new HashSet<T>( collection );
    }

    private void setHighId( IdType type, long highId )
    {
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( IdGeneratorFactory.class ).get( type ).setHighId( highId );
    }
}
