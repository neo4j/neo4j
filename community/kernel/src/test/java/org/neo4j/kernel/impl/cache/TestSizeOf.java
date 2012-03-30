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

package org.neo4j.kernel.impl.cache;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.cache.SizeOfs.REFERENCE_SIZE;
import static org.neo4j.kernel.impl.cache.SizeOfs.sizeOf;
import static org.neo4j.kernel.impl.cache.SizeOfs.sizeOfArray;
import static org.neo4j.kernel.impl.cache.SizeOfs.withArrayOverhead;
import static org.neo4j.kernel.impl.cache.SizeOfs.withArrayOverheadIncludingReferences;
import static org.neo4j.kernel.impl.cache.SizeOfs.withObjectOverhead;
import static org.neo4j.kernel.impl.cache.SizeOfs.withReference;

import java.lang.reflect.Array;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager.CacheType;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestGraphDatabaseFactory;

public class TestSizeOf
{
    private static GraphDatabaseAPI db;
    public static final int _8_BYTES_FOR_ID = 8;
    public static final int _4_BYTES_FOR_INDEX = 4;
    public static final int _4_BYTES_FOR_VALUE = 4;

    @BeforeClass
    public static void setupDB()
    {
        db = new ImpermanentGraphDatabase( stringMap( Config.CACHE_TYPE, CacheType.gcr.name() ) );
    }
    
    @AfterClass
    public static void shutdown() throws Exception
    {
        db.shutdown();
    }
    
    @Before
    public void clearCache()
    {
        db.getNodeManager().clearCache();
    }
    
    @SuppressWarnings( "unchecked" )
    private Cache<NodeImpl> getNodeCache()
    {
        // This is a bit fragile because we depend on the order of caches() returns its caches.
        return (Cache<NodeImpl>) IteratorUtil.first( db.getNodeManager().caches() );
    }
    
    @SuppressWarnings( "unchecked" )
    private Cache<RelationshipImpl> getRelationshipCache()
    {
        // This is a bit fragile because we depend on the order of caches() returns its caches.
        return (Cache<RelationshipImpl>) IteratorUtil.last( db.getNodeManager().caches() );
    }
    
    private Node createNodeAndLoadFresh( Map<String, Object> properties, int nrOfRelationships, int nrOfTypes )
    {
        return createNodeAndLoadFresh( properties, nrOfRelationships, nrOfTypes, 0 );
    }
    
    private Node createNodeAndLoadFresh( Map<String, Object> properties, int nrOfRelationships, int nrOfTypes, int directionStride )
    {
        Node node = null;
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().
            newImpermanentDatabaseBuilder().
            setConfig( GraphDatabaseSettings.cache_type, GraphDatabaseSettings.CacheTypeSetting.gcr ).
            newGraphDatabase();
        Transaction tx = db.beginTx();
        try
        {
            node = db.createNode();
            setProperties( properties, node );
            for ( int t = 0; t < nrOfTypes; t++ )
            {
                RelationshipType type = DynamicRelationshipType.withName( relTypeName( t ) );
                for ( int i = 0, dir = 0; i < nrOfRelationships; i++, dir = (dir+directionStride)%3 )
                {
                    switch ( dir )
                    {
                    case 0: node.createRelationshipTo( db.createNode(), type ); break;
                    case 1: db.createNode().createRelationshipTo( node, type ); break;
                    case 2: node.createRelationshipTo( node, type ); break;
                    default: throw new IllegalArgumentException( "Invalid direction " + dir );
                    }
                }
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        
        clearCache();
        
        if ( !properties.isEmpty() )
            loadProperties( node );
        if ( nrOfRelationships*nrOfTypes > 0 )
            count( node.getRelationships() );
        
        return node;
    }

    private void loadProperties( PropertyContainer entity )
    {
        for ( String key : entity.getPropertyKeys() )
            entity.getProperty( key );
    }

    private void setProperties( Map<String, Object> properties, PropertyContainer entity )
    {
        for ( Map.Entry<String, Object> property : properties.entrySet() )
            entity.setProperty( property.getKey(), property.getValue() );
    }
    
    private Relationship createRelationshipAndLoadFresh( Map<String, Object> properties )
    {
        Relationship relationship = null;
        Transaction tx = db.beginTx();
        try
        {
            relationship = db.createNode().createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            setProperties( properties, relationship );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        
        clearCache();
        
        if ( !properties.isEmpty() )
            loadProperties( relationship );
        
        return relationship;
    }

    private String relTypeName( int t )
    {
        return "mytype" + t;
    }
    
    @Test
    public void cacheSizeCorrelatesWithNodeSizeAfterFullyLoadingRelationships() throws Exception
    {
        // Create node with a couple of relationships
        Node node = createNodeAndLoadFresh( map(), 10, 1 );
        Cache<NodeImpl> nodeCache = getNodeCache();
        
        // Just an initial sanity assertion, we start off with a clean cache
        clearCache();
        assertEquals( 0, nodeCache.size() );
        
        // Fully cache the node and its relationships
        count( node.getRelationships() );
        
        // Now the node cache size should be the same as doing node.size()
        assertEquals( db.getNodeManager().getNodeForProxy( node.getId(), null ).size(), nodeCache.size() );
    }
    
    private int sizeOfNode( Node node )
    {
        return db.getNodeManager().getNodeForProxy( node.getId(), null ).size();
    }
    
    private int sizeOfRelationship( Relationship relationship )
    {
        return db.getNodeManager().getRelationshipForProxy( relationship.getId(), null ).size();
    }
    
    private int withNodeOverhead( int size )
    {
        return withObjectOverhead(
                REFERENCE_SIZE/*properties reference*/+
                8/*registeredSize w/ padding*/+
                REFERENCE_SIZE/*relationships reference*/+
                8/*relChainPosition*/+
                8/*id*/+
                size );
    }
    
    private int withRelationshipOverhead( int size )
    {
        return withObjectOverhead(
                REFERENCE_SIZE/*properties reference*/+
                8/*registeredSize w/ padding*/+
                8/*idAndMore*/+
                8/*startNodeId and endNodeId*/+
                size );
    }
    
    public static RelationshipArraySize array( String type )
    {
        return new RelationshipArraySize( type );
    }
    
    private static class RelationshipArraySize
    {
        private final String type;
        private int nrOut;
        private int nrIn;
        private int nrLoop;

        RelationshipArraySize( String type )
        {
            this.type = type;
        }

        RelationshipArraySize withOutRelationships( int nrOut )
        {
            this.nrOut = nrOut;
            return this;
        }

        RelationshipArraySize withInRelationships( int nrIn )
        {
            this.nrIn = nrIn;
            return this;
        }

        RelationshipArraySize withLoopRelationships( int nrLoop )
        {
            this.nrLoop = nrLoop;
            return this;
        }
        
        RelationshipArraySize withRelationshipInAllDirections( int nr )
        {
            this.nrOut = this.nrIn = this.nrLoop = nr;
            return this;
        }

        int size()
        {
            int size = REFERENCE_SIZE + sizeOf( type ) +  // for the type String reference and size itself in RelIdArray
                    REFERENCE_SIZE * 2; // for the IdBlock references in RelIdArray
            for ( int rels : new int[] { nrOut, nrIn, nrLoop } )
            {
                if ( rels > 0 )
                    size += withObjectOverhead( withReference( withArrayOverhead( 4 * (rels + 1) ) ) );
            }
            if ( nrLoop > 0 )
                size += REFERENCE_SIZE; // RelIdArrayWithLoops is used for for those with loops in
            return withObjectOverhead( size );
        }
    }
    
    static RelationshipsSize relationships()
    {
        return new RelationshipsSize();
    }
    
    private static class RelationshipsSize
    {
        private int length;
        private int size;
        
        RelationshipsSize add( RelationshipArraySize array )
        {
            this.size += array.size();
            this.length++;
            return this;
        }
        
        int size()
        {
            return withArrayOverheadIncludingReferences( size, length );
        }
    }
    
    static PropertiesSize properties()
    {
        return new PropertiesSize();
    }
    
    private static class PropertiesSize
    {
        private int length;
        private int size;
        
        /*
         * For each PropertyData, this will include the object overhead of the PropertyData object itself.
         */
        private PropertiesSize add( int size )
        {
            length++;
            this.size += withObjectOverhead( size );
            return this;
        }
        
        PropertiesSize withSmallPrimitiveProperty()
        {
            return add( _8_BYTES_FOR_ID + _4_BYTES_FOR_INDEX + _4_BYTES_FOR_VALUE );
        }
        
        PropertiesSize withStringProperty( String value )
        {
            return add( _8_BYTES_FOR_ID + _4_BYTES_FOR_INDEX + withReference( sizeOf( value ) ) );
        }
        
        PropertiesSize withArrayProperty( Object array )
        {
            return add( _8_BYTES_FOR_ID + _4_BYTES_FOR_INDEX + REFERENCE_SIZE + sizeOfArray( array ) );
        }
        
        public int size()
        {
            return withArrayOverheadIncludingReferences( size, length );
        }
    }

    private void assertArraySize( Object array, int sizePerItem )
    {
        assertEquals( withArrayOverhead( Array.getLength( array )*sizePerItem ), sizeOfArray( array ) );
    }
    
    @Test
    public void sizeOfEmptyNode() throws Exception
    {
        Node node = createNodeAndLoadFresh( map(), 0, 0 );
        assertEquals( withNodeOverhead( 0 ), sizeOfNode( node ) );
    }

    @Test
    public void sizeOfNodeWithOneProperty() throws Exception
    {
        Node node = createNodeAndLoadFresh( map( "age", 5 ), 0, 0 );
        assertEquals( withNodeOverhead( properties().withSmallPrimitiveProperty().size() ), sizeOfNode( node ) );
    }
    
    @Test
    public void sizeOfNodeWithSomeProperties() throws Exception
    {
        String name = "Mattias";
        Node node = createNodeAndLoadFresh( map( "age", 5, "name", name ), 0, 0 );
        assertEquals( withNodeOverhead(
                properties().withSmallPrimitiveProperty().withStringProperty( name ).size() ),
                sizeOfNode( node ) );
    }

    @Test
    public void sizeOfNodeWithOneRelationship() throws Exception
    {
        Node node = createNodeAndLoadFresh( map(), 1, 1 );
        assertEquals( withNodeOverhead(
                relationships().add( array( relTypeName( 0 ) ).withOutRelationships( 1 ) ).size() ),
                sizeOfNode( node ) );
    }
    
    @Test
    public void sizeOfNodeWithSomeRelationshipsOfSameType() throws Exception
    {
        Node node = createNodeAndLoadFresh( map(), 10, 1 );
        assertEquals( withNodeOverhead( relationships().add(
                array( relTypeName( 0 ) ).withOutRelationships( 10 ) ).size() ), sizeOfNode( node ) );
    }
    
    @Test
    public void sizeOfNodeWithSomeRelationshipOfDifferentTypes() throws Exception
    {
        Node node = createNodeAndLoadFresh( map(), 3, 3 );
        assertEquals( withNodeOverhead( relationships().add(
                array( relTypeName( 0 ) ).withOutRelationships( 3 ) ).add(
                array( relTypeName( 1 ) ).withOutRelationships( 3 ) ).add(
                array( relTypeName( 2 ) ).withOutRelationships( 3 ) ).size() ), sizeOfNode( node ) );
    }
    
    @Test
    public void sizeOfNodeWithSomeRelationshipOfDifferentTypesAndDirections() throws Exception
    {
        Node node = createNodeAndLoadFresh( map(), 9, 3, 1 );
        assertEquals( withNodeOverhead( relationships().add(
                array( relTypeName( 0 ) ).withRelationshipInAllDirections( 3 ) ).add(
                array( relTypeName( 1 ) ).withRelationshipInAllDirections( 3 ) ).add(
                array( relTypeName( 2 ) ).withRelationshipInAllDirections( 3 ) ).size() ),
                sizeOfNode( node ) );
    }

    @Test
    public void sizeOfNodeWithRelationshipsAndProperties() throws Exception
    {
        int[] array = new int[]{10, 11, 12, 13};
        Node node = createNodeAndLoadFresh( map( "age", 10, "array", array ), 9, 3, 1 );
        assertEquals(
            withNodeOverhead( relationships().add(
                array( relTypeName( 0 ) ).withRelationshipInAllDirections( 3 ) ).add(
                array( relTypeName( 1 ) ).withRelationshipInAllDirections( 3 ) ).add(
                array( relTypeName( 2 ) ).withRelationshipInAllDirections( 3 ) ).size() +
                properties().withSmallPrimitiveProperty().withArrayProperty( array ).size() ),
            sizeOfNode( node ) );
    }

    @Test
    public void sizeOfEmptyRelationship() throws Exception
    {
        Relationship relationship = createRelationshipAndLoadFresh( map() );
        assertEquals( withRelationshipOverhead( 0 ), sizeOfRelationship( relationship ) );
    }
    
    @Test
    public void sizeOfRelationshipWithOneProperty() throws Exception
    {
        Relationship relationship = createRelationshipAndLoadFresh( map( "age", 5 ) );
        assertEquals( withRelationshipOverhead( properties().withSmallPrimitiveProperty().size() ),
                sizeOfRelationship( relationship ) );
    }
    
    @Test
    public void sizeOfRelationshipWithSomeProperties() throws Exception
    {
        String name = "Mattias";
        Relationship relationship = createRelationshipAndLoadFresh( map( "age", 5, "name", name ) );
        assertEquals( withRelationshipOverhead(
                properties().withSmallPrimitiveProperty().withStringProperty( name ).size() ),
                sizeOfRelationship( relationship ) );
    }

    @Test
    public void assumeWorstCaseJavaObjectOverhead() throws Exception
    {
        int size = 88;
        assertEquals( size+16, withObjectOverhead( size ) );
    }
    
    @Test
    public void assumeWorstCaseJavaArrayObjectOverhead() throws Exception
    {
        int size = 88;
        assertEquals( size+24, withArrayOverhead( size ) );
    }
    
    @Test
    public void assumeWorstCaseJavaArrayObjectOverheadIncludingReferences() throws Exception
    {
        int size = 24;
        int length = 5;
        assertEquals( withArrayOverhead( size+REFERENCE_SIZE*length ), withArrayOverheadIncludingReferences( size, length ) );
    }
    
    @Test
    public void assumeWorstCaseReferenceToJavaObject() throws Exception
    {
        int size = 24;
        assertEquals( size + REFERENCE_SIZE, withReference( size ) );
    }
    
    @Test
    public void sizeOfPrimitiveArrayIsCorrectlyCalculated() throws Exception
    {
        assertArraySize( new boolean[] { true, false }, 1 );
        assertArraySize( new byte[] { 1, 2, 3 }, 1 );
        assertArraySize( new short[] { 23, 21, 1, 45 }, 2 );
        assertArraySize( new int[] { 100, 121, 1, 3, 45 }, 4 );
        assertArraySize( new long[] { 403L, 3849329L, 23829L }, 8 );
        assertArraySize( new float[] { 342.0F, 21F, 43.4567F }, 4 );
        assertArraySize( new double[] { 45748.98D, 38493D }, 8 );
        
        // 32 includes the reference to the Integer item (8), object overhead (16) and int value w/ padding (8)
        assertArraySize( new Integer[] { 123, 456, 789 }, 32 );
    }
}
