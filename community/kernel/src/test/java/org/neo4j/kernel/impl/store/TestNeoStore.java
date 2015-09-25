/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.UTF8;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.store.NeoStore.Position;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState.PropertyReceiver;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.NeoStoreDataSourceRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class TestNeoStore
{
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    @Rule
    public TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTestWithEphemeralFS( fs.get(), getClass() );
    @Rule
    public NeoStoreDataSourceRule dsRule = new NeoStoreDataSourceRule();
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private PageCache pageCache;
    private File storeDir;
    private PropertyStore pStore;
    private RelationshipTypeTokenStore rtStore;
    private NeoStoreDataSource ds;

    @Before
    public void setUpNeoStore() throws Exception
    {
        storeDir = dir.graphDbDir();
        Config config = new Config( new HashMap<String, String>(), GraphDatabaseSettings.class );
        Monitors monitors = new Monitors();
        pageCache = pageCacheRule.getPageCache( fs.get() );
        StoreFactory sf = new StoreFactory(
                storeDir,
                config,
                new DefaultIdGeneratorFactory( fs.get() ),
                pageCache,
                fs.get(),
                NullLogProvider.getInstance(),
                monitors );
        sf.createNeoStore().close();
    }

    private static class MyPropertyKeyToken extends Token
    {
        private static Map<String, Token> stringToIndex = new HashMap<>();
        private static Map<Integer, Token> intToIndex = new HashMap<>();

        protected MyPropertyKeyToken( String key, int keyId )
        {
            super( key, keyId );
        }

        public static Iterable<Token> index( String key )
        {
            if ( stringToIndex.containsKey( key ) )
            {
                return Arrays.asList( stringToIndex.get( key ) );
            }
            return Collections.emptyList();
        }

        public static Token getIndexFor( int index )
        {
            return intToIndex.get( index );
        }

        public static void add( MyPropertyKeyToken index )
        {
            stringToIndex.put( index.name(), index );
            intToIndex.put( index.id(), index );
        }
    }

    private Token createDummyIndex( int id, String key )
    {
        MyPropertyKeyToken index = new MyPropertyKeyToken( key, id );
        MyPropertyKeyToken.add( index );
        return index;
    }

    private void initializeStores( File storeDir, Map<String, String> additionalConfig ) throws IOException
    {
        ds = dsRule.getDataSource( storeDir, fs.get(), pageCache, additionalConfig );
        ds.init();
        ds.start();

        NeoStore neoStore = ds.get();
        pStore = neoStore.getPropertyStore();
        rtStore = neoStore.getRelationshipTypeTokenStore();
        storeLayer = ds.getStoreLayer();
        propertyLoader = new PropertyLoader( neoStore );
    }

    private byte txCount = (byte) 0;
    private KernelTransaction tx;
    private TransactionRecordState transaction;
    private StoreReadLayer storeLayer;
    private PropertyLoader propertyLoader;

    private void startTx() throws TransactionFailureException
    {
        txCount++;
        tx = ds.getKernel().newTransaction();
        transaction = (( KernelTransactionImplementation)tx).getTransactionRecordState();
    }

    private void commitTx() throws TransactionFailureException
    {
        tx.success();
        tx.close();
    }

    @After
    public void tearDownNeoStore()
    {
        for ( String file : new String[] {
                "neo",
                "neo.nodestore.db",
                "neo.nodestore.db.labels",
                "neo.propertystore.db",
                "neo.propertystore.db.index",
                "neo.propertystore.db.index.keys",
                "neo.propertystore.db.strings",
                "neo.propertystore.db.arrays",
                "neo.relationshipstore.db",
                "neo.relationshiptypestore.db",
                "neo.relationshiptypestore.db.names",
                "neo.schemastore.db",
        } )
        {
            fs.get().deleteFile( new File( storeDir, file ) );
            fs.get().deleteFile( new File( storeDir, file + ".id" ) );
        }

        File file = new File( "." );
        for ( File nioFile : fs.get().listFiles( file ) )
        {
            if ( nioFile.getName().startsWith( PhysicalLogFile.DEFAULT_NAME ) )
            {
                fs.get().deleteFile( nioFile );
            }
        }
    }

    private int index( String key )
    {
        Iterator<Token> itr = MyPropertyKeyToken.index( key ).iterator();
        if ( !itr.hasNext() )
        {
            int id = (int) nextId( PropertyKeyTokenRecord.class );
            createDummyIndex( id, key );
            transaction.createPropertyKeyToken( key, id );
            return id;
        }
        return itr.next().id();
    }

    private long nextId( Class<?> clazz )
    {
        NeoStore neoStore = ds.get();
        if ( clazz.equals( PropertyKeyTokenRecord.class ) )
        {
            return neoStore.getPropertyKeyTokenStore().nextId();
        }
        if ( clazz.equals( RelationshipType.class ) )
        {
            return neoStore.getRelationshipTypeTokenStore().nextId();
        }
        if ( clazz.equals( Node.class ) )
        {
            return neoStore.getNodeStore().nextId();
        }
        if ( clazz.equals( Relationship.class ) )
        {
            return neoStore.getRelationshipStore().nextId();
        }
        throw new IllegalArgumentException( clazz.getName() );
    }

    @Test
    public void testCreateNeoStore() throws Exception
    {
        initializeStores( storeDir, stringMap() );
        startTx();
        // setup test population
        long node1 = nextId( Node.class );
        transaction.nodeCreate( node1 );
        long node2 = nextId( Node.class );
        transaction.nodeCreate( node2 );
        DefinedProperty n1prop1 = transaction.nodeAddProperty(
                node1, index( "prop1" ), "string1" );
        DefinedProperty n1prop2 = transaction.nodeAddProperty(
                node1, index( "prop2" ), 1 );
        DefinedProperty n1prop3 = transaction.nodeAddProperty(
                node1, index( "prop3" ), true );

        DefinedProperty n2prop1 = transaction.nodeAddProperty(
                node2, index( "prop1" ), "string2" );
        DefinedProperty n2prop2 = transaction.nodeAddProperty(
                node2, index( "prop2" ), 2 );
        DefinedProperty n2prop3 = transaction.nodeAddProperty(
                node2, index( "prop3" ), false );

        int relType1 = (int) nextId( RelationshipType.class );
        String typeName1 = "relationshiptype1";
        transaction.createRelationshipTypeToken( typeName1, relType1 );
        int relType2 = (int) nextId( RelationshipType.class );
        String typeName2 = "relationshiptype2";
        transaction.createRelationshipTypeToken( typeName2, relType2 );
        long rel1 = nextId( Relationship.class );
        transaction.relCreate( rel1, relType1, node1, node2 );
        long rel2 = nextId( Relationship.class );
        transaction.relCreate( rel2, relType2, node2, node1 );

        DefinedProperty r1prop1 = transaction.relAddProperty(
                rel1, index( "prop1" ), "string1" );
        DefinedProperty r1prop2 = transaction.relAddProperty(
                rel1, index( "prop2" ), 1 );
        DefinedProperty r1prop3 = transaction.relAddProperty(
                rel1, index( "prop3" ), true );

        DefinedProperty r2prop1 = transaction.relAddProperty(
                rel2, index( "prop1" ), "string2" );
        DefinedProperty r2prop2 = transaction.relAddProperty(
                rel2, index( "prop2" ), 2 );
        DefinedProperty r2prop3 = transaction.relAddProperty(
                rel2, index( "prop3" ), false );
        commitTx();
        ds.stop();

        initializeStores( storeDir, stringMap() );
        startTx();
        // validate node
        validateNodeRel1( node1, n1prop1, n1prop2, n1prop3, rel1, rel2,
                relType1, relType2 );
        validateNodeRel2( node2, n2prop1, n2prop2, n2prop3, rel1, rel2,
                relType1, relType2 );
        // validate rels
        validateRel1( rel1, r1prop1, r1prop2, r1prop3, node1, node2, relType1 );
        validateRel2( rel2, r2prop1, r2prop2, r2prop3, node2, node1, relType2 );
        validateRelTypes( relType1, relType2 );
        // validate reltypes
        validateRelTypes( relType1, relType2 );
        commitTx();
        ds.stop();

        initializeStores( storeDir, stringMap() );
        startTx();
        // validate and delete rels
        deleteRel1( rel1, r1prop1, r1prop2, r1prop3, node1, node2, relType1 );
        deleteRel2( rel2, r2prop1, r2prop2, r2prop3, node2, node1, relType2 );
        // validate and delete nodes
        deleteNode1( node1, n1prop1, n1prop2, n1prop3 );
        deleteNode2( node2, n2prop1, n2prop2, n2prop3 );
        commitTx();
        ds.stop();

        initializeStores( storeDir, stringMap() );
        startTx();
        assertFalse( nodeExists( node1 ) );
        assertFalse( nodeExists( node2 ) );
        testGetRels( new long[]{rel1, rel2} );
        // testGetProps( neoStore, new int[] {
        // n1prop1, n1prop2, n1prop3, n2prop1, n2prop2, n2prop3,
        // r1prop1, r1prop2, r1prop3, r2prop1, r2prop2, r2prop3
        // } );
        long nodeIds[] = new long[10];
        for ( int i = 0; i < 3; i++ )
        {
            nodeIds[i] = nextId( Node.class );
            transaction.nodeCreate( nodeIds[i] );
            transaction.nodeAddProperty( nodeIds[i],
                    index( "nisse" ), new Integer( 10 - i ) );
        }
        for ( int i = 0; i < 2; i++ )
        {
            long id = nextId( Relationship.class );
            transaction.relCreate( id, relType1, nodeIds[i], nodeIds[i + 1] );
            transaction.relDelete( id );
        }
        for ( int i = 0; i < 3; i++ )
        {
            transaction.nodeDelete( nodeIds[i] );
        }
        commitTx();
        ds.stop();
    }

    private void validateNodeRel1( final long node, DefinedProperty prop1,
            DefinedProperty prop2, DefinedProperty prop3, long rel1, long rel2,
            final int relType1, final int relType2 ) throws IOException, EntityNotFoundException
    {
        assertTrue( nodeExists( node ) );
        ArrayMap<Integer, Pair<DefinedProperty,Long>> props = new ArrayMap<>();
        PropertyReceiver receiver = newPropertyReceiver( props );
        propertyLoader.nodeLoadProperties( node, receiver );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).other();
            PropertyRecord record = pStore.getRecord( id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).first().propertyKeyId() );
            DefinedProperty data = block.newPropertyData( pStore );
            if ( data.propertyKeyId() == prop1.propertyKeyId() )
            {
                assertEquals( "prop1", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( "string1", data.value() );
                transaction.nodeChangeProperty( node, prop1.propertyKeyId(), "-string1" );
            }
            else if ( data.propertyKeyId() == prop2.propertyKeyId() )
            {
                assertEquals( "prop2", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( 1, data.value() );
                transaction.nodeChangeProperty( node, prop2.propertyKeyId(), new Integer( -1 ) );
            }
            else if ( data.propertyKeyId() == prop3.propertyKeyId() )
            {
                assertEquals( "prop3", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( true, data.value() );
                transaction.nodeChangeProperty( node, prop3.propertyKeyId(), false );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );
        count = 0;

        try ( Cursor<NodeItem> nodeCursor = ((KernelStatement) tx.acquireStatement()).getStoreStatement()
                .acquireSingleNodeCursor(
                node ) )
        {
            nodeCursor.next();

            try ( Cursor<RelationshipItem> relationships = nodeCursor.get().relationships( Direction.BOTH ) )
            {
                while ( relationships.next() )
                {
                    long rel = relationships.get().id();
                    if ( rel == rel1 )
                    {
                        assertEquals( node, relationships.get().startNode() );
                        assertEquals( relType1, relationships.get().type() );
                    }
                    else if ( rel == rel2 )
                    {
                        assertEquals( node, relationships.get().endNode() );
                        assertEquals( relType2, relationships.get().type() );
                    }
                    else
                    {
                        throw new IOException();
                    }
                    count++;

                }
            }
        }
        assertEquals( 2, count );
    }

    private PropertyReceiver newPropertyReceiver( final ArrayMap<Integer, Pair<DefinedProperty, Long>> props )
    {
        return new PropertyReceiver()
        {
            @Override
            public void receive( DefinedProperty property, long propertyRecordId )
            {
                props.put( property.propertyKeyId(), Pair.of( property, propertyRecordId ) );
            }
        };
    }

    private void validateNodeRel2( final long node, DefinedProperty prop1,
            DefinedProperty prop2, DefinedProperty prop3,
            long rel1, long rel2, final int relType1, final int relType2 )
                    throws IOException, EntityNotFoundException, RuntimeException
    {
        assertTrue( nodeExists( node ) );
        ArrayMap<Integer, Pair<DefinedProperty,Long>> props = new ArrayMap<>();
        propertyLoader.nodeLoadProperties( node, newPropertyReceiver( props ) );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).other();
            PropertyRecord record = pStore.getRecord( id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).first().propertyKeyId() );
            DefinedProperty data = block.newPropertyData( pStore );
            if ( data.propertyKeyId() == prop1.propertyKeyId() )
            {
                assertEquals( "prop1", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( "string2", data.value() );
                transaction.nodeChangeProperty( node, prop1.propertyKeyId(), "-string2" );
            }
            else if ( data.propertyKeyId() == prop2.propertyKeyId() )
            {
                assertEquals( "prop2", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( 2, data.value() );
                transaction.nodeChangeProperty( node, prop2.propertyKeyId(), new Integer( -2 ) );
            }
            else if ( data.propertyKeyId() == prop3.propertyKeyId() )
            {
                assertEquals( "prop3", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( false, data.value() );
                transaction.nodeChangeProperty( node, prop3.propertyKeyId(), true );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );
        count = 0;

        try ( Cursor<NodeItem> nodeCursor = ((KernelStatement) tx.acquireStatement()).getStoreStatement().acquireSingleNodeCursor(
                node ) )
        {
            nodeCursor.next();

            try ( Cursor<RelationshipItem> relationships = nodeCursor.get().relationships( Direction.BOTH ) )
            {
                while ( relationships.next() )
                {
                    long rel = relationships.get().id();
                    if ( rel == rel1 )
                    {
                        assertEquals( node, relationships.get().endNode() );
                        assertEquals( relType1, relationships.get().type() );
                    }
                    else if ( rel == rel2 )
                    {
                        assertEquals( node, relationships.get().startNode() );
                        assertEquals( relType2, relationships.get().type() );
                    }
                    else
                    {
                        throw new IOException();
                    }
                    count++;

                }
            }
        }
        assertEquals( 2, count );
    }

    private boolean nodeExists( long nodeId )
    {
        try (StoreStatement statement = storeLayer.acquireStatement())
        {
            try ( Cursor<NodeItem> node = statement.acquireSingleNodeCursor( nodeId ) )
            {
                return node.next();
            }
        }
    }

    private void validateRel1( long rel, DefinedProperty prop1,
            DefinedProperty prop2, DefinedProperty prop3, long firstNode, long secondNode,
            int relType ) throws IOException
    {
        ArrayMap<Integer, Pair<DefinedProperty,Long>> props = new ArrayMap<>();
        propertyLoader.relLoadProperties( rel, newPropertyReceiver( props ) );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).other();
            PropertyRecord record = pStore.getRecord( id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).first().propertyKeyId() );
            DefinedProperty data = block.newPropertyData( pStore );
            if ( data.propertyKeyId() == prop1.propertyKeyId() )
            {
                assertEquals( "prop1", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( "string1", data.value() );
                transaction.relChangeProperty( rel, prop1.propertyKeyId(), "-string1" );
            }
            else if ( data.propertyKeyId() == prop2.propertyKeyId() )
            {
                assertEquals( "prop2", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( 1, data.value() );
                transaction.relChangeProperty( rel, prop2.propertyKeyId(), new Integer( -1 ) );
            }
            else if ( data.propertyKeyId() == prop3.propertyKeyId() )
            {
                assertEquals( "prop3", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( true, data.value() );
                transaction.relChangeProperty( rel, prop3.propertyKeyId(), false );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );
        assertRelationshipData( rel, firstNode, secondNode, relType );
    }

    private void assertRelationshipData( long rel, final long firstNode, final long secondNode,
            final int relType )
    {
        try
        {
            storeLayer.relationshipVisit( rel, new RelationshipVisitor<RuntimeException>()
            {
                @Override
                public void visit( long relId, int type, long startNode, long endNode )
                {
                    assertEquals( firstNode, startNode );
                    assertEquals( secondNode, endNode );
                    assertEquals( relType, type );
                }
            } );
        }
        catch ( EntityNotFoundException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void validateRel2( long rel, DefinedProperty prop1,
            DefinedProperty prop2, DefinedProperty prop3,
            long firstNode, long secondNode, int relType ) throws IOException
    {
        ArrayMap<Integer, Pair<DefinedProperty,Long>> props = new ArrayMap<>();
        propertyLoader.relLoadProperties( rel, newPropertyReceiver( props ) );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).other();
            PropertyRecord record = pStore.getRecord( id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).first().propertyKeyId() );
            DefinedProperty data = block.newPropertyData( pStore );
            if ( data.propertyKeyId() == prop1.propertyKeyId() )
            {
                assertEquals( "prop1", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( "string2", data.value() );
                transaction.relChangeProperty( rel, prop1.propertyKeyId(), "-string2" );
            }
            else if ( data.propertyKeyId() == prop2.propertyKeyId() )
            {
                assertEquals( "prop2", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( 2, data.value() );
                transaction.relChangeProperty( rel, prop2.propertyKeyId(), new Integer( -2 ) );
            }
            else if ( data.propertyKeyId() == prop3.propertyKeyId() )
            {
                assertEquals( "prop3", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( false, data.value() );
                transaction.relChangeProperty( rel, prop3.propertyKeyId(), true );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );
        assertRelationshipData( rel, firstNode, secondNode, relType );
    }

    private void validateRelTypes( int relType1, int relType2 )
            throws IOException
    {
        Token data = rtStore.getToken( relType1 );
        assertEquals( relType1, data.id() );
        assertEquals( "relationshiptype1", data.name() );
        data = rtStore.getToken( relType2 );
        assertEquals( relType2, data.id() );
        assertEquals( "relationshiptype2", data.name() );
        List<RelationshipTypeToken> allData = rtStore.getTokens( Integer.MAX_VALUE );
        assertEquals( 2, allData.size() );
        for ( int i = 0; i < 2; i++ )
        {
            if ( allData.get(i).id() == relType1 )
            {
                assertEquals( relType1, allData.get(i).id() );
                assertEquals( "relationshiptype1", allData.get(i).name() );
            }
            else if ( allData.get(i).id() == relType2 )
            {
                assertEquals( relType2, allData.get(i).id() );
                assertEquals( "relationshiptype2", allData.get(i).name() );
            }
            else
            {
                throw new IOException();
            }
        }
    }

    private void deleteRel1( long rel, DefinedProperty prop1, DefinedProperty prop2,
            DefinedProperty prop3, long firstNode, long secondNode, int relType )
                    throws IOException, EntityNotFoundException
    {
        ArrayMap<Integer, Pair<DefinedProperty,Long>> props = new ArrayMap<>();
        propertyLoader.relLoadProperties( rel, newPropertyReceiver( props ) );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).other();
            PropertyRecord record = pStore.getRecord( id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).first().propertyKeyId() );
            DefinedProperty data = block.newPropertyData( pStore );
            if ( data.propertyKeyId() == prop1.propertyKeyId() )
            {
                assertEquals( "prop1", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( "-string1", data.value() );
            }
            else if ( data.propertyKeyId() == prop2.propertyKeyId() )
            {
                assertEquals( "prop2", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( -1, data.value() );
            }
            else if ( data.propertyKeyId() == prop3.propertyKeyId() )
            {
                assertEquals( "prop3", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( false, data.value() );
                transaction.relRemoveProperty( rel, prop3.propertyKeyId() );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );
        CountingPropertyReceiver propertyCounter = new CountingPropertyReceiver();
        propertyLoader.relLoadProperties( rel, propertyCounter );
        assertEquals( 3, propertyCounter.count );
        assertRelationshipData( rel, firstNode, secondNode, relType );;
        transaction.relDelete( rel );

        assertHasRelationships( firstNode );

        assertHasRelationships( secondNode );
    }

    private static class CountingPropertyReceiver implements PropertyReceiver
    {
        private int count;

        @Override
        public void receive( DefinedProperty property, long propertyRecordId )
        {
            count++;
        }
    }

    private void deleteRel2( long rel, DefinedProperty prop1, DefinedProperty prop2,
            DefinedProperty prop3, long firstNode, long secondNode, int relType )
                    throws IOException, EntityNotFoundException
    {
        ArrayMap<Integer, Pair<DefinedProperty,Long>> props = new ArrayMap<>();
        propertyLoader.relLoadProperties( rel, newPropertyReceiver( props ) );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).other();
            PropertyRecord record = pStore.getRecord( id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).first().propertyKeyId() );
            DefinedProperty data = block.newPropertyData( pStore );
            if ( data.propertyKeyId() == prop1.propertyKeyId() )
            {
                assertEquals( "prop1", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( "-string2", data.value() );
            }
            else if ( data.propertyKeyId() == prop2.propertyKeyId() )
            {
                assertEquals( "prop2", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( -2, data.value() );
            }
            else if ( data.propertyKeyId() == prop3.propertyKeyId() )
            {
                assertEquals( "prop3", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( true, data.value() );
                transaction.relRemoveProperty( rel, prop3.propertyKeyId() );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );
        CountingPropertyReceiver propertyCounter = new CountingPropertyReceiver();
        propertyLoader.relLoadProperties( rel, propertyCounter );
        assertEquals( 3, propertyCounter.count );
        assertRelationshipData( rel, firstNode, secondNode, relType );
        transaction.relDelete( rel );

        assertHasRelationships( firstNode );

        assertHasRelationships( secondNode );

    }

    private void assertHasRelationships( long node )
    {
        try ( Cursor<NodeItem> nodeCursor = ((KernelStatement) tx.acquireStatement()).getStoreStatement()
                .acquireSingleNodeCursor(

                node ) )
        {
            nodeCursor.next();
            PrimitiveLongIterator rels = nodeCursor.get().getRelationships( Direction.BOTH );
            assertTrue( rels.hasNext() );
        }
    }

    private void deleteNode1( long node, DefinedProperty prop1,
            DefinedProperty prop2, DefinedProperty prop3 )
            throws IOException, EntityNotFoundException
    {
        ArrayMap<Integer, Pair<DefinedProperty,Long>> props = new ArrayMap<>();
        propertyLoader.nodeLoadProperties( node, newPropertyReceiver( props ) );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).other();
            PropertyRecord record = pStore.getRecord( id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).first().propertyKeyId() );
            DefinedProperty data = block.newPropertyData( pStore );
            if ( data.propertyKeyId() == prop1.propertyKeyId() )
            {
                assertEquals( "prop1", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( "-string1", data.value() );
            }
            else if ( data.propertyKeyId() == prop2.propertyKeyId() )
            {
                assertEquals( "prop2", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( -1, data.value() );
            }
            else if ( data.propertyKeyId() == prop3.propertyKeyId() )
            {
                assertEquals( "prop3", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( false, data.value() );
                transaction.nodeRemoveProperty( node, prop3.propertyKeyId() );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );
        CountingPropertyReceiver propertyCounter = new CountingPropertyReceiver();
        propertyLoader.nodeLoadProperties( node, propertyCounter );
        assertEquals( 3, propertyCounter.count );
        assertHasRelationships( node );
        transaction.nodeDelete( node );
    }

    private void deleteNode2( long node, DefinedProperty prop1,
            DefinedProperty prop2, DefinedProperty prop3 )
            throws IOException, EntityNotFoundException
    {
        ArrayMap<Integer, Pair<DefinedProperty,Long>> props = new ArrayMap<>();
        propertyLoader.nodeLoadProperties( node, newPropertyReceiver( props ) );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).other();
            PropertyRecord record = pStore.getRecord( id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).first().propertyKeyId() );
            DefinedProperty data = block.newPropertyData( pStore );
            if ( data.propertyKeyId() == prop1.propertyKeyId() )
            {
                assertEquals( "prop1", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( "-string2", data.value() );
            }
            else if ( data.propertyKeyId() == prop2.propertyKeyId() )
            {
                assertEquals( "prop2", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( -2, data.value() );
            }
            else if ( data.propertyKeyId() == prop3.propertyKeyId() )
            {
                assertEquals( "prop3", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( true, data.value() );
                transaction.nodeRemoveProperty( node, prop3.propertyKeyId() );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );
        CountingPropertyReceiver propertyCounter = new CountingPropertyReceiver();
        propertyLoader.nodeLoadProperties( node, propertyCounter );
        assertEquals( 3, propertyCounter.count );

        assertHasRelationships( node );

        transaction.nodeDelete( node );
    }

    private void testGetRels( long relIds[] )
    {
        try (StoreStatement statement = storeLayer.acquireStatement())
        {
            for ( long relId : relIds )
            {
                try ( Cursor<RelationshipItem> relationship = statement.acquireSingleRelationshipCursor( relId ) )
                {
                    assertFalse( relationship.next() );
                }
            }
        }
    }

    @Test
    public void testRels1() throws Exception
    {
        initializeStores( storeDir, stringMap() );
        startTx();
        int relType1 = (int) nextId( RelationshipType.class );
        String typeName = "relationshiptype1";
        transaction.createRelationshipTypeToken( typeName, relType1 );
        long nodeIds[] = new long[3];
        for ( int i = 0; i < 3; i++ )
        {
            nodeIds[i] = nextId( Node.class );
            transaction.nodeCreate( nodeIds[i] );
            transaction.nodeAddProperty( nodeIds[i],
                    index( "nisse" ), new Integer( 10 - i ) );
        }
        for ( int i = 0; i < 2; i++ )
        {
            transaction.relCreate( nextId( Relationship.class ),
                    relType1, nodeIds[i], nodeIds[i + 1] );
        }
        commitTx();
        startTx();
        for ( int i = 0; i < 3; i += 2 )
        {
            try ( Cursor<NodeItem> nodeCursor = ((KernelStatement) tx.acquireStatement()).getStoreStatement()
                    .acquireSingleNodeCursor(
                    nodeIds[i] ) )
            {
                nodeCursor.next();
                PrimitiveLongIterator relationships = nodeCursor.get().getRelationships( Direction.BOTH );
                while ( relationships.hasNext() )
                {
                    transaction.relDelete( relationships.next() );
                }
            }

            transaction.nodeDelete( nodeIds[i] );
        }
        commitTx();
        ds.stop();
    }

    @Test
    @Ignore
    public void testRels2() throws Exception
    {
        initializeStores( storeDir, stringMap() );
        startTx();
        int relType1 = (int) nextId( RelationshipType.class );
        String typeName = "relationshiptype1";
        transaction.createRelationshipTypeToken( typeName, relType1 );
        long nodeIds[] = new long[3];
        for ( int i = 0; i < 3; i++ )
        {
            nodeIds[i] = nextId( Node.class );
            transaction.nodeCreate( nodeIds[i] );
            transaction.nodeAddProperty( nodeIds[i],
                    index( "nisse" ), new Integer( 10 - i ) );
        }
        for ( int i = 0; i < 2; i++ )
        {
            transaction.relCreate( nextId( Relationship.class ),
                    relType1, nodeIds[i], nodeIds[i + 1] );
        }
        transaction.relCreate( nextId( Relationship.class ),
                relType1, nodeIds[0], nodeIds[2] );
        commitTx();
        startTx();
        for ( int i = 0; i < 3; i++ )
        {
            try ( Cursor<NodeItem> nodeCursor = ((KernelStatement) tx.acquireStatement()).getStoreStatement()
                    .acquireSingleNodeCursor(
                    nodeIds[i] ) )
            {
                nodeCursor.next();
                PrimitiveLongIterator relationships = nodeCursor.get().getRelationships( Direction.BOTH );
                while ( relationships.hasNext() )
                {
                    transaction.relDelete( relationships.next() );
                }
            }

            transaction.nodeDelete( nodeIds[i] );
        }
        commitTx();
        ds.stop();
    }

    @Test
    public void testRels3() throws Exception
    {
        // test linked list stuff during relationship delete
        initializeStores( storeDir, stringMap() );
        startTx();
        int relType1 = (int) nextId( RelationshipType.class );
        transaction.createRelationshipTypeToken( "relationshiptype1", relType1 );
        long nodeIds[] = new long[8];
        for ( int i = 0; i < nodeIds.length; i++ )
        {
            nodeIds[i] = nextId( Node.class );
            transaction.nodeCreate( nodeIds[i] );
        }
        for ( int i = 0; i < nodeIds.length / 2; i++ )
        {
            transaction.relCreate( nextId( Relationship.class ),
                    relType1, nodeIds[i], nodeIds[i * 2] );
        }
        long rel5 = nextId( Relationship.class );
        transaction.relCreate( rel5, relType1, nodeIds[0], nodeIds[5] );
        long rel2 = nextId( Relationship.class );
        transaction.relCreate( rel2, relType1, nodeIds[1], nodeIds[2] );
        long rel3 = nextId( Relationship.class );
        transaction.relCreate( rel3, relType1, nodeIds[1], nodeIds[3] );
        long rel6 = nextId( Relationship.class );
        transaction.relCreate( rel6, relType1, nodeIds[1], nodeIds[6] );
        long rel1 = nextId( Relationship.class );
        transaction.relCreate( rel1, relType1, nodeIds[0], nodeIds[1] );
        long rel4 = nextId( Relationship.class );
        transaction.relCreate( rel4, relType1, nodeIds[0], nodeIds[4] );
        long rel7 = nextId( Relationship.class );
        transaction.relCreate( rel7, relType1, nodeIds[0], nodeIds[7] );
        commitTx();
        startTx();
        transaction.relDelete( rel7 );
        transaction.relDelete( rel4 );
        transaction.relDelete( rel1 );
        transaction.relDelete( rel6 );
        transaction.relDelete( rel3 );
        transaction.relDelete( rel2 );
        transaction.relDelete( rel5 );
        commitTx();
        ds.stop();
    }

    @Test
    public void testProps1() throws Exception
    {
        initializeStores( storeDir, stringMap() );
        startTx();
        long nodeId = nextId( Node.class );
        transaction.nodeCreate( nodeId );
        pStore.nextId();
        DefinedProperty prop = transaction.nodeAddProperty(
                nodeId, index( "nisse" ),
                new Integer( 10 ) );
        commitTx();
        ds.stop();
        initializeStores( storeDir, stringMap() );
        startTx();
        transaction.nodeChangeProperty( nodeId, prop.propertyKeyId(), new Integer( 5 ) );
        transaction.nodeRemoveProperty( nodeId, prop.propertyKeyId() );
        transaction.nodeDelete( nodeId );
        commitTx();
        ds.stop();
    }

    @Test
    public void testSetBlockSize() throws Exception
    {
        File storeDir = dir.directory( "small_store" );
        initializeStores( storeDir, stringMap( "string_block_size", "62", "array_block_size", "302" ) );
        assertEquals( 62 + AbstractDynamicStore.BLOCK_HEADER_SIZE,
                pStore.getStringBlockSize() );
        assertEquals( 302 + AbstractDynamicStore.BLOCK_HEADER_SIZE,
                pStore.getArrayBlockSize() );
        ds.stop();
    }

    @Test
    public void setVersion() throws Exception
    {
        FileSystemAbstraction fileSystem = fs.get();
        File storeDir = new File("target/test-data/set-version").getAbsoluteFile();
        new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( storeDir ).shutdown();
        assertEquals( 0, NeoStore.setRecord( pageCache, new File( storeDir,
                NeoStore.DEFAULT_NAME ).getAbsoluteFile(), Position.LOG_VERSION, 10 ) );
        assertEquals( 10, NeoStore.setRecord( pageCache, new File( storeDir,
                NeoStore.DEFAULT_NAME ).getAbsoluteFile(), Position.LOG_VERSION, 12 ) );

        Monitors monitors = new Monitors();
        Config config = new Config( new HashMap<String, String>(), GraphDatabaseSettings.class );
        StoreFactory sf = new StoreFactory(
                storeDir,
                config,
                new DefaultIdGeneratorFactory( fileSystem ),
                pageCache,
                fileSystem,
                NullLogProvider.getInstance(),
                monitors );

        NeoStore neoStore = sf.newNeoStore( false );
        assertEquals( 12, neoStore.getCurrentLogVersion() );
        neoStore.close();
    }

    @Test
    public void shouldNotReadNonRecordDataAsRecord() throws Exception
    {
        FileSystemAbstraction fileSystem = fs.get();
        File neoStoreDir = new File( "/tmp/graph.db/neostore" ).getAbsoluteFile();
        StoreFactory factory =
                new StoreFactory( fileSystem, neoStoreDir, pageCache, NullLogProvider.getInstance(), new Monitors() );
        NeoStore neoStore = factory.newNeoStore( true );
        neoStore.setCreationTime( 3 );
        neoStore.setRandomNumber( 4 );
        neoStore.setCurrentLogVersion( 5 );
        neoStore.setLastCommittedAndClosedTransactionId( 6, 0, 0, 0 );
        neoStore.setStoreVersion( 7 );
        neoStore.setGraphNextProp( 8 );
        neoStore.setLatestConstraintIntroducingTx( 9 );
        neoStore.rebuildCountStoreIfNeeded();
        neoStore.flush();
        neoStore.close();

        File file = new File( neoStoreDir, NeoStore.DEFAULT_NAME );
        try ( StoreChannel channel = fileSystem.open( file, "rw" ) )
        {
            byte[] trailer =
                    UTF8.encode( CommonAbstractStore.buildTypeDescriptorAndVersion( neoStore.getTypeDescriptor() ) );
            channel.position( 0 );
            channel.write( ByteBuffer.wrap( UTF8.encode( "This is some data that is not a record." ) ) );
        }

        neoStore = factory.newNeoStore( false );
        assertEquals( NeoStore.FIELD_NOT_PRESENT, neoStore.getCreationTime() );
        assertEquals( NeoStore.FIELD_NOT_PRESENT, neoStore.getRandomNumber() );
        assertEquals( NeoStore.FIELD_NOT_PRESENT, neoStore.getCurrentLogVersion() );
        assertEquals( NeoStore.FIELD_NOT_PRESENT, neoStore.getLastCommittedTransactionId() );
        assertEquals( NeoStore.FIELD_NOT_PRESENT, neoStore.getStoreVersion() );
        assertEquals( 8, neoStore.getGraphNextProp() );
        assertEquals( 9, neoStore.getLatestConstraintIntroducingTx() );

        neoStore.close();
    }

    @Test
    public void testSetLatestConstraintTx() throws Exception
    {
        // given
        Monitors monitors = new Monitors();
        Config config = new Config( new HashMap<String, String>(), GraphDatabaseSettings.class );
        StoreFactory sf = new StoreFactory(
                dir.directory(),
                config,
                new DefaultIdGeneratorFactory( fs.get() ),
                pageCacheRule.getPageCache( fs.get() ),
                fs.get(),
                NullLogProvider.getInstance(),
                monitors );

        // when
        NeoStore neoStore = sf.newNeoStore( true );

        // then the default is 0
        assertEquals( 0l, neoStore.getLatestConstraintIntroducingTx() );

        // when
        neoStore.setLatestConstraintIntroducingTx( 10l );

        // then
        assertEquals( 10l, neoStore.getLatestConstraintIntroducingTx() );

        // when
        neoStore.flush();
        neoStore.close();
        neoStore = sf.newNeoStore( false );

        // then the value should have been stored
        assertEquals( 10l, neoStore.getLatestConstraintIntroducingTx() );
        neoStore.close();
    }

    @Test
    public void shouldInitializeTheTxIdToOne()
    {
        StoreFactory factory =
                new StoreFactory( fs.get(), new File( "graph.db/neostore" ), pageCache, NullLogProvider.getInstance(),
                        new Monitors() );

        NeoStore neoStore = factory.newNeoStore( true );
        neoStore.close();

        neoStore = factory.newNeoStore( false );
        long lastCommittedTransactionId = neoStore.getLastCommittedTransactionId();
        neoStore.close();

        assertEquals( TransactionIdStore.BASE_TX_ID, lastCommittedTransactionId );
    }

    @Test
    public void shouldThrowUnderlyingStorageExceptionWhenFailingToLoadStorage()
    {
        FileSystemAbstraction fileSystem = fs.get();
        File neoStoreDir = new File( "/tmp/graph.db/neostore" ).getAbsoluteFile();
        StoreFactory factory =
                new StoreFactory( fileSystem, neoStoreDir, pageCache, NullLogProvider.getInstance(), new Monitors() );

        NeoStore neoStore = factory.newNeoStore( true );
        neoStore.close();
        File file = new File( neoStoreDir, NeoStore.DEFAULT_NAME );
        fileSystem.deleteFile( file );
        exception.expect( UnderlyingStorageException.class );
        factory.newNeoStore( false );
        exception.expect( IllegalStateException.class );
    }

    @Test
    public void shouldAddUpgradeFieldsToTheNeoStoreIfNotPresent() throws IOException
    {
        FileSystemAbstraction fileSystem = fs.get();
        File neoStoreDir = new File( "/tmp/graph.db/neostore" ).getAbsoluteFile();
        StoreFactory factory =
                new StoreFactory( fileSystem, neoStoreDir, pageCache, NullLogProvider.getInstance(), new Monitors() );
        NeoStore neoStore = factory.newNeoStore( true );
        neoStore.setCreationTime( 3 );
        neoStore.setRandomNumber( 4 );
        neoStore.setCurrentLogVersion( 5 );
        neoStore.setLastCommittedAndClosedTransactionId( 6, 0, 0, 0 );
        neoStore.setStoreVersion( 7 );
        neoStore.setGraphNextProp( 8 );
        neoStore.setLatestConstraintIntroducingTx( 9 );
        neoStore.rebuildCountStoreIfNeeded();
        neoStore.flush();
        neoStore.close();

        File file = new File( neoStoreDir, NeoStore.DEFAULT_NAME );
        try ( StoreChannel channel = fileSystem.open( file, "rw" ) )
        {
            byte[] trailer = UTF8.encode( CommonAbstractStore.buildTypeDescriptorAndVersion( neoStore
                    .getTypeDescriptor() ) );
            channel.truncate( channel.size() - 2 * NeoStore.RECORD_SIZE );
            channel.position( channel.size() - trailer.length );
            channel.write( ByteBuffer.wrap( trailer ) );
        }

        assertNotEquals( 10, neoStore.getUpgradeTransaction()[0] );
        assertNotEquals( 11, neoStore.getUpgradeTime() );

        NeoStore.setRecord( pageCache, file, Position.UPGRADE_TRANSACTION_ID, 10 );
        NeoStore.setRecord( pageCache, file, Position.UPGRADE_TRANSACTION_CHECKSUM, 11 );
        NeoStore.setRecord( pageCache, file, Position.UPGRADE_TIME, 12 );

        neoStore = factory.newNeoStore( false );
        assertEquals( 3, neoStore.getCreationTime() );
        assertEquals( 4, neoStore.getRandomNumber() );
        assertEquals( 5, neoStore.getCurrentLogVersion() );
        assertEquals( 6, neoStore.getLastCommittedTransactionId() );
        assertEquals( 7, neoStore.getStoreVersion() );
        assertEquals( 8, neoStore.getGraphNextProp() );
        assertEquals( 9, neoStore.getLatestConstraintIntroducingTx() );
        assertArrayEquals( new long[] {10, 11}, neoStore.getUpgradeTransaction() );
        assertEquals( 12, neoStore.getUpgradeTime() );
        neoStore.close();
    }
}
