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
package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.CombiningIterable;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.TransactionInterceptorProviders;
import org.neo4j.kernel.api.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaConnection;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockManagerImpl;
import org.neo4j.kernel.impl.transaction.PlaceboTm;
import org.neo4j.kernel.impl.transaction.RagManager;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.transaction.xaframework.DefaultLogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies;
import org.neo4j.kernel.impl.transaction.xaframework.RecoveryVerifier;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.SingleLoggingService;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class TestNeoStore
{
    private PropertyStore pStore;
    private RelationshipTypeStore rtStore;
    private NeoStoreXaDataSource ds;
    private NeoStoreXaConnection xaCon;
    private final EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
    private final String path = "dir";

    private File file( String name )
    {
        return new File( path, name);
    }

    @Before
    public void setUpNeoStore() throws Exception
    {
        Config config = new Config( new HashMap<String, String>(), GraphDatabaseSettings.class );
        StoreFactory sf = new StoreFactory( config, new DefaultIdGeneratorFactory(), new DefaultWindowPoolFactory(),
                fileSystem, StringLogger.SYSTEM, null );
        sf.createNeoStore( file( "neo" ) ).close();
    }

    private static class MyPropertyIndex extends
            org.neo4j.kernel.impl.core.PropertyIndex
    {
        private static Map<String, PropertyIndex> stringToIndex = new HashMap<String, PropertyIndex>();
        private static Map<Integer, PropertyIndex> intToIndex = new HashMap<Integer, PropertyIndex>();

        protected MyPropertyIndex( String key, int keyId )
        {
            super( key, keyId );
        }

        public static Iterable<PropertyIndex> index( String key )
        {
            if ( stringToIndex.containsKey( key ) )
            {
                return Arrays.asList( new PropertyIndex[]{stringToIndex
                        .get( key )} );
            }
            return Collections.emptyList();
        }

        public static PropertyIndex getIndexFor( int index )
        {
            return intToIndex.get( index );
        }

        public static void add( MyPropertyIndex index )
        {
            // TODO Auto-generated method stub
            stringToIndex.put( index.getKey(), index );
            intToIndex.put( index.getKeyId(), index );
        }
    }

    private PropertyIndex createDummyIndex( int id, String key )
    {
        MyPropertyIndex index = new MyPropertyIndex( key, id );
        MyPropertyIndex.add( index );
        return index;
    }

    private void initializeStores() throws IOException
    {
        LockManager lockManager = new LockManagerImpl( new RagManager() );

        final Config config = new Config( MapUtil.stringMap(
                InternalAbstractGraphDatabase.Configuration.store_dir.name(), path,
                InternalAbstractGraphDatabase.Configuration.neo_store.name(), "neo",
                InternalAbstractGraphDatabase.Configuration.logical_log.name(), file( "nioneo_logical.log" ).getPath() ),
                GraphDatabaseSettings.class );
        StoreFactory sf = new StoreFactory( config, new DefaultIdGeneratorFactory(), new DefaultWindowPoolFactory(),
                fileSystem, StringLogger.DEV_NULL, null );

        ds = new NeoStoreXaDataSource(config, sf, lockManager, StringLogger.DEV_NULL,
                new XaFactory(config, TxIdGenerator.DEFAULT, new PlaceboTm( lockManager, TxIdGenerator.DEFAULT ),
                        new DefaultLogBufferFactory(), fileSystem, new DevNullLoggingService(), RecoveryVerifier.ALWAYS_VALID,
                        LogPruneStrategies.NO_PRUNING ), TransactionStateFactory.noStateFactory( new DevNullLoggingService() ),
                        noCacheAccess(), mock(SchemaIndexProvider.class),
                        new TransactionInterceptorProviders( Collections.<TransactionInterceptorProvider>emptyList(), new DependencyResolver()
        {
            @Override
            public <T> T resolveDependency( Class<T> type ) throws IllegalArgumentException
            {
                return (T) config;
            }
        } ), null, new SingleLoggingService( DEV_NULL ) );
        ds.init();
        ds.start();

        xaCon = ds.getXaConnection();
        pStore = xaCon.getPropertyStore();
        rtStore = xaCon.getRelationshipTypeStore();
    }

    private CacheAccessBackDoor noCacheAccess()
    {
        return new CacheAccessBackDoor()
        {
            @Override
            public void removeSchemaRuleFromCache( long id )
            {
            }
            
            @Override
            public void removeRelationshipTypeFromCache( int id )
            {
            }
            
            @Override
            public void removeRelationshipFromCache( long id )
            {
            }
            
            @Override
            public void removeNodeFromCache( long nodeId )
            {
            }
            
            @Override
            public void removeGraphPropertiesFromCache()
            {
            }
            
            @Override
            public void patchDeletedRelationshipNodes( long relId, long firstNodeId, long firstNodeNextRelId,
                    long secondNodeId, long secondNodeNextRelId )
            {
            }
            
            @Override
            public void addSchemaRule( SchemaRule schemaRule )
            {
            }
            
            @Override
            public void addRelationshipType( NameData type )
            {
            }
            
            @Override
            public void addPropertyIndex( NameData index )
            {
            }
        };
    }

    private Xid dummyXid;
    private byte txCount = (byte) 0;
    XAResource xaResource;

    private void startTx() throws XAException
    {
        dummyXid = new XidImpl( new byte[txCount], new byte[txCount] );
        txCount++;
        xaResource = xaCon.getXaResource();
        xaResource.start( dummyXid, XAResource.TMNOFLAGS );
    }

    private void commitTx() throws XAException
    {
        xaResource.end( dummyXid, XAResource.TMSUCCESS );
        xaResource.commit( dummyXid, true );
        // xaCon.clearAllTransactions();
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
            fileSystem.deleteFile( file( file ) );
            fileSystem.deleteFile( file( file + ".id" ) );
        }
        
        File file = new File( "." );
        for ( File nioFile : fileSystem.listFiles( file ) )
        {
            if ( nioFile.getName().startsWith( "nioneo_logical.log" ) )
            {
                fileSystem.deleteFile( nioFile );
            }
        }
    }

    private PropertyIndex index( String key )
    {
        Iterator<PropertyIndex> itr = MyPropertyIndex.index( key ).iterator();
        if ( !itr.hasNext() )
        {
            int id = (int) ds.nextId( PropertyIndex.class );
            PropertyIndex index = createDummyIndex( id, key );
            xaCon.getWriteTransaction().createPropertyIndex( key, id );
            return index;
        }
        return itr.next();
    }

    @Test
    public void testCreateNeoStore() throws Exception
    {
        initializeStores();
        startTx();
        // setup test population
        long node1 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node1 );
        long node2 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node2 );
        PropertyData n1prop1 = xaCon.getWriteTransaction().nodeAddProperty(
                node1, index( "prop1" ), "string1" );
        PropertyData n1prop2 = xaCon.getWriteTransaction().nodeAddProperty(
                node1, index( "prop2" ), 1 );
        PropertyData n1prop3 = xaCon.getWriteTransaction().nodeAddProperty(
                node1, index( "prop3" ), true );

        PropertyData n2prop1 = xaCon.getWriteTransaction().nodeAddProperty(
                node2, index( "prop1" ), "string2" );
        PropertyData n2prop2 = xaCon.getWriteTransaction().nodeAddProperty(
                node2, index( "prop2" ), 2 );
        PropertyData n2prop3 = xaCon.getWriteTransaction().nodeAddProperty(
                node2, index( "prop3" ), false );

        int relType1 = (int) ds.nextId( RelationshipType.class );
        xaCon.getWriteTransaction().createRelationshipType( relType1, "relationshiptype1" );
        int relType2 = (int) ds.nextId( RelationshipType.class );
        xaCon.getWriteTransaction().createRelationshipType( relType2, "relationshiptype2" );
        long rel1 = ds.nextId( Relationship.class );
        xaCon.getWriteTransaction().relationshipCreate( rel1, relType1, node1, node2 );
        long rel2 = ds.nextId( Relationship.class );
        xaCon.getWriteTransaction().relationshipCreate( rel2, relType2, node2, node1 );

        PropertyData r1prop1 = xaCon.getWriteTransaction().relAddProperty(
                rel1, index( "prop1" ), "string1" );
        PropertyData r1prop2 = xaCon.getWriteTransaction().relAddProperty(
                rel1, index( "prop2" ), 1 );
        PropertyData r1prop3 = xaCon.getWriteTransaction().relAddProperty(
                rel1, index( "prop3" ), true );

        PropertyData r2prop1 = xaCon.getWriteTransaction().relAddProperty(
                rel2, index( "prop1" ), "string2" );
        PropertyData r2prop2 = xaCon.getWriteTransaction().relAddProperty(
                rel2, index( "prop2" ), 2 );
        PropertyData r2prop3 = xaCon.getWriteTransaction().relAddProperty(
                rel2, index( "prop3" ), false );
        commitTx();
        ds.stop();

        initializeStores();
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

        initializeStores();
        startTx();
        // validate and delete rels
        deleteRel1( rel1, r1prop1, r1prop2, r1prop3, node1, node2, relType1 );
        deleteRel2( rel2, r2prop1, r2prop2, r2prop3, node2, node1, relType2 );
        // validate and delete nodes
        deleteNode1( node1, n1prop1, n1prop2, n1prop3 );
        deleteNode2( node2, n2prop1, n2prop2, n2prop3 );
        commitTx();
        ds.stop();

        initializeStores();
        startTx();
        assertNull( xaCon.getWriteTransaction().nodeLoadLight( node1 ) );
        assertNull( xaCon.getWriteTransaction().nodeLoadLight( node2 ) );
        testGetRels( new long[]{rel1, rel2} );
        // testGetProps( neoStore, new int[] {
        // n1prop1, n1prop2, n1prop3, n2prop1, n2prop2, n2prop3,
        // r1prop1, r1prop2, r1prop3, r2prop1, r2prop2, r2prop3
        // } );
        long nodeIds[] = new long[10];
        for ( int i = 0; i < 3; i++ )
        {
            nodeIds[i] = ds.nextId( Node.class );
            xaCon.getWriteTransaction().nodeCreate( nodeIds[i] );
            xaCon.getWriteTransaction().nodeAddProperty( nodeIds[i],
                    index( "nisse" ), new Integer( 10 - i ) );
        }
        for ( int i = 0; i < 2; i++ )
        {
            long id = ds.nextId( Relationship.class );
            xaCon.getWriteTransaction().relationshipCreate( id, relType1, nodeIds[i], nodeIds[i + 1] );
            xaCon.getWriteTransaction().relDelete( id );
        }
        for ( int i = 0; i < 3; i++ )
        {
            AtomicLong pos = getPosition( xaCon, nodeIds[i] );
            for ( RelationshipRecord rel : getMore( xaCon, nodeIds[i], pos ) )
            {
                xaCon.getWriteTransaction().relDelete( rel.getId() );
            }
            xaCon.getWriteTransaction().nodeDelete( nodeIds[i] );
        }
        commitTx();
        ds.stop();
    }

    private AtomicLong getPosition( NeoStoreXaConnection xaCon, long node )
    {
        return new AtomicLong( xaCon.getWriteTransaction().getRelationshipChainPosition( node ) );
    }

    @SuppressWarnings("unchecked")
    private Iterable<RelationshipRecord> getMore( NeoStoreXaConnection xaCon, long node, AtomicLong pos )
    {
        Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, Long> rels =
                xaCon.getWriteTransaction().getMoreRelationships( node, pos.get() );
        pos.set( rels.other() );
        List<Iterable<RelationshipRecord>> list = new ArrayList<Iterable<RelationshipRecord>>();
        for ( Map.Entry<DirectionWrapper, Iterable<RelationshipRecord>> entry : rels.first().entrySet() )
        {
            list.add( entry.getValue() );
        }
        return new CombiningIterable<RelationshipRecord>( list );
    }

    private void validateNodeRel1( long node, PropertyData prop1,
                                   PropertyData prop2, PropertyData prop3, long rel1, long rel2,
                                   int relType1, int relType2 ) throws IOException
    {
        NodeRecord nodeRecord = xaCon.getWriteTransaction().nodeLoadLight( node );
        assertTrue( nodeRecord != null );
        ArrayMap<Integer, PropertyData> props = xaCon.getWriteTransaction().nodeLoadProperties( node, false );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).getId();
            PropertyRecord record = pStore.getRecord( id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).getIndex() );
            PropertyData data = block.newPropertyData( record );
            if ( data.getIndex() == prop1.getIndex() )
            {
                assertEquals( "prop1", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( "string1", data.getValue() );
                xaCon.getWriteTransaction().nodeChangeProperty( node, prop1, "-string1" );
            }
            else if ( data.getIndex() == prop2.getIndex() )
            {
                assertEquals( "prop2", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( new Integer( 1 ), data.getValue() );
                xaCon.getWriteTransaction().nodeChangeProperty( node, prop2, new Integer( -1 ) );
            }
            else if ( data.getIndex() == prop3.getIndex() )
            {
                assertEquals( "prop3", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( new Boolean( true ), data.getValue() );
                xaCon.getWriteTransaction().nodeChangeProperty( node, prop3, new Boolean( false ) );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );
        count = 0;
        AtomicLong pos = getPosition( xaCon, node );
        while ( true )
        {
            Iterable<RelationshipRecord> relData = getMore( xaCon, node, pos );
            if ( !relData.iterator().hasNext() )
            {
                break;
            }
            for ( RelationshipRecord rel : relData )
            {
                if ( rel.getId() == rel1 )
                {
                    assertEquals( node, rel.getFirstNode() );
                    assertEquals( relType1, rel.getType() );
                }
                else if ( rel.getId() == rel2 )
                {
                    assertEquals( node, rel.getSecondNode() );
                    assertEquals( relType2, rel.getType() );
                }
                else
                {
                    throw new IOException();
                }
                count++;
            }
        }
        assertEquals( 2, count );
    }

    private void validateNodeRel2( long node, PropertyData prop1,
                                   PropertyData prop2, PropertyData prop3,
                                   long rel1, long rel2, int relType1, int relType2 ) throws IOException
    {
        NodeRecord nodeRecord = xaCon.getWriteTransaction().nodeLoadLight( node );
        assertTrue( nodeRecord != null );
        ArrayMap<Integer, PropertyData> props = xaCon.getWriteTransaction().nodeLoadProperties( node, false );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).getId();
            PropertyRecord record = pStore.getRecord( id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).getIndex() );
            PropertyData data = block.newPropertyData( record );
            if ( data.getIndex() == prop1.getIndex() )
            {
                assertEquals( "prop1", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( "string2", data.getValue() );
                xaCon.getWriteTransaction().nodeChangeProperty( node, prop1, "-string2" );
            }
            else if ( data.getIndex() == prop2.getIndex() )
            {
                assertEquals( "prop2", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( new Integer( 2 ), data.getValue() );
                xaCon.getWriteTransaction().nodeChangeProperty( node, prop2, new Integer( -2 ) );
            }
            else if ( data.getIndex() == prop3.getIndex() )
            {
                assertEquals( "prop3", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( new Boolean( false ), data.getValue() );
                xaCon.getWriteTransaction().nodeChangeProperty( node, prop3, new Boolean( true ) );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );
        count = 0;

        AtomicLong pos = getPosition( xaCon, node );
        while ( true )
        {
            Iterable<RelationshipRecord> relData = getMore( xaCon, node, pos );
            if ( !relData.iterator().hasNext() )
            {
                break;
            }
            for ( RelationshipRecord rel : relData )
            {
                if ( rel.getId() == rel1 )
                {
                    assertEquals( node, rel.getSecondNode() );
                    assertEquals( relType1, rel.getType() );
                }
                else if ( rel.getId() == rel2 )
                {
                    assertEquals( node, rel.getFirstNode() );
                    assertEquals( relType2, rel.getType() );
                }
                else
                {
                    throw new IOException();
                }
                count++;
            }
        }
        assertEquals( 2, count );
    }

    private void validateRel1( long rel, PropertyData prop1,
                               PropertyData prop2, PropertyData prop3,
                               long firstNode, long secondNode, int relType ) throws IOException
    {
        ArrayMap<Integer, PropertyData> props = xaCon.getWriteTransaction().relLoadProperties( rel,
                false );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).getId();
            PropertyRecord record = pStore.getRecord( id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).getIndex() );
            PropertyData data = block.newPropertyData( record );
            if ( data.getIndex() == prop1.getIndex() )
            {
                assertEquals( "prop1", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( "string1", data.getValue() );
                xaCon.getWriteTransaction().relChangeProperty( rel, prop1, "-string1" );
            }
            else if ( data.getIndex() == prop2.getIndex() )
            {
                assertEquals( "prop2", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( new Integer( 1 ), data.getValue() );
                xaCon.getWriteTransaction().relChangeProperty( rel, prop2, new Integer( -1 ) );
            }
            else if ( data.getIndex() == prop3.getIndex() )
            {
                assertEquals( "prop3", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( new Boolean( true ), data.getValue() );
                xaCon.getWriteTransaction().relChangeProperty( rel, prop3, new Boolean( false ) );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );
        RelationshipRecord relData = xaCon.getWriteTransaction().relLoadLight( rel );
        assertEquals( firstNode, relData.getFirstNode() );
        assertEquals( secondNode, relData.getSecondNode() );
        assertEquals( relType, relData.getType() );
    }

    private void validateRel2( long rel, PropertyData prop1,
                               PropertyData prop2, PropertyData prop3,
                               long firstNode, long secondNode, int relType ) throws IOException
    {
        ArrayMap<Integer, PropertyData> props = xaCon.getWriteTransaction().relLoadProperties( rel,
                false );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).getId();
            PropertyRecord record = pStore.getRecord( id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).getIndex() );
            PropertyData data = block.newPropertyData( record );
            if ( data.getIndex() == prop1.getIndex() )
            {
                assertEquals( "prop1", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( "string2", data.getValue() );
                xaCon.getWriteTransaction().relChangeProperty( rel, prop1, "-string2" );
            }
            else if ( data.getIndex() == prop2.getIndex() )
            {
                assertEquals( "prop2", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( new Integer( 2 ), data.getValue() );
                xaCon.getWriteTransaction().relChangeProperty( rel, prop2, new Integer( -2 ) );
            }
            else if ( data.getIndex() == prop3.getIndex() )
            {
                assertEquals( "prop3", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( new Boolean( false ), data.getValue() );
                xaCon.getWriteTransaction().relChangeProperty( rel, prop3, new Boolean( true ) );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );
        RelationshipRecord relData = xaCon.getWriteTransaction().relLoadLight( rel );
        assertEquals( firstNode, relData.getFirstNode() );
        assertEquals( secondNode, relData.getSecondNode() );
        assertEquals( relType, relData.getType() );
    }

    private void validateRelTypes( int relType1, int relType2 )
            throws IOException
    {
        NameData data = rtStore.getName( relType1 );
        assertEquals( relType1, data.getId() );
        assertEquals( "relationshiptype1", data.getName() );
        data = rtStore.getName( relType2 );
        assertEquals( relType2, data.getId() );
        assertEquals( "relationshiptype2", data.getName() );
        NameData allData[] = rtStore.getNames( Integer.MAX_VALUE );
        assertEquals( 2, allData.length );
        for ( int i = 0; i < 2; i++ )
        {
            if ( allData[i].getId() == relType1 )
            {
                assertEquals( relType1, allData[i].getId() );
                assertEquals( "relationshiptype1", allData[i].getName() );
            }
            else if ( allData[i].getId() == relType2 )
            {
                assertEquals( relType2, allData[i].getId() );
                assertEquals( "relationshiptype2", allData[i].getName() );
            }
            else
            {
                throw new IOException();
            }
        }
    }

    private void deleteRel1( long rel, PropertyData prop1, PropertyData prop2,
                             PropertyData prop3,
                             long firstNode, long secondNode, int relType ) throws IOException
    {
        ArrayMap<Integer, PropertyData> props = xaCon.getWriteTransaction().relLoadProperties( rel,
                false );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).getId();
            PropertyRecord record = pStore.getRecord( id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).getIndex() );
            PropertyData data = block.newPropertyData( record );
            if ( data.getIndex() == prop1.getIndex() )
            {
                assertEquals( "prop1", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( "-string1", data.getValue() );
            }
            else if ( data.getIndex() == prop2.getIndex() )
            {
                assertEquals( "prop2", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( new Integer( -1 ), data.getValue() );
            }
            else if ( data.getIndex() == prop3.getIndex() )
            {
                assertEquals( "prop3", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( new Boolean( false ), data.getValue() );
                xaCon.getWriteTransaction().relRemoveProperty( rel, prop3 );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );
        assertEquals( 3, xaCon.getWriteTransaction().relLoadProperties( rel, false ).size() );
        RelationshipRecord relData = xaCon.getWriteTransaction().relLoadLight( rel );
        assertEquals( firstNode, relData.getFirstNode() );
        assertEquals( secondNode, relData.getSecondNode() );
        assertEquals( relType, relData.getType() );
        xaCon.getWriteTransaction().relDelete( rel );
        AtomicLong firstPos = getPosition( xaCon, firstNode );
        Iterator<RelationshipRecord> first = getMore( xaCon, firstNode, firstPos ).iterator();
        first.next();
        AtomicLong secondPos = getPosition( xaCon, secondNode );
        Iterator<RelationshipRecord> second = getMore( xaCon, secondNode, secondPos ).iterator();
        second.next();
        assertTrue( first.hasNext() );
        assertTrue( second.hasNext() );
    }

    private void deleteRel2( long rel, PropertyData prop1, PropertyData prop2,
                             PropertyData prop3,
                             long firstNode, long secondNode, int relType ) throws IOException
    {
        ArrayMap<Integer, PropertyData> props = xaCon.getWriteTransaction().relLoadProperties( rel,
                false );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).getId();
            PropertyRecord record = pStore.getRecord( id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).getIndex() );
            PropertyData data = block.newPropertyData( record );
            if ( data.getIndex() == prop1.getIndex() )
            {
                assertEquals( "prop1", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( "-string2", data.getValue() );
            }
            else if ( data.getIndex() == prop2.getIndex() )
            {
                assertEquals( "prop2", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( new Integer( -2 ), data.getValue() );
            }
            else if ( data.getIndex() == prop3.getIndex() )
            {
                assertEquals( "prop3", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( new Boolean( true ), data.getValue() );
                xaCon.getWriteTransaction().relRemoveProperty( rel, prop3 );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );
        assertEquals( 3, xaCon.getWriteTransaction().relLoadProperties( rel, false ).size() );
        RelationshipRecord relData = xaCon.getWriteTransaction().relLoadLight( rel );
        assertEquals( firstNode, relData.getFirstNode() );
        assertEquals( secondNode, relData.getSecondNode() );
        assertEquals( relType, relData.getType() );
        xaCon.getWriteTransaction().relDelete( rel );
        AtomicLong firstPos = getPosition( xaCon, firstNode );
        Iterator<RelationshipRecord> first = getMore( xaCon, firstNode, firstPos ).iterator();
        AtomicLong secondPos = getPosition( xaCon, secondNode );
        Iterator<RelationshipRecord> second = getMore( xaCon, secondNode, secondPos ).iterator();
        assertTrue( first.hasNext() );
        assertTrue( second.hasNext() );
    }

    private void deleteNode1( long node, PropertyData prop1,
                              PropertyData prop2, PropertyData prop3 )
            throws IOException
    {
        ArrayMap<Integer, PropertyData> props = xaCon.getWriteTransaction().nodeLoadProperties( node, false );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).getId();
            PropertyRecord record = pStore.getRecord( id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).getIndex() );
            PropertyData data = block.newPropertyData( record );
            if ( data.getIndex() == prop1.getIndex() )
            {
                assertEquals( "prop1", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( "-string1", data.getValue() );
            }
            else if ( data.getIndex() == prop2.getIndex() )
            {
                assertEquals( "prop2", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( new Integer( -1 ), data.getValue() );
            }
            else if ( data.getIndex() == prop3.getIndex() )
            {
                assertEquals( "prop3", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( new Boolean( false ), data.getValue() );
                xaCon.getWriteTransaction().nodeRemoveProperty( node, prop3 );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );
        assertEquals( 3, xaCon.getWriteTransaction().nodeLoadProperties( node, false ).size() );
        AtomicLong pos = getPosition( xaCon, node );
        Iterator<RelationshipRecord> rels = getMore( xaCon, node, pos ).iterator();
        assertTrue( rels.hasNext() );
        xaCon.getWriteTransaction().nodeDelete( node );
    }

    private void deleteNode2( long node, PropertyData prop1,
                              PropertyData prop2, PropertyData prop3 )
            throws IOException
    {
        ArrayMap<Integer, PropertyData> props = xaCon.getWriteTransaction().nodeLoadProperties( node, false );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).getId();
            PropertyRecord record = pStore.getRecord( id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).getIndex() );
            PropertyData data = block.newPropertyData( record );
            if ( data.getIndex() == prop1.getIndex() )
            {
                assertEquals( "prop1", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( "-string2", data.getValue() );
            }
            else if ( data.getIndex() == prop2.getIndex() )
            {
                assertEquals( "prop2", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( new Integer( -2 ), data.getValue() );
            }
            else if ( data.getIndex() == prop3.getIndex() )
            {
                assertEquals( "prop3", MyPropertyIndex.getIndexFor(
                        keyId ).getKey() );
                assertEquals( new Boolean( true ), data.getValue() );
                xaCon.getWriteTransaction().nodeRemoveProperty( node, prop3 );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );
        assertEquals( 3, xaCon.getWriteTransaction().nodeLoadProperties( node, false ).size() );
        AtomicLong pos = getPosition( xaCon, node );
        Iterator<RelationshipRecord> rels = getMore( xaCon, node, pos ).iterator();
        assertTrue( rels.hasNext() );
        xaCon.getWriteTransaction().nodeDelete( node );
    }

    private void testGetRels( long relIds[] )
    {
        for ( long relId : relIds )
        {
            assertEquals( null, xaCon.getWriteTransaction().relLoadLight( relId ) );
        }
    }

    @Test
    public void testRels1() throws Exception
    {
        initializeStores();
        startTx();
        int relType1 = (int) ds.nextId( RelationshipType.class );
        xaCon.getWriteTransaction().createRelationshipType( relType1, "relationshiptype1" );
        long nodeIds[] = new long[3];
        for ( int i = 0; i < 3; i++ )
        {
            nodeIds[i] = ds.nextId( Node.class );
            xaCon.getWriteTransaction().nodeCreate( nodeIds[i] );
            xaCon.getWriteTransaction().nodeAddProperty( nodeIds[i],
                    index( "nisse" ), new Integer( 10 - i ) );
        }
        for ( int i = 0; i < 2; i++ )
        {
            xaCon.getWriteTransaction().relationshipCreate( ds.nextId( Relationship.class ),
                    relType1, nodeIds[i], nodeIds[i + 1] );
        }
        commitTx();
        startTx();
        for ( int i = 0; i < 3; i += 2 )
        {
            AtomicLong pos = getPosition( xaCon, nodeIds[i] );
            for ( RelationshipRecord rel : getMore( xaCon, nodeIds[i], pos ) )
            {
                xaCon.getWriteTransaction().relDelete( rel.getId() );
            }
            xaCon.getWriteTransaction().nodeDelete( nodeIds[i] );
        }
        commitTx();
        ds.stop();
    }

    @Test
    @Ignore
    public void testRels2() throws Exception
    {
        initializeStores();
        startTx();
        int relType1 = (int) ds.nextId( RelationshipType.class );
        xaCon.getWriteTransaction().createRelationshipType( relType1, "relationshiptype1" );
        long nodeIds[] = new long[3];
        for ( int i = 0; i < 3; i++ )
        {
            nodeIds[i] = ds.nextId( Node.class );
            xaCon.getWriteTransaction().nodeCreate( nodeIds[i] );
            xaCon.getWriteTransaction().nodeAddProperty( nodeIds[i],
                    index( "nisse" ), new Integer( 10 - i ) );
        }
        for ( int i = 0; i < 2; i++ )
        {
            xaCon.getWriteTransaction().relationshipCreate( ds.nextId( Relationship.class ),
                    relType1, nodeIds[i], nodeIds[i + 1] );
        }
        xaCon.getWriteTransaction().relationshipCreate( ds.nextId( Relationship.class ),
                relType1, nodeIds[0], nodeIds[2] );
        commitTx();
        startTx();
        for ( int i = 0; i < 3; i++ )
        {
            AtomicLong pos = getPosition( xaCon, nodeIds[i] );
            for ( RelationshipRecord rel : getMore( xaCon, nodeIds[i], pos ) )
            {
                xaCon.getWriteTransaction().relDelete( rel.getId() );
            }
            xaCon.getWriteTransaction().nodeDelete( nodeIds[i] );
        }
        commitTx();
        ds.stop();
    }

    @Test
    public void testRels3() throws Exception
    {
        // test linked list stuff during relationship delete
        initializeStores();
        startTx();
        int relType1 = (int) ds.nextId( RelationshipType.class );
        xaCon.getWriteTransaction().createRelationshipType( relType1, "relationshiptype1" );
        long nodeIds[] = new long[8];
        for ( int i = 0; i < nodeIds.length; i++ )
        {
            nodeIds[i] = ds.nextId( Node.class );
            xaCon.getWriteTransaction().nodeCreate( nodeIds[i] );
        }
        for ( int i = 0; i < nodeIds.length / 2; i++ )
        {
            xaCon.getWriteTransaction().relationshipCreate( ds.nextId( Relationship.class ),
                    relType1, nodeIds[i], nodeIds[i * 2] );
        }
        long rel5 = ds.nextId( Relationship.class );
        xaCon.getWriteTransaction().relationshipCreate( rel5, relType1, nodeIds[0], nodeIds[5] );
        long rel2 = ds.nextId( Relationship.class );
        xaCon.getWriteTransaction().relationshipCreate( rel2, relType1, nodeIds[1], nodeIds[2] );
        long rel3 = ds.nextId( Relationship.class );
        xaCon.getWriteTransaction().relationshipCreate( rel3, relType1, nodeIds[1], nodeIds[3] );
        long rel6 = ds.nextId( Relationship.class );
        xaCon.getWriteTransaction().relationshipCreate( rel6, relType1, nodeIds[1], nodeIds[6] );
        long rel1 = ds.nextId( Relationship.class );
        xaCon.getWriteTransaction().relationshipCreate( rel1, relType1, nodeIds[0], nodeIds[1] );
        long rel4 = ds.nextId( Relationship.class );
        xaCon.getWriteTransaction().relationshipCreate( rel4, relType1, nodeIds[0], nodeIds[4] );
        long rel7 = ds.nextId( Relationship.class );
        xaCon.getWriteTransaction().relationshipCreate( rel7, relType1, nodeIds[0], nodeIds[7] );
        commitTx();
        startTx();
        xaCon.getWriteTransaction().relDelete( rel7 );
        xaCon.getWriteTransaction().relDelete( rel4 );
        xaCon.getWriteTransaction().relDelete( rel1 );
        xaCon.getWriteTransaction().relDelete( rel6 );
        xaCon.getWriteTransaction().relDelete( rel3 );
        xaCon.getWriteTransaction().relDelete( rel2 );
        xaCon.getWriteTransaction().relDelete( rel5 );
        // xaCon.getWriteTransaction().nodeDelete( nodeIds[2] );
        // xaCon.getWriteTransaction().nodeDelete( nodeIds[3] );
        // xaCon.getWriteTransaction().nodeDelete( nodeIds[1] );
        // xaCon.getWriteTransaction().nodeDelete( nodeIds[4] );
        // xaCon.getWriteTransaction().nodeDelete( nodeIds[0] );
        commitTx();
        ds.stop();
    }

    @Test
    public void testProps1() throws Exception
    {
        initializeStores();
        startTx();
        long nodeId = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( nodeId );
        long propertyId = pStore.nextId();
        PropertyData prop = xaCon.getWriteTransaction().nodeAddProperty(
                nodeId, index( "nisse" ),
                new Integer( 10 ) );
        commitTx();
        ds.stop();
        initializeStores();
        startTx();
        xaCon.getWriteTransaction().nodeChangeProperty( nodeId, prop,
                new Integer( 5 ) );
        xaCon.getWriteTransaction().nodeRemoveProperty( nodeId, prop );
        xaCon.getWriteTransaction().nodeDelete( nodeId );
        commitTx();
        ds.stop();
    }

    @Test
    public void testSetBlockSize() throws Exception
    {
        tearDownNeoStore();

        Config config = new Config( MapUtil.stringMap( "string_block_size", "62", "array_block_size", "302" ),
                GraphDatabaseSettings.class );
        StoreFactory sf = new StoreFactory( config, new DefaultIdGeneratorFactory(), new DefaultWindowPoolFactory(),
                fileSystem, StringLogger.DEV_NULL, null );
        sf.createNeoStore( file( "neo" ) ).close();

        initializeStores();
        assertEquals( 62 + AbstractDynamicStore.BLOCK_HEADER_SIZE,
                pStore.getStringBlockSize() );
        assertEquals( 302 + AbstractDynamicStore.BLOCK_HEADER_SIZE,
                pStore.getArrayBlockSize() );
        ds.stop();
    }

    @Test
    public void setVersion() throws Exception
    {
        String storeDir = "target/test-data/set-version";
        new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( storeDir ).shutdown();
        assertEquals( 1, NeoStore.setVersion( fileSystem, new File( storeDir ), 10 ) );
        assertEquals( 10, NeoStore.setVersion( fileSystem, new File( storeDir ), 12 ) );

        StoreFactory sf = new StoreFactory( new Config( new HashMap<String, String>(), GraphDatabaseSettings.class ),
                new DefaultIdGeneratorFactory(), new DefaultWindowPoolFactory(), fileSystem, StringLogger.DEV_NULL, null );

        NeoStore neoStore = sf.newNeoStore( new File( storeDir, NeoStore.DEFAULT_NAME ) );
        assertEquals( 12, neoStore.getVersion() );
        neoStore.close();
    }
}
