/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Functions;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Provider;
import org.neo4j.helpers.collection.CombiningIterable;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.standard.StandardPageCache;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.TransactionEventHandlers;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.KernelSchemaStateStore;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.scan.InMemoryLabelScanStore;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.cache.NoCacheProvider;
import org.neo4j.kernel.impl.core.DefaultCaches;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipLoadingPosition;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.StartupStatisticsProvider;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.nioneo.xa.PropertyLoader;
import org.neo4j.kernel.impl.nioneo.xa.RelationshipChainLoader;
import org.neo4j.kernel.impl.nioneo.xa.TransactionRecordState;
import org.neo4j.kernel.impl.nioneo.xa.TransactionRecordState.PropertyReceiver;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.transaction.xaframework.DefaultTxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntry;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.nioneo.store.StoreFactory.configForStoreDir;

public class TestNeoStore
{
    private PropertyStore pStore;
    private RelationshipTypeTokenStore rtStore;
    private NeoStoreXaDataSource ds;
    private TargetDirectory targetDirectory;
    private File path;

    @ClassRule
    public static PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    @Rule public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );
    private PageCache pageCache;

    private File file( String name )
    {
        return new File( path, name);
    }

    @Before
    public void setUpNeoStore() throws Exception
    {
        targetDirectory = TargetDirectory.forTest( fs.get(), getClass() );
        path = targetDirectory.cleanDirectory( "dir" );
        Config config = StoreFactory.configForStoreDir(
                new Config( new HashMap<String, String>(), GraphDatabaseSettings.class ), path );
        Monitors monitors = new Monitors();
        pageCache = pageCacheRule.getPageCache( fs.get(), config );
        StoreFactory sf = new StoreFactory(
                config,
                new DefaultIdGeneratorFactory(),
                pageCache,
                fs.get(),
                StringLogger.DEV_NULL,
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

    private void initializeStores( Map<String, String> additionalConfig ) throws IOException
    {
        final Config config = new Config( MapUtil.stringMap( additionalConfig,
                InternalAbstractGraphDatabase.Configuration.store_dir.name(), path.getPath(),
                InternalAbstractGraphDatabase.Configuration.neo_store.name(), "neo" ),
                GraphDatabaseSettings.class );
        EphemeralFileSystemAbstraction fs = this.fs.get();
        Monitors monitors = new Monitors();
        StoreFactory sf = new StoreFactory(
                config,
                new DefaultIdGeneratorFactory(),
                pageCache,
                fs,
                StringLogger.DEV_NULL,
                monitors );
        KernelHealth kernelHealth = mock( KernelHealth.class );

        Locks locks = mock( Locks.class );
        Locks.Client lockClient = mock(Locks.Client.class);
        TransactionHeaderInformation headerInformation = new TransactionHeaderInformation( -1, 1, new byte[0] );
        when(locks.newClient()).thenReturn( lockClient );
        DefaultCaches caches = new DefaultCaches( StringLogger.DEV_NULL, new Monitors() );
        caches.configure( new NoCacheProvider(), config );
        NodeManager nodeManager = new NodeManager( null, null, new ThreadToStatementContextBridge() );
        ds = new NeoStoreXaDataSource(config, sf, StringLogger.DEV_NULL,
                null, DevNullLoggingService.DEV_NULL,
                new KernelSchemaStateStore(),
                mock(TokenNameLookup.class),
                dependencyResolverForNoIndexProvider(),
                mock( PropertyKeyTokenHolder.class ), mock(LabelTokenHolder.class),
                mock( RelationshipTypeTokenHolder.class), locks,
                mock( SchemaWriteGuard.class), mock( TransactionEventHandlers.class), IndexingService.NO_MONITOR, fs,
                new Function<NeoStore, Function<List<LogEntry>, List<LogEntry>>>()
                {
                    @Override
                    public Function<List<LogEntry>, List<LogEntry>> apply( NeoStore neoStore )
                    {
                        return Functions.<List<LogEntry>>identity();
                    }
                }, mock( StoreUpgrader.class ), mock( TransactionMonitor.class ), kernelHealth,
                new DefaultTxIdGenerator( new Provider<TransactionIdStore>()
                {
                    @Override
                    public TransactionIdStore instance()
                    {
                        return ds.evaluate();
                    }
                } ), TransactionHeaderInformationFactory.DEFAULT, new StartupStatisticsProvider(), caches, nodeManager,
                null, null, InternalAbstractGraphDatabase.defaultCommitProcessFactory );
        ds.init();
        ds.start();

        NeoStore neoStore = ds.evaluate();
        pStore = neoStore.getPropertyStore();
        rtStore = neoStore.getRelationshipTypeTokenStore();
        storeLayer = ds.getStoreLayer();
        relationshipLoader = new RelationshipChainLoader( neoStore );
        propertyLoader = new PropertyLoader( neoStore );
    }

    private DependencyResolver dependencyResolverForNoIndexProvider()
    {
        return new DependencyResolver.Adapter()
        {
            private final LabelScanStoreProvider labelScanStoreProvider =
                    new LabelScanStoreProvider( new InMemoryLabelScanStore(), 10 );

            @Override
            public <T> T resolveDependency( Class<T> type, SelectionStrategy selector ) throws IllegalArgumentException
            {
                if ( SchemaIndexProvider.class.isAssignableFrom( type ) )
                {
                    return type.cast( SchemaIndexProvider.NO_INDEX_PROVIDER );
                }
                else if ( LabelScanStoreProvider.class.isAssignableFrom( type ) )
                {
                    return type.cast( labelScanStoreProvider );
                }
                throw new IllegalArgumentException( type.toString() );
            }
        };
    }

    private byte txCount = (byte) 0;
    private KernelTransaction tx;
    private TransactionRecordState transaction;
    private StoreReadLayer storeLayer;
    private RelationshipChainLoader relationshipLoader;
    private PropertyLoader propertyLoader;

    private void startTx() throws TransactionFailureException
    {
        txCount++;
        tx = ds.getKernel().newTransaction();
        transaction = tx.getTransactionRecordState();
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
            fs.get().deleteFile( file( file ) );
            fs.get().deleteFile( file( file + ".id" ) );
        }

        File file = new File( "." );
        for ( File nioFile : fs.get().listFiles( file ) )
        {
            if ( nioFile.getName().startsWith( "nioneo_logical.log" ) )
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
        NeoStore neoStore = ds.evaluate();
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
        initializeStores( stringMap() );
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
        transaction.createRelationshipTypeToken( relType1, typeName1 );
        int relType2 = (int) nextId( RelationshipType.class );
        String typeName2 = "relationshiptype2";
        transaction.createRelationshipTypeToken( relType2, typeName2 );
        long rel1 = nextId( Relationship.class );
        transaction.relationshipCreate( rel1, relType1, node1, node2 );
        long rel2 = nextId( Relationship.class );
        transaction.relationshipCreate( rel2, relType2, node2, node1 );

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

        initializeStores( stringMap() );
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

        initializeStores( stringMap() );
        startTx();
        // validate and delete rels
        deleteRel1( rel1, r1prop1, r1prop2, r1prop3, node1, node2, relType1 );
        deleteRel2( rel2, r2prop1, r2prop2, r2prop3, node2, node1, relType2 );
        // validate and delete nodes
        deleteNode1( node1, n1prop1, n1prop2, n1prop3 );
        deleteNode2( node2, n2prop1, n2prop2, n2prop3 );
        commitTx();
        ds.stop();

        initializeStores( stringMap() );
        startTx();
        assertFalse( storeLayer.nodeExists( node1 ) );
        assertFalse( storeLayer.nodeExists( node2 ) );
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
            transaction.relationshipCreate( id, relType1, nodeIds[i], nodeIds[i + 1] );
            transaction.relDelete( id );
        }
        for ( int i = 0; i < 3; i++ )
        {
            MutableRelationshipLoadingPosition pos = getPosition( nodeIds[i] );
            for ( RelationshipRecord rel : getMore( nodeIds[i], pos ) )
            {
                transaction.relDelete( rel.getId() );
            }
            transaction.nodeDelete( nodeIds[i] );
        }
        commitTx();
        ds.stop();
    }

    private MutableRelationshipLoadingPosition getPosition( long node )
    {
        return new MutableRelationshipLoadingPosition(
                relationshipLoader.getRelationshipChainPosition( node ) );
    }

    private static class MutableRelationshipLoadingPosition implements RelationshipLoadingPosition
    {
        private RelationshipLoadingPosition actual;

        MutableRelationshipLoadingPosition( RelationshipLoadingPosition actual )
        {
            this.actual = actual;
        }

        void setActual( RelationshipLoadingPosition actual )
        {
            this.actual = actual;
        }

        @Override
        public void updateFirst( long first )
        {
            actual.updateFirst( first );
        }

        @Override
        public long position( DirectionWrapper direction, int[] types )
        {
            return actual.position( direction, types );
        }

        @Override
        public long nextPosition( long position, DirectionWrapper direction, int[] types )
        {
            return actual.nextPosition( position, direction, types );
        }

        @Override
        public boolean hasMore( DirectionWrapper direction, int[] types )
        {
            return actual.hasMore( direction, types );
        }

        @Override
        public void compareAndAdvance( long relIdDeleted, long nextRelId )
        {
            actual.compareAndAdvance( relIdDeleted, nextRelId );
        }

        @Override
        public RelationshipLoadingPosition clone()
        {
            return actual.clone();
        }
    }

    private Iterable<RelationshipRecord> getMore( long node, MutableRelationshipLoadingPosition position )
    {
        Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, RelationshipLoadingPosition> rels =
                relationshipLoader.getMoreRelationships( node, position, DirectionWrapper.BOTH,
                        new int[0] );
        List<Iterable<RelationshipRecord>> list = new ArrayList<>();
        for ( Map.Entry<DirectionWrapper, Iterable<RelationshipRecord>> entry : rels.first().entrySet() )
        {
            list.add( entry.getValue() );
        }
        position.setActual( rels.other() );
        return new CombiningIterable<>( list );
    }

    private void validateNodeRel1( long node, DefinedProperty prop1,
            DefinedProperty prop2, DefinedProperty prop3, long rel1, long rel2,
            int relType1, int relType2 ) throws IOException
    {
        assertTrue( storeLayer.nodeExists( node ) );
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
        MutableRelationshipLoadingPosition pos = getPosition( node );
        while ( true )
        {
            Iterable<RelationshipRecord> relData = getMore( node, pos );
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

    private void validateNodeRel2( long node, DefinedProperty prop1,
            DefinedProperty prop2, DefinedProperty prop3,
            long rel1, long rel2, int relType1, int relType2 ) throws IOException
    {
        assertTrue( storeLayer.nodeExists( node ) );
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

        MutableRelationshipLoadingPosition pos = getPosition( node );
        while ( true )
        {
            Iterable<RelationshipRecord> relData = getMore( node, pos );
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
                public void visit( long relId, long startNode, long endNode, int type )
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
        Token allData[] = rtStore.getTokens( Integer.MAX_VALUE );
        assertEquals( 2, allData.length );
        for ( int i = 0; i < 2; i++ )
        {
            if ( allData[i].id() == relType1 )
            {
                assertEquals( relType1, allData[i].id() );
                assertEquals( "relationshiptype1", allData[i].name() );
            }
            else if ( allData[i].id() == relType2 )
            {
                assertEquals( relType2, allData[i].id() );
                assertEquals( "relationshiptype2", allData[i].name() );
            }
            else
            {
                throw new IOException();
            }
        }
    }

    private void deleteRel1( long rel, DefinedProperty prop1, DefinedProperty prop2,
            DefinedProperty prop3, long firstNode, long secondNode, int relType )
                    throws IOException
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
        MutableRelationshipLoadingPosition firstPos = getPosition( firstNode );
        Iterator<RelationshipRecord> first = getMore( firstNode, firstPos ).iterator();
        first.next();
        MutableRelationshipLoadingPosition secondPos = getPosition( secondNode );
        Iterator<RelationshipRecord> second = getMore( secondNode, secondPos ).iterator();
        second.next();
        assertTrue( first.hasNext() );
        assertTrue( second.hasNext() );
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
            DefinedProperty prop3, long firstNode, long secondNode, int relType ) throws IOException
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
        MutableRelationshipLoadingPosition firstPos = getPosition( firstNode );
        Iterator<RelationshipRecord> first = getMore( firstNode, firstPos ).iterator();
        MutableRelationshipLoadingPosition secondPos = getPosition( secondNode );
        Iterator<RelationshipRecord> second = getMore( secondNode, secondPos ).iterator();
        assertTrue( first.hasNext() );
        assertTrue( second.hasNext() );
    }

    private void deleteNode1( long node, DefinedProperty prop1,
            DefinedProperty prop2, DefinedProperty prop3 )
            throws IOException
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
        MutableRelationshipLoadingPosition pos = getPosition( node );
        Iterator<RelationshipRecord> rels = getMore( node, pos ).iterator();
        assertTrue( rels.hasNext() );
        transaction.nodeDelete( node );
    }

    private void deleteNode2( long node, DefinedProperty prop1,
            DefinedProperty prop2, DefinedProperty prop3 )
            throws IOException
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
        MutableRelationshipLoadingPosition pos = getPosition( node );
        Iterator<RelationshipRecord> rels = getMore( node, pos ).iterator();
        assertTrue( rels.hasNext() );
        transaction.nodeDelete( node );
    }

    private void testGetRels( long relIds[] )
    {
        for ( long relId : relIds )
        {
            assertFalse( storeLayer.relationshipExists( relId ) );
        }
    }

    @Test
    public void testRels1() throws Exception
    {
        initializeStores( stringMap() );
        startTx();
        int relType1 = (int) nextId( RelationshipType.class );
        String typeName = "relationshiptype1";
        transaction.createRelationshipTypeToken( relType1, typeName );
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
            transaction.relationshipCreate( nextId( Relationship.class ),
                    relType1, nodeIds[i], nodeIds[i + 1] );
        }
        commitTx();
        startTx();
        for ( int i = 0; i < 3; i += 2 )
        {
            MutableRelationshipLoadingPosition pos = getPosition( nodeIds[i] );
            for ( RelationshipRecord rel : getMore( nodeIds[i], pos ) )
            {
                transaction.relDelete( rel.getId() );
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
        initializeStores( stringMap() );
        startTx();
        int relType1 = (int) nextId( RelationshipType.class );
        String typeName = "relationshiptype1";
        transaction.createRelationshipTypeToken( relType1, typeName );
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
            transaction.relationshipCreate( nextId( Relationship.class ),
                    relType1, nodeIds[i], nodeIds[i + 1] );
        }
        transaction.relationshipCreate( nextId( Relationship.class ),
                relType1, nodeIds[0], nodeIds[2] );
        commitTx();
        startTx();
        for ( int i = 0; i < 3; i++ )
        {
            MutableRelationshipLoadingPosition pos = getPosition( nodeIds[i] );
            for ( RelationshipRecord rel : getMore( nodeIds[i], pos ) )
            {
                transaction.relDelete( rel.getId() );
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
        initializeStores( stringMap() );
        startTx();
        int relType1 = (int) nextId( RelationshipType.class );
        transaction.createRelationshipTypeToken( relType1, "relationshiptype1" );
        long nodeIds[] = new long[8];
        for ( int i = 0; i < nodeIds.length; i++ )
        {
            nodeIds[i] = nextId( Node.class );
            transaction.nodeCreate( nodeIds[i] );
        }
        for ( int i = 0; i < nodeIds.length / 2; i++ )
        {
            transaction.relationshipCreate( nextId( Relationship.class ),
                    relType1, nodeIds[i], nodeIds[i * 2] );
        }
        long rel5 = nextId( Relationship.class );
        transaction.relationshipCreate( rel5, relType1, nodeIds[0], nodeIds[5] );
        long rel2 = nextId( Relationship.class );
        transaction.relationshipCreate( rel2, relType1, nodeIds[1], nodeIds[2] );
        long rel3 = nextId( Relationship.class );
        transaction.relationshipCreate( rel3, relType1, nodeIds[1], nodeIds[3] );
        long rel6 = nextId( Relationship.class );
        transaction.relationshipCreate( rel6, relType1, nodeIds[1], nodeIds[6] );
        long rel1 = nextId( Relationship.class );
        transaction.relationshipCreate( rel1, relType1, nodeIds[0], nodeIds[1] );
        long rel4 = nextId( Relationship.class );
        transaction.relationshipCreate( rel4, relType1, nodeIds[0], nodeIds[4] );
        long rel7 = nextId( Relationship.class );
        transaction.relationshipCreate( rel7, relType1, nodeIds[0], nodeIds[7] );
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
        initializeStores( stringMap() );
        startTx();
        long nodeId = nextId( Node.class );
        transaction.nodeCreate( nodeId );
        pStore.nextId();
        DefinedProperty prop = transaction.nodeAddProperty(
                nodeId, index( "nisse" ),
                new Integer( 10 ) );
        commitTx();
        ds.stop();
        initializeStores( stringMap() );
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
        targetDirectory.cleanup();

        initializeStores( stringMap( "string_block_size", "62", "array_block_size", "302" ) );
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
        File storeDir = new File("target/test-data/set-version");
        new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( storeDir.getAbsolutePath() ).shutdown();
        assertEquals( 1, NeoStore.setVersion( fileSystem, new File( storeDir, NeoStore.DEFAULT_NAME ).getAbsoluteFile(), 10 ) );
        assertEquals( 10, NeoStore.setVersion( fileSystem, new File( storeDir, NeoStore.DEFAULT_NAME ).getAbsoluteFile(), 12 ) );

        Monitors monitors = new Monitors();
        Config config = new Config( new HashMap<String, String>(), GraphDatabaseSettings.class );
        StoreFactory sf = new StoreFactory(
                configForStoreDir( config, storeDir ),
                new DefaultIdGeneratorFactory(),
                pageCache,
                fileSystem,
                StringLogger.DEV_NULL,
                monitors );

        NeoStore neoStore = sf.newNeoStore( false );
        assertEquals( 12, neoStore.getCurrentLogVersion() );
        neoStore.close();
    }

    @Test
    public void testSetLatestConstraintTx() throws Exception
    {
        // given
        new GraphDatabaseFactory().newEmbeddedDatabase( testDir.absolutePath() ).shutdown();
        Monitors monitors = new Monitors();
        Config config = new Config( new HashMap<String, String>(), GraphDatabaseSettings.class );
        DefaultFileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
        StoreFactory sf = new StoreFactory(
                configForStoreDir( config, testDir.directory() ),
                new DefaultIdGeneratorFactory(),
                new StandardPageCache( fileSystemAbstraction, 1014, 4096 ),
                fileSystemAbstraction,
                StringLogger.DEV_NULL,
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
}
