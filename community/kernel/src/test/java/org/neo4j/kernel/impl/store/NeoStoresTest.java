/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.properties.PropertyKeyValue;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.DynamicRecordFormat;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState.PropertyReceiver;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.Token;
import org.neo4j.string.UTF8;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.ThreadTestUtils;
import org.neo4j.test.rule.ConfigurablePageCacheRule;
import org.neo4j.test.rule.NeoStoreDataSourceRule;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.counts_store_rotation_timeout;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.AssertOpen.ALWAYS_OPEN;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK;
import static org.neo4j.kernel.impl.store.RecordStore.getRecord;
import static org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat.FIELD_NOT_PRESENT;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.storageengine.api.Direction.BOTH;

public class NeoStoresTest
{

    private static final NullLogProvider LOG_PROVIDER = NullLogProvider.getInstance();
    private final PageCacheRule pageCacheRule = new ConfigurablePageCacheRule();
    private final ExpectedException exception = ExpectedException.none();
    private final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final TestDirectory dir = TestDirectory.testDirectory( fs.get() );
    private final NeoStoreDataSourceRule dsRule = new NeoStoreDataSourceRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( exception ).around( pageCacheRule )
            .around( fs ).around( dir ).around( dsRule );

    private PageCache pageCache;
    private File storeDir;
    private PropertyStore pStore;
    private RelationshipTypeTokenStore rtStore;
    private NeoStoreDataSource ds;
    private KernelTransaction tx;
    private TransactionState transaction;
    private StoreReadLayer storeLayer;
    private PropertyLoader propertyLoader;

    @Before
    public void setUpNeoStores()
    {
        storeDir = dir.graphDbDir();
        Config config = Config.defaults();
        pageCache = pageCacheRule.getPageCache( fs.get() );
        StoreFactory sf = new StoreFactory( storeDir, config, new DefaultIdGeneratorFactory( fs.get() ), pageCache,
                fs.get(), NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );
        sf.openAllNeoStores( true ).close();
    }

    @Test
    public void impossibleToGetStoreFromClosedNeoStoresContainer()
    {
        Config config = Config.defaults();
        StoreFactory sf = new StoreFactory( storeDir, config, new DefaultIdGeneratorFactory( fs.get() ), pageCache,
                fs.get(), NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );
        NeoStores neoStores = sf.openAllNeoStores( true );

        assertNotNull( neoStores.getMetaDataStore() );

        neoStores.close();

        exception.expect( IllegalStateException.class );
        exception.expectMessage( "Specified store was already closed.");
        neoStores.getMetaDataStore();
    }

    @Test
    public void notAllowCreateDynamicStoreWithNegativeBlockSize()
    {
        Config config = Config.defaults();
        StoreFactory sf = new StoreFactory( storeDir, config, new DefaultIdGeneratorFactory( fs.get() ), pageCache,
                fs.get(), NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );

        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Block size of dynamic array store should be positive integer." );

        try ( NeoStores neoStores = sf.openNeoStores( true ) )
        {
            neoStores.createDynamicArrayStore( "someStore", IdType.ARRAY_BLOCK, -2 );
        }
    }

    @Test
    public void impossibleToGetNotRequestedStore()
    {
        Config config = Config.defaults();
        StoreFactory sf = new StoreFactory( storeDir, config, new DefaultIdGeneratorFactory( fs.get() ), pageCache,
                fs.get(), NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );

        exception.expect( IllegalStateException.class );
        exception.expectMessage(
                "Specified store was not initialized. Please specify " + StoreType.META_DATA.name() +
                " as one of the stores types that should be open to be able to use it." );
        try ( NeoStores neoStores = sf.openNeoStores( true, StoreType.NODE_LABEL ) )
        {
            neoStores.getMetaDataStore();
        }
    }

    @Test
    public void testCreateStore() throws Exception
    {
        initializeStores( storeDir, stringMap() );
        startTx();
        // setup test population
        long node1 = nextId( Node.class );
        transaction.nodeDoCreate( node1 );
        long node2 = nextId( Node.class );
        transaction.nodeDoCreate( node2 );
        StorageProperty n1prop1 = nodeAddProperty( node1, index( "prop1" ), "string1" );
        StorageProperty n1prop2 = nodeAddProperty( node1, index( "prop2" ), 1 );
        StorageProperty n1prop3 = nodeAddProperty( node1, index( "prop3" ), true );

        StorageProperty n2prop1 = nodeAddProperty( node2, index( "prop1" ), "string2" );
        StorageProperty n2prop2 = nodeAddProperty( node2, index( "prop2" ), 2 );
        StorageProperty n2prop3 = nodeAddProperty( node2, index( "prop3" ), false );

        int relType1 = (int) nextId( RelationshipType.class );
        String typeName1 = "relationshiptype1";
        transaction.relationshipTypeDoCreateForName( typeName1, relType1 );
        int relType2 = (int) nextId( RelationshipType.class );
        String typeName2 = "relationshiptype2";
        transaction.relationshipTypeDoCreateForName( typeName2, relType2 );
        long rel1 = nextId( Relationship.class );
        transaction.relationshipDoCreate( rel1, relType1, node1, node2 );
        long rel2 = nextId( Relationship.class );
        transaction.relationshipDoCreate( rel2, relType2, node2, node1 );

        StorageProperty r1prop1 = relAddProperty( rel1, index( "prop1" ), "string1" );
        StorageProperty r1prop2 = relAddProperty( rel1, index( "prop2" ), 1 );
        StorageProperty r1prop3 = relAddProperty( rel1, index( "prop3" ), true );

        StorageProperty r2prop1 = relAddProperty( rel2, index( "prop1" ), "string2" );
        StorageProperty r2prop2 = relAddProperty( rel2, index( "prop2" ), 2 );
        StorageProperty r2prop3 = relAddProperty( rel2, index( "prop3" ), false );
        commitTx();
        ds.stop();

        initializeStores( storeDir, stringMap() );
        startTx();
        // validate node
        validateNodeRel1( node1, n1prop1, n1prop2, n1prop3, rel1, rel2, relType1, relType2 );
        validateNodeRel2( node2, n2prop1, n2prop2, n2prop3, rel1, rel2, relType1, relType2 );
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
        long[] nodeIds = new long[10];
        for ( int i = 0; i < 3; i++ )
        {
            nodeIds[i] = nextId( Node.class );
            transaction.nodeDoCreate( nodeIds[i] );
            nodeAddProperty( nodeIds[i], index( "nisse" ), 10 - i );
        }
        for ( int i = 0; i < 2; i++ )
        {
            long id = nextId( Relationship.class );
            transaction.relationshipDoCreate( id, relType1, nodeIds[i], nodeIds[i + 1] );
            transaction.relationshipDoDelete( id, relType1, nodeIds[i], nodeIds[i + 1] );
        }
        for ( int i = 0; i < 3; i++ )
        {
            transaction.nodeDoDelete( nodeIds[i] );
        }
        commitTx();
        ds.stop();
    }

    private StorageProperty nodeAddProperty( long nodeId, int key, Object value )
    {
        StorageProperty property = new PropertyKeyValue( key, Values.of( value ) );
        StorageProperty oldProperty = null;
        try ( StorageStatement statement = storeLayer.newStatement();
                Cursor<NodeItem> cursor = statement.acquireSingleNodeCursor( nodeId ) )
        {
            if ( cursor.next() )
            {
                if ( cursor.next() )
                {
                    StorageProperty fetched = getProperty( key, statement, cursor.get().nextPropertyId() );
                    if ( fetched != null )
                    {
                        oldProperty = fetched;
                    }
                }
            }
        }

        if ( oldProperty == null )
        {
            transaction.nodeDoAddProperty( nodeId, key, property.value() );
        }
        else
        {
            transaction.nodeDoChangeProperty( nodeId, key, oldProperty.value(), property.value() );
        }
        return property;
    }

    private StorageProperty relAddProperty( long relationshipId, int key, Object value )
    {
        StorageProperty property = new PropertyKeyValue( key, Values.of( value ) );
        Value oldValue = Values.NO_VALUE;
        try ( StorageStatement statement = storeLayer.newStatement();
                Cursor<RelationshipItem> cursor = statement.acquireSingleRelationshipCursor( relationshipId ) )
        {
            if ( cursor.next() )
            {
                StorageProperty fetched = getProperty( key, statement, cursor.get().nextPropertyId() );
                if ( fetched != null )
                {
                    oldValue = fetched.value();
                }
            }
        }

        transaction.relationshipDoReplaceProperty( relationshipId, key, oldValue, Values.of( value ) );
        return property;
    }

    private StorageProperty getProperty( int key, StorageStatement statement, long propertyId )
    {
        try ( Cursor<PropertyItem> propertyCursor = statement
                .acquireSinglePropertyCursor( propertyId, key, NO_LOCK, ALWAYS_OPEN ) )
        {
            if ( propertyCursor.next() )
            {
                Value oldValue = propertyCursor.get().value();
                if ( oldValue != null )
                {
                    return new PropertyKeyValue( key, oldValue );
                }
            }
        }
        return null;
    }

    @Test
    public void testRels1() throws Exception
    {
        initializeStores( storeDir, stringMap() );
        startTx();
        int relType1 = (int) nextId( RelationshipType.class );
        String typeName = "relationshiptype1";
        transaction.relationshipTypeDoCreateForName( typeName, relType1 );
        long[] nodeIds = new long[3];
        for ( int i = 0; i < 3; i++ )
        {
            nodeIds[i] = nextId( Node.class );
            transaction.nodeDoCreate( nodeIds[i] );
            nodeAddProperty( nodeIds[i], index( "nisse" ), 10 - i );
        }
        for ( int i = 0; i < 2; i++ )
        {
            transaction.relationshipDoCreate( nextId( Relationship.class ),
                    relType1, nodeIds[i], nodeIds[i + 1] );
        }
        commitTx();
        startTx();
        for ( int i = 0; i < 3; i += 2 )
        {
            deleteRelationships( nodeIds[i] );
            transaction.nodeDoDelete( nodeIds[i] );
        }
        commitTx();
        ds.stop();
    }

    private void relDelete( long id )
    {
        RelationshipVisitor<RuntimeException> visitor = ( relId, type, startNode, endNode ) ->
                transaction.relationshipDoDelete( relId, type, startNode, endNode );
        if ( !transaction.relationshipVisit( id, visitor ) )
        {
            try
            {
                storeLayer.relationshipVisit( id, visitor );
            }
            catch ( EntityNotFoundException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    @Test
    public void testRels2() throws Exception
    {
        initializeStores( storeDir, stringMap() );
        startTx();
        int relType1 = (int) nextId( RelationshipType.class );
        String typeName = "relationshiptype1";
        transaction.relationshipTypeDoCreateForName( typeName, relType1 );
        long[] nodeIds = new long[3];
        for ( int i = 0; i < 3; i++ )
        {
            nodeIds[i] = nextId( Node.class );
            transaction.nodeDoCreate( nodeIds[i] );
            nodeAddProperty( nodeIds[i], index( "nisse" ), 10 - i );
        }
        for ( int i = 0; i < 2; i++ )
        {
            transaction.relationshipDoCreate( nextId( Relationship.class ),
                    relType1, nodeIds[i], nodeIds[i + 1] );
        }
        transaction.relationshipDoCreate( nextId( Relationship.class ),
                relType1, nodeIds[0], nodeIds[2] );
        commitTx();
        startTx();
        for ( int i = 0; i < 3; i++ )
        {
            deleteRelationships( nodeIds[i] );
            transaction.nodeDoDelete( nodeIds[i] );
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
        transaction.relationshipTypeDoCreateForName( "relationshiptype1", relType1 );
        long[] nodeIds = new long[8];
        for ( int i = 0; i < nodeIds.length; i++ )
        {
            nodeIds[i] = nextId( Node.class );
            transaction.nodeDoCreate( nodeIds[i] );
        }
        for ( int i = 0; i < nodeIds.length / 2; i++ )
        {
            transaction.relationshipDoCreate( nextId( Relationship.class ),
                    relType1, nodeIds[i], nodeIds[i * 2] );
        }
        long rel5 = nextId( Relationship.class );
        transaction.relationshipDoCreate( rel5, relType1, nodeIds[0], nodeIds[5] );
        long rel2 = nextId( Relationship.class );
        transaction.relationshipDoCreate( rel2, relType1, nodeIds[1], nodeIds[2] );
        long rel3 = nextId( Relationship.class );
        transaction.relationshipDoCreate( rel3, relType1, nodeIds[1], nodeIds[3] );
        long rel6 = nextId( Relationship.class );
        transaction.relationshipDoCreate( rel6, relType1, nodeIds[1], nodeIds[6] );
        long rel1 = nextId( Relationship.class );
        transaction.relationshipDoCreate( rel1, relType1, nodeIds[0], nodeIds[1] );
        long rel4 = nextId( Relationship.class );
        transaction.relationshipDoCreate( rel4, relType1, nodeIds[0], nodeIds[4] );
        long rel7 = nextId( Relationship.class );
        transaction.relationshipDoCreate( rel7, relType1, nodeIds[0], nodeIds[7] );
        commitTx();
        startTx();
        relDelete( rel7 );
        relDelete( rel4 );
        relDelete( rel1 );
        relDelete( rel6 );
        relDelete( rel3 );
        relDelete( rel2 );
        relDelete( rel5 );
        commitTx();
        ds.stop();
    }

    @Test
    public void testProps1() throws Exception
    {
        initializeStores( storeDir, stringMap() );
        startTx();
        long nodeId = nextId( Node.class );
        transaction.nodeDoCreate( nodeId );
        pStore.nextId();
        StorageProperty prop = nodeAddProperty( nodeId, index( "nisse" ), 10 );
        commitTx();
        ds.stop();
        initializeStores( storeDir, stringMap() );
        startTx();
        StorageProperty prop2 = nodeAddProperty( nodeId, prop.propertyKeyId(), 5 );
        transaction.nodeDoRemoveProperty( nodeId, prop2.propertyKeyId() );
        transaction.nodeDoDelete( nodeId );
        commitTx();
        ds.stop();
    }

    @Test
    public void testSetBlockSize() throws Exception
    {
        File storeDir = dir.directory( "small_store" );
        initializeStores( storeDir, stringMap(
                "unsupported.dbms.block_size.strings", "62",
                "unsupported.dbms.block_size.array_properties", "302" ) );
        assertEquals( 62 + DynamicRecordFormat.RECORD_HEADER_SIZE,
                pStore.getStringStore().getRecordSize() );
        assertEquals( 302 + DynamicRecordFormat.RECORD_HEADER_SIZE,
                pStore.getArrayStore().getRecordSize() );
        ds.stop();
    }

    @Test
    public void setVersion() throws Exception
    {
        FileSystemAbstraction fileSystem = fs.get();
        File storeDir = new File( "target/test-data/set-version" ).getAbsoluteFile();
        createTestDatabase( fileSystem, storeDir ).shutdown();
        assertEquals( 0, MetaDataStore.setRecord( pageCache, new File( storeDir,
                MetaDataStore.DEFAULT_NAME ).getAbsoluteFile(), Position.LOG_VERSION, 10 ) );
        assertEquals( 10, MetaDataStore.setRecord( pageCache, new File( storeDir,
                MetaDataStore.DEFAULT_NAME ).getAbsoluteFile(), Position.LOG_VERSION, 12 ) );

        Config config = Config.defaults();
        StoreFactory sf = new StoreFactory( storeDir, config, new DefaultIdGeneratorFactory( fileSystem ), pageCache,
                fileSystem, LOG_PROVIDER, EmptyVersionContextSupplier.EMPTY );

        NeoStores neoStores = sf.openAllNeoStores();
        assertEquals( 12, neoStores.getMetaDataStore().getCurrentLogVersion() );
        neoStores.close();
    }

    @Test
    public void shouldNotReadNonRecordDataAsRecord() throws Exception
    {
        FileSystemAbstraction fileSystem = fs.get();
        File neoStoreDir = new File( "/tmp/graph.db/neostore" ).getAbsoluteFile();
        StoreFactory factory = newStoreFactory( neoStoreDir, pageCache, fileSystem );
        long recordVersion = defaultStoreVersion();
        try ( NeoStores neoStores = factory.openAllNeoStores( true ) )
        {
            MetaDataStore metaDataStore = neoStores.getMetaDataStore();
            metaDataStore.setCreationTime( 3 );
            metaDataStore.setRandomNumber( 4 );
            metaDataStore.setCurrentLogVersion( 5 );
            metaDataStore.setLastCommittedAndClosedTransactionId( 6, 0, 0, 43, 44 );
            metaDataStore.setStoreVersion( recordVersion );

            metaDataStore.setGraphNextProp( 8 );
            metaDataStore.setLatestConstraintIntroducingTx( 9 );
        }

        File file = new File( neoStoreDir, MetaDataStore.DEFAULT_NAME );
        try ( StoreChannel channel = fileSystem.open( file, OpenMode.READ_WRITE ) )
        {
            channel.position( 0 );
            channel.write( ByteBuffer.wrap( UTF8.encode( "This is some data that is not a record." ) ) );
        }

        MetaDataStore.setRecord( pageCache, file, Position.STORE_VERSION, recordVersion );

        try ( NeoStores neoStores = factory.openAllNeoStores() )
        {
            MetaDataStore metaDataStore = neoStores.getMetaDataStore();
            assertEquals( FIELD_NOT_PRESENT, metaDataStore.getCreationTime() );
            assertEquals( FIELD_NOT_PRESENT, metaDataStore.getRandomNumber() );
            assertEquals( FIELD_NOT_PRESENT, metaDataStore.getCurrentLogVersion() );
            assertEquals( FIELD_NOT_PRESENT, metaDataStore.getLastCommittedTransactionId() );
            assertEquals( FIELD_NOT_PRESENT, metaDataStore.getLastClosedTransactionId() );
            assertEquals( recordVersion, metaDataStore.getStoreVersion() );
            assertEquals( 8, metaDataStore.getGraphNextProp() );
            assertEquals( 9, metaDataStore.getLatestConstraintIntroducingTx() );
            assertArrayEquals( metaDataStore.getLastClosedTransaction(), new long[]{FIELD_NOT_PRESENT,44,43} );
        }
    }

    @Test
    public void testSetLatestConstraintTx()
    {
        // given
        Config config = Config.defaults();
        StoreFactory sf = new StoreFactory( dir.directory(), config, new DefaultIdGeneratorFactory( fs.get() ),
                pageCacheRule.getPageCache( fs.get() ), fs.get(), LOG_PROVIDER, EmptyVersionContextSupplier.EMPTY );

        // when
        NeoStores neoStores = sf.openAllNeoStores( true );
        MetaDataStore metaDataStore = neoStores.getMetaDataStore();

        // then the default is 0
        assertEquals( 0L, metaDataStore.getLatestConstraintIntroducingTx() );

        // when
        metaDataStore.setLatestConstraintIntroducingTx( 10L );

        // then
        assertEquals( 10L, metaDataStore.getLatestConstraintIntroducingTx() );

        // when
        neoStores.flush( IOLimiter.unlimited() );
        neoStores.close();
        neoStores = sf.openAllNeoStores();

        // then the value should have been stored
        assertEquals( 10L, neoStores.getMetaDataStore().getLatestConstraintIntroducingTx() );
        neoStores.close();
    }

    @Test
    public void shouldInitializeTheTxIdToOne()
    {
        StoreFactory factory =
                new StoreFactory(
                        new File( "graph.db/neostore" ), Config.defaults(), new DefaultIdGeneratorFactory( fs.get() ),
                        pageCache, fs.get(),
                        LOG_PROVIDER, EmptyVersionContextSupplier.EMPTY );

        try ( NeoStores neoStores = factory.openAllNeoStores( true ) )
        {
            neoStores.getMetaDataStore();
        }

        try ( NeoStores neoStores = factory.openAllNeoStores() )
        {
            long lastCommittedTransactionId = neoStores.getMetaDataStore().getLastCommittedTransactionId();
            assertEquals( TransactionIdStore.BASE_TX_ID, lastCommittedTransactionId );
        }
    }

    @Test
    public void shouldThrowUnderlyingStorageExceptionWhenFailingToLoadStorage()
    {
        FileSystemAbstraction fileSystem = fs.get();
        File neoStoreDir = new File( "/tmp/graph.db/neostore" ).getAbsoluteFile();
        StoreFactory factory = new StoreFactory(
                neoStoreDir, Config.defaults(), new DefaultIdGeneratorFactory( fileSystem ), pageCache, fileSystem,
                LOG_PROVIDER, EmptyVersionContextSupplier.EMPTY );

        try ( NeoStores neoStores = factory.openAllNeoStores( true ) )
        {
            neoStores.getMetaDataStore();
        }
        File file = new File( neoStoreDir, MetaDataStore.DEFAULT_NAME );
        fileSystem.deleteFile( file );

        exception.expect( StoreNotFoundException.class );
        try ( NeoStores neoStores = factory.openAllNeoStores() )
        {
            neoStores.getMetaDataStore();
        }
    }

    @Test
    public void shouldAddUpgradeFieldsToTheNeoStoreIfNotPresent() throws IOException
    {
        FileSystemAbstraction fileSystem = fs.get();
        File neoStoreDir = new File( "/tmp/graph.db/neostore" ).getAbsoluteFile();
        StoreFactory factory = newStoreFactory( neoStoreDir, pageCache, fileSystem );
        long recordVersion = defaultStoreVersion();
        try ( NeoStores neoStores = factory.openAllNeoStores( true ) )
        {
            MetaDataStore metaDataStore = neoStores.getMetaDataStore();
            metaDataStore.setCreationTime( 3 );
            metaDataStore.setRandomNumber( 4 );
            metaDataStore.setCurrentLogVersion( 5 );
            metaDataStore.setLastCommittedAndClosedTransactionId( 6, 42, BASE_TX_COMMIT_TIMESTAMP, 43, 44 );
            metaDataStore.setStoreVersion( recordVersion );

            metaDataStore.setGraphNextProp( 8 );
            metaDataStore.setLatestConstraintIntroducingTx( 9 );
        }

        File file = new File( neoStoreDir, MetaDataStore.DEFAULT_NAME );

        assertNotEquals( 10, MetaDataStore.getRecord( pageCache, file, Position.UPGRADE_TRANSACTION_ID ) );
        assertNotEquals( 11, MetaDataStore.getRecord( pageCache, file, Position.UPGRADE_TRANSACTION_CHECKSUM ) );

        MetaDataStore.setRecord( pageCache, file, Position.UPGRADE_TRANSACTION_ID, 10 );
        MetaDataStore.setRecord( pageCache, file, Position.UPGRADE_TRANSACTION_CHECKSUM, 11 );
        MetaDataStore.setRecord( pageCache, file, Position.UPGRADE_TIME, 12 );

        try ( NeoStores neoStores = factory.openAllNeoStores() )
        {
            MetaDataStore metaDataStore = neoStores.getMetaDataStore();
            assertEquals( 3, metaDataStore.getCreationTime() );
            assertEquals( 4, metaDataStore.getRandomNumber() );
            assertEquals( 5, metaDataStore.getCurrentLogVersion() );
            assertEquals( 6, metaDataStore.getLastCommittedTransactionId() );
            assertEquals( recordVersion, metaDataStore.getStoreVersion() );
            assertEquals( 8, metaDataStore.getGraphNextProp() );
            assertEquals( 9, metaDataStore.getLatestConstraintIntroducingTx() );
            assertEquals( new TransactionId( 10, 11, BASE_TX_COMMIT_TIMESTAMP ),
                    metaDataStore.getUpgradeTransaction() );
            assertEquals( 12, metaDataStore.getUpgradeTime() );
            assertArrayEquals( metaDataStore.getLastClosedTransaction(), new long[]{6,44,43} );
        }

        MetaDataStore.setRecord( pageCache, file, Position.UPGRADE_TRANSACTION_COMMIT_TIMESTAMP, 13 );

        try ( NeoStores neoStores = factory.openAllNeoStores() )
        {
            MetaDataStore metaDataStore = neoStores.getMetaDataStore();
            assertEquals( new TransactionId( 10, 11, 13 ),
                    metaDataStore.getUpgradeTransaction() );
        }
    }

    @Test
    public void shouldSetHighestTransactionIdWhenNeeded() throws Throwable
    {
        // GIVEN
        FileSystemAbstraction fileSystem = fs.get();
        fileSystem.mkdirs( storeDir );
        StoreFactory factory = new StoreFactory(
                storeDir, Config.defaults(), new DefaultIdGeneratorFactory( fileSystem ), pageCache, fileSystem,
                LOG_PROVIDER, EmptyVersionContextSupplier.EMPTY );

        try ( NeoStores neoStore = factory.openAllNeoStores( true ) )
        {
            MetaDataStore store = neoStore.getMetaDataStore();
            store.setLastCommittedAndClosedTransactionId( 40, 4444, BASE_TX_COMMIT_TIMESTAMP,
                    LogHeader.LOG_HEADER_SIZE, 0 );

            // WHEN
            store.transactionCommitted( 42, 6666, BASE_TX_COMMIT_TIMESTAMP );

            // THEN
            assertEquals( new TransactionId( 42, 6666, BASE_TX_COMMIT_TIMESTAMP ),
                    store.getLastCommittedTransaction() );
            assertArrayEquals( store.getLastClosedTransaction(), new long[]{40,0,LogHeader.LOG_HEADER_SIZE} );
        }
    }

    @Test
    public void shouldNotSetHighestTransactionIdWhenNeeded() throws Throwable
    {
        // GIVEN
        FileSystemAbstraction fileSystem = fs.get();
        fileSystem.mkdirs( storeDir );
        StoreFactory factory = new StoreFactory(
                storeDir, Config.defaults(), new DefaultIdGeneratorFactory( fileSystem ), pageCache, fileSystem,
                LOG_PROVIDER, EmptyVersionContextSupplier.EMPTY );

        try ( NeoStores neoStore = factory.openAllNeoStores( true ) )
        {
            MetaDataStore store = neoStore.getMetaDataStore();
            store.setLastCommittedAndClosedTransactionId( 40, 4444, BASE_TX_COMMIT_TIMESTAMP,
                    LogHeader.LOG_HEADER_SIZE, 0 );

            // WHEN
            store.transactionCommitted( 39, 3333, BASE_TX_COMMIT_TIMESTAMP );

            // THEN
            assertEquals( new TransactionId( 40, 4444, BASE_TX_COMMIT_TIMESTAMP ),
                    store.getLastCommittedTransaction() );
            assertArrayEquals( store.getLastClosedTransaction(), new long[]{40,0,LogHeader.LOG_HEADER_SIZE} );
        }
    }

    @Test
    public void shouldCloseAllTheStoreEvenIfExceptionsAreThrown() throws Exception
    {
        // given
        FileSystemAbstraction fileSystem = fs.get();
        Config defaults = Config.defaults( counts_store_rotation_timeout, "60m" );
        StoreFactory factory =
                new StoreFactory( storeDir, defaults, new DefaultIdGeneratorFactory( fileSystem ), pageCache,
                        fileSystem, LOG_PROVIDER, EmptyVersionContextSupplier.EMPTY );
        NeoStores neoStore = factory.openAllNeoStores( true );

        // let's hack the counts store so it fails to rotate and hence it fails to close as well...
        final CountsTracker counts = neoStore.getCounts();
        counts.start();
        long nextTxId = neoStore.getMetaDataStore().getLastCommittedTransactionId() + 1;
        AtomicReference<Throwable> exRef = new AtomicReference<>();
        Thread thread = new Thread( () ->
        {
            try
            {
                counts.rotate( nextTxId );
            }
            catch ( InterruptedIOException e )
            {
                // expected due to the interrupted below
            }
            catch ( Throwable e )
            {
                exRef.set( e );
                throw new RuntimeException( e );
            }
        } );
        thread.start();

        // let's wait for the thread to start waiting for the next transaction id
        ThreadTestUtils.awaitThreadState( thread, TimeUnit.SECONDS.toMillis( 5 ), Thread.State.TIMED_WAITING,
                Thread.State.WAITING );

        try
        {
            // when we close the stores...
            neoStore.close();
            fail( "should have thrown2" );
        }
        catch ( IllegalStateException ex )
        {
            // then
            assertEquals( "Cannot stop in state: rotating", ex.getMessage() );
        }

        thread.interrupt();
        thread.join();

        // and the page cache closes with no errors
        pageCache.close();
        // and only InterruptedIOException is thrown in the other thread
        assertNull( exRef.get() );
    }

    @Test
    public void isPresentAfterCreatingAllStores() throws Exception
    {
        // given
        FileSystemAbstraction fileSystem = fs.get();
        fileSystem.deleteRecursively( storeDir );
        DefaultIdGeneratorFactory idFactory = new DefaultIdGeneratorFactory( fileSystem );
        StoreFactory factory = new StoreFactory( storeDir, Config.defaults(), idFactory, pageCache, fileSystem,
                LOG_PROVIDER, EmptyVersionContextSupplier.EMPTY );

        // when
        try ( NeoStores ignore = factory.openAllNeoStores( true ) )
        {
            // then
            assertTrue( NeoStores.isStorePresent( pageCache, storeDir ) );
        }
    }

    @Test
    public void isPresentFalseAfterCreatingAllButLastStoreType() throws Exception
    {
        // given
        FileSystemAbstraction fileSystem = fs.get();
        fileSystem.deleteRecursively( storeDir );
        DefaultIdGeneratorFactory idFactory = new DefaultIdGeneratorFactory( fileSystem );
        StoreFactory factory = new StoreFactory( storeDir, Config.defaults(), idFactory, pageCache, fileSystem,
                LOG_PROVIDER, EmptyVersionContextSupplier.EMPTY );
        StoreType[] allStoreTypes = StoreType.values();
        StoreType[] allButLastStoreTypes = Arrays.copyOf( allStoreTypes, allStoreTypes.length - 1 );

        // when
        try ( NeoStores ignore = factory.openNeoStores( true, allButLastStoreTypes ) )
        {
            // then
            assertFalse( NeoStores.isStorePresent( pageCache, storeDir ) );
        }
    }

    private static class MyPropertyKeyToken extends Token
    {
        private static Map<String,Token> stringToIndex = new HashMap<>();
        private static Map<Integer,Token> intToIndex = new HashMap<>();

        protected MyPropertyKeyToken( String key, int keyId )
        {
            super( key, keyId );
        }

        public static Iterable<Token> index( String key )
        {
            if ( stringToIndex.containsKey( key ) )
            {
                return Collections.singletonList( stringToIndex.get( key ) );
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

    private static long defaultStoreVersion()
    {
        return MetaDataStore.versionStringToLong( RecordFormatSelector.defaultFormat().storeVersion() );
    }

    private static StoreFactory newStoreFactory( File neoStoreDir, PageCache pageCache, FileSystemAbstraction fs )
    {
        RecordFormats recordFormats = RecordFormatSelector.defaultFormat();
        Config config = Config.defaults();
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs );
        return new StoreFactory( neoStoreDir, config, idGeneratorFactory, pageCache, fs, recordFormats, LOG_PROVIDER,
                EmptyVersionContextSupplier.EMPTY );
    }

    private Token createDummyIndex( int id, String key )
    {
        MyPropertyKeyToken index = new MyPropertyKeyToken( key, id );
        MyPropertyKeyToken.add( index );
        return index;
    }

    private void initializeStores( File storeDir, Map<String,String> additionalConfig ) throws IOException
    {
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( Config.defaults( additionalConfig ) );
        ds = dsRule.getDataSource( storeDir, fs.get(), pageCache, dependencies );
        ds.init();
        ds.start();

        NeoStores neoStores = ds.getDependencyResolver()
                .resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        pStore = neoStores.getPropertyStore();
        rtStore = neoStores.getRelationshipTypeTokenStore();
        storeLayer = ds.getStoreLayer();
        propertyLoader = new PropertyLoader( neoStores );
    }

    private void startTx() throws TransactionFailureException
    {
        tx = ds.getKernel().newTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED );
        transaction = ((KernelTransactionImplementation) tx).txState();
    }

    private void commitTx() throws TransactionFailureException
    {
        tx.success();
        tx.close();
    }

    private int index( String key )
    {
        Iterator<Token> itr = MyPropertyKeyToken.index( key ).iterator();
        if ( !itr.hasNext() )
        {
            int id = (int) nextId( PropertyKeyTokenRecord.class );
            createDummyIndex( id, key );
            transaction.propertyKeyDoCreateForName( key, id );
            return id;
        }
        return itr.next().id();
    }

    private long nextId( Class<?> clazz )
    {
        NeoStores neoStores = ds.getDependencyResolver()
                .resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        if ( clazz.equals( PropertyKeyTokenRecord.class ) )
        {
            return neoStores.getPropertyKeyTokenStore().nextId();
        }
        if ( clazz.equals( RelationshipType.class ) )
        {
            return neoStores.getRelationshipTypeTokenStore().nextId();
        }
        if ( clazz.equals( Node.class ) )
        {
            return neoStores.getNodeStore().nextId();
        }
        if ( clazz.equals( Relationship.class ) )
        {
            return neoStores.getRelationshipStore().nextId();
        }
        throw new IllegalArgumentException( clazz.getName() );
    }

    private void validateNodeRel1( final long node, StorageProperty prop1,
            StorageProperty prop2, StorageProperty prop3, long rel1, long rel2,
            final int relType1, final int relType2 ) throws IOException
    {
        assertTrue( nodeExists( node ) );
        Map<Integer,Pair<StorageProperty,Long>> props = new HashMap<>();
        PropertyReceiver<StorageProperty> receiver = newPropertyReceiver( props );
        propertyLoader.nodeLoadProperties( node, receiver );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).other();
            PropertyRecord record = getRecord( pStore, id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).first().propertyKeyId() );
            StorageProperty data = block.newPropertyKeyValue( pStore );
            if ( data.propertyKeyId() == prop1.propertyKeyId() )
            {
                assertEquals( "prop1", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( "string1", data.value().asObject() );
                nodeAddProperty( node, prop1.propertyKeyId(), "-string1" );
            }
            else if ( data.propertyKeyId() == prop2.propertyKeyId() )
            {
                assertEquals( "prop2", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( 1, data.value().asObject() );
                nodeAddProperty( node, prop2.propertyKeyId(), -1 );
            }
            else if ( data.propertyKeyId() == prop3.propertyKeyId() )
            {
                assertEquals( "prop3", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( true, data.value().asObject() );
                nodeAddProperty( node, prop3.propertyKeyId(), false );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );

        count = validateAndCountRelationships( node, rel1, rel2, relType1, relType2 );
        assertEquals( 2, count );
    }

    private int validateAndCountRelationships( long node, long rel1, long rel2, int relType1, int relType2 )
            throws IOException
    {
        int count = 0;
        try ( KernelStatement statement = (KernelStatement) tx.acquireStatement();
              Cursor<NodeItem> nodeCursor = statement.getStoreStatement().acquireSingleNodeCursor( node ) )
        {
            nodeCursor.next();

            NodeItem nodeItem = nodeCursor.get();
            try ( Cursor<RelationshipItem> relationships = statement.getStoreStatement().acquireNodeRelationshipCursor(
                    nodeItem.isDense(), nodeItem.id(), nodeItem.nextRelationshipId(), BOTH, ALWAYS_TRUE_INT ) )
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
        return count;
    }

    private PropertyReceiver<StorageProperty> newPropertyReceiver( final Map<Integer,Pair<StorageProperty,Long>> props )
    {
        return ( property, propertyRecordId ) -> props.put( property.propertyKeyId(), Pair.of( property, propertyRecordId ) );
    }

    private void validateNodeRel2( final long node, StorageProperty prop1,
            StorageProperty prop2, StorageProperty prop3,
            long rel1, long rel2, final int relType1, final int relType2 )
            throws IOException, RuntimeException
    {
        assertTrue( nodeExists( node ) );
        Map<Integer,Pair<StorageProperty,Long>> props = new HashMap<>();
        propertyLoader.nodeLoadProperties( node, newPropertyReceiver( props ) );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).other();
            PropertyRecord record = getRecord( pStore, id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).first().propertyKeyId() );
            StorageProperty data = block.newPropertyKeyValue( pStore );
            if ( data.propertyKeyId() == prop1.propertyKeyId() )
            {
                assertEquals( "prop1", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( "string2", data.value().asObject() );
                nodeAddProperty( node, prop1.propertyKeyId(), "-string2" );
            }
            else if ( data.propertyKeyId() == prop2.propertyKeyId() )
            {
                assertEquals( "prop2", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( 2, data.value().asObject() );
                nodeAddProperty( node, prop2.propertyKeyId(), -2 );
            }
            else if ( data.propertyKeyId() == prop3.propertyKeyId() )
            {
                assertEquals( "prop3", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( false, data.value().asObject() );
                nodeAddProperty( node, prop3.propertyKeyId(), true );
            }
            else
            {
                throw new IOException();
            }
            count++;
        }
        assertEquals( 3, count );
        count = 0;

        try ( KernelStatement statement = (KernelStatement) tx.acquireStatement();
              Cursor<NodeItem> nodeCursor = statement.getStoreStatement().acquireSingleNodeCursor( node ) )
        {
            nodeCursor.next();
            NodeItem nodeItem = nodeCursor.get();
            try ( Cursor<RelationshipItem> relationships = statement.getStoreStatement().acquireNodeRelationshipCursor(
                    nodeItem.isDense(), nodeItem.id(), nodeItem.nextRelationshipId(), BOTH, ALWAYS_TRUE_INT ) )
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
        try ( StorageStatement statement = storeLayer.newStatement() )
        {
            try ( Cursor<NodeItem> node = statement.acquireSingleNodeCursor( nodeId ) )
            {
                return node.next();
            }
        }
    }

    private void validateRel1( long rel, StorageProperty prop1,
            StorageProperty prop2, StorageProperty prop3, long firstNode, long secondNode,
            int relType ) throws IOException
    {
        Map<Integer,Pair<StorageProperty,Long>> props = new HashMap<>();
        propertyLoader.relLoadProperties( rel, newPropertyReceiver( props ) );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).other();
            PropertyRecord record = getRecord( pStore, id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).first().propertyKeyId() );
            StorageProperty data = block.newPropertyKeyValue( pStore );
            if ( data.propertyKeyId() == prop1.propertyKeyId() )
            {
                assertEquals( "prop1", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( "string1", data.value().asObject() );
                relAddProperty( rel, prop1.propertyKeyId(), "-string1" );
            }
            else if ( data.propertyKeyId() == prop2.propertyKeyId() )
            {
                assertEquals( "prop2", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( 1, data.value().asObject() );
                relAddProperty( rel, prop2.propertyKeyId(), -1 );
            }
            else if ( data.propertyKeyId() == prop3.propertyKeyId() )
            {
                assertEquals( "prop3", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( true, data.value().asObject() );
                relAddProperty( rel, prop3.propertyKeyId(), false );
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
            storeLayer.relationshipVisit( rel, ( relId, type, startNode, endNode ) ->
            {
                assertEquals( firstNode, startNode );
                assertEquals( secondNode, endNode );
                assertEquals( relType, type );
            } );
        }
        catch ( EntityNotFoundException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void validateRel2( long rel, StorageProperty prop1,
            StorageProperty prop2, StorageProperty prop3,
            long firstNode, long secondNode, int relType ) throws IOException
    {
        Map<Integer,Pair<StorageProperty,Long>> props = new HashMap<>();
        propertyLoader.relLoadProperties( rel, newPropertyReceiver( props ) );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).other();
            PropertyRecord record = getRecord( pStore, id );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).first().propertyKeyId() );
            StorageProperty data = block.newPropertyKeyValue( pStore );
            if ( data.propertyKeyId() == prop1.propertyKeyId() )
            {
                assertEquals( "prop1", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( "string2", data.value().asObject() );
                relAddProperty( rel, prop1.propertyKeyId(), "-string2" );
            }
            else if ( data.propertyKeyId() == prop2.propertyKeyId() )
            {
                assertEquals( "prop2", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( 2, data.value().asObject() );
                relAddProperty( rel, prop2.propertyKeyId(), -2 );
            }
            else if ( data.propertyKeyId() == prop3.propertyKeyId() )
            {
                assertEquals( "prop3", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( false, data.value().asObject() );
                relAddProperty( rel, prop3.propertyKeyId(), true );
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

    private void deleteRel1( long rel, StorageProperty prop1, StorageProperty prop2,
            StorageProperty prop3, long firstNode, long secondNode, int relType )
            throws Exception
    {
        Map<Integer,Pair<StorageProperty,Long>> props = new HashMap<>();
        propertyLoader.relLoadProperties( rel, newPropertyReceiver( props ) );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).other();
            PropertyRecord record = pStore.getRecord( id, pStore.newRecord(), NORMAL );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).first().propertyKeyId() );
            StorageProperty data = block.newPropertyKeyValue( pStore );
            if ( data.propertyKeyId() == prop1.propertyKeyId() )
            {
                assertEquals( "prop1", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( "-string1", data.value().asObject() );
            }
            else if ( data.propertyKeyId() == prop2.propertyKeyId() )
            {
                assertEquals( "prop2", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( -1, data.value().asObject() );
            }
            else if ( data.propertyKeyId() == prop3.propertyKeyId() )
            {
                assertEquals( "prop3", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( false, data.value().asObject() );
                transaction.relationshipDoRemoveProperty( rel, prop3.propertyKeyId() );
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
        relDelete( rel );

        assertHasRelationships( firstNode );

        assertHasRelationships( secondNode );
    }

    private static class CountingPropertyReceiver implements PropertyReceiver<StorageProperty>
    {
        private int count;

        @Override
        public void receive( StorageProperty property, long propertyRecordId )
        {
            count++;
        }
    }

    private void deleteRel2( long rel, StorageProperty prop1, StorageProperty prop2,
            StorageProperty prop3, long firstNode, long secondNode, int relType )
            throws Exception
    {
        Map<Integer,Pair<StorageProperty,Long>> props = new HashMap<>();
        propertyLoader.relLoadProperties( rel, newPropertyReceiver( props ) );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).other();
            PropertyRecord record = pStore.getRecord( id, pStore.newRecord(), NORMAL );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).first().propertyKeyId() );
            StorageProperty data = block.newPropertyKeyValue( pStore );
            if ( data.propertyKeyId() == prop1.propertyKeyId() )
            {
                assertEquals( "prop1", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( "-string2", data.value().asObject() );
            }
            else if ( data.propertyKeyId() == prop2.propertyKeyId() )
            {
                assertEquals( "prop2", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( -2, data.value().asObject() );
            }
            else if ( data.propertyKeyId() == prop3.propertyKeyId() )
            {
                assertEquals( "prop3", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( true, data.value().asObject() );
                transaction.relationshipDoRemoveProperty( rel, prop3.propertyKeyId() );
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
        relDelete( rel );

        assertHasRelationships( firstNode );

        assertHasRelationships( secondNode );

    }

    private void assertHasRelationships( long node )
    {
        try ( KernelStatement statement = (KernelStatement) tx.acquireStatement();
              Cursor<NodeItem> nodeCursor = statement.getStoreStatement().acquireSingleNodeCursor( node ) )
        {
            nodeCursor.next();
            NodeItem nodeItem = nodeCursor.get();
            try ( Cursor<RelationshipItem> relationships = statement.getStoreStatement().acquireNodeRelationshipCursor(
                    nodeItem.isDense(), nodeItem.id(), nodeItem.nextRelationshipId(), BOTH, ALWAYS_TRUE_INT ) )
            {
                assertTrue( relationships.next() );
            }
        }
    }

    private void deleteNode1( long node, StorageProperty prop1,
            StorageProperty prop2, StorageProperty prop3 )
            throws IOException
    {
        Map<Integer,Pair<StorageProperty,Long>> props = new HashMap<>();
        propertyLoader.nodeLoadProperties( node, newPropertyReceiver( props ) );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).other();
            PropertyRecord record = pStore.getRecord( id, pStore.newRecord(), NORMAL );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).first().propertyKeyId() );
            StorageProperty data = block.newPropertyKeyValue( pStore );
            if ( data.propertyKeyId() == prop1.propertyKeyId() )
            {
                assertEquals( "prop1", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( "-string1", data.value().asObject() );
            }
            else if ( data.propertyKeyId() == prop2.propertyKeyId() )
            {
                assertEquals( "prop2", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( -1, data.value().asObject() );
            }
            else if ( data.propertyKeyId() == prop3.propertyKeyId() )
            {
                assertEquals( "prop3", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( false, data.value().asObject() );
                transaction.nodeDoRemoveProperty( node, prop3.propertyKeyId() );
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
        transaction.nodeDoDelete( node );
    }

    private void deleteNode2( long node, StorageProperty prop1,
            StorageProperty prop2, StorageProperty prop3 )
            throws IOException
    {
        Map<Integer,Pair<StorageProperty,Long>> props = new HashMap<>();
        propertyLoader.nodeLoadProperties( node, newPropertyReceiver( props ) );
        int count = 0;
        for ( int keyId : props.keySet() )
        {
            long id = props.get( keyId ).other();
            PropertyRecord record = pStore.getRecord( id, pStore.newRecord(), NORMAL );
            PropertyBlock block = record.getPropertyBlock( props.get( keyId ).first().propertyKeyId() );
            StorageProperty data = block.newPropertyKeyValue( pStore );
            if ( data.propertyKeyId() == prop1.propertyKeyId() )
            {
                assertEquals( "prop1", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( "-string2", data.value().asObject() );
            }
            else if ( data.propertyKeyId() == prop2.propertyKeyId() )
            {
                assertEquals( "prop2", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( -2, data.value().asObject() );
            }
            else if ( data.propertyKeyId() == prop3.propertyKeyId() )
            {
                assertEquals( "prop3", MyPropertyKeyToken.getIndexFor(
                        keyId ).name() );
                assertEquals( true, data.value().asObject() );
                transaction.nodeDoRemoveProperty( node, prop3.propertyKeyId() );
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

        transaction.nodeDoDelete( node );
    }

    private void testGetRels( long[] relIds )
    {
        try ( StorageStatement statement = storeLayer.newStatement() )
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

    private void deleteRelationships( long nodeId )
    {
        try ( KernelStatement statement = (KernelStatement) tx.acquireStatement();
              Cursor<NodeItem> nodeCursor = statement.getStoreStatement().acquireSingleNodeCursor( nodeId ) )
        {
            assertTrue( nodeCursor.next() );
            NodeItem nodeItem = nodeCursor.get();
            statement.getStoreStatement()
                    .acquireNodeRelationshipCursor( nodeItem.isDense(), nodeItem.id(), nodeItem.nextRelationshipId(),
                            BOTH, ALWAYS_TRUE_INT ).forAll( rel -> relDelete( rel.id() ) );
        }
    }

    private GraphDatabaseService createTestDatabase( FileSystemAbstraction fileSystem, File storeDir )
    {
        return new TestGraphDatabaseFactory()
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fileSystem ) )
                .newImpermanentDatabase( storeDir );
    }
}
