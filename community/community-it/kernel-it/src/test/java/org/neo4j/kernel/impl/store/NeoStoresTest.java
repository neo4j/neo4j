/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.function.LongSupplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.indexed.IndexedIdGenerator;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.TransactionRecordState.PropertyReceiver;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.RelationshipVisitor;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.string.UTF8;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.counts_store_rotation_timeout;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat.FIELD_NOT_PRESENT;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

@EphemeralNeo4jLayoutExtension
@EphemeralPageCacheExtension
public class NeoStoresTest
{
    private static final NullLogProvider LOG_PROVIDER = NullLogProvider.getInstance();

    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory dir;
    @Inject
    private PageCache pageCache;
    @Inject
    private DatabaseLayout databaseLayout;

    private PropertyStore pStore;
    private NodeStore nodeStore;
    private Database database;
    private KernelTransaction tx;
    private TransactionState transactionState;
    private StorageReader storageReader;
    private TokenHolder propertyKeyTokenHolder;
    private DatabaseManagementService managementService;

    @BeforeEach
    public void setUpNeoStores()
    {
        Config config = Config.defaults();
        StoreFactory sf = getStoreFactory( config, databaseLayout, fs, NullLogProvider.getInstance() );
        sf.openAllNeoStores( true ).close();
        propertyKeyTokenHolder = new DelegatingTokenHolder( this::createPropertyKeyToken, TokenHolder.TYPE_PROPERTY_KEY );
    }

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    private int createPropertyKeyToken( String name, boolean internal )
    {
        return (int) nextId( PropertyKeyTokenRecord.class );
    }

    @Test
    void impossibleToGetStoreFromClosedNeoStoresContainer()
    {
        Config config = Config.defaults();
        StoreFactory sf = getStoreFactory( config, databaseLayout, fs, NullLogProvider.getInstance() );
        NeoStores neoStores = sf.openAllNeoStores( true );

        assertNotNull( neoStores.getMetaDataStore() );

        neoStores.close();

        var e = assertThrows( IllegalStateException.class, neoStores::getMetaDataStore );
        assertEquals( "Specified store was already closed.", e.getMessage() );
    }

    @Test
    void notAllowCreateDynamicStoreWithNegativeBlockSize()
    {
        Config config = Config.defaults();
        StoreFactory sf = getStoreFactory( config, databaseLayout, fs, NullLogProvider.getInstance() );

        var e = assertThrows( IllegalArgumentException.class, () ->
        {
            try ( NeoStores neoStores = sf.openNeoStores( true ) )
            {
                neoStores.createDynamicArrayStore( new File( "someStore" ), new File( "someIdFile" ), IdType.ARRAY_BLOCK, -2, NULL );
            }
        } );
        assertEquals( "Block size of dynamic array store should be positive integer.", e.getMessage() );
    }

    @Test
    void impossibleToGetNotRequestedStore()
    {
        Config config = Config.defaults();
        StoreFactory sf = getStoreFactory( config, databaseLayout, fs, NullLogProvider.getInstance() );

        var e = assertThrows( IllegalStateException.class, () ->
        {
            try ( NeoStores neoStores = sf.openNeoStores( true, StoreType.NODE_LABEL ) )
            {
                neoStores.getMetaDataStore();
            }
        } );
        assertEquals( "Specified store was not initialized. Please specify " + StoreType.META_DATA.name() +
                " as one of the stores types that should be open to be able to use it." , e.getMessage() );
    }

    private StorageProperty nodeAddProperty( long nodeId, int key, Object value )
    {
        StorageProperty property = new PropertyKeyValue( key, Values.of( value ) );
        StorageProperty oldProperty = null;
        try ( StorageNodeCursor nodeCursor = storageReader.allocateNodeCursor( NULL ) )
        {
            nodeCursor.single( nodeId );
            if ( nodeCursor.next() )
            {
                StorageProperty fetched = getProperty( key, nodeCursor.propertiesReference() );
                if ( fetched != null )
                {
                    oldProperty = fetched;
                }
            }
        }

        if ( oldProperty == null )
        {
            transactionState.nodeDoAddProperty( nodeId, key, property.value() );
        }
        else
        {
            transactionState.nodeDoChangeProperty( nodeId, key, property.value() );
        }
        return property;
    }

    private StorageProperty getProperty( int key, long propertyId )
    {
        try ( StoragePropertyCursor propertyCursor = storageReader.allocatePropertyCursor( NULL ) )
        {
            propertyCursor.initNodeProperties( propertyId );
            if ( propertyCursor.next() )
            {
                Value oldValue = propertyCursor.propertyValue();
                if ( oldValue != null )
                {
                    return new PropertyKeyValue( key, oldValue );
                }
            }
        }
        return null;
    }

    @Test
    void testRels1() throws Exception
    {
        reinitializeStores( databaseLayout );
        startTx();
        int relType1 = (int) nextId( RelationshipType.class );
        String typeName = "relationshiptype1";
        transactionState.relationshipTypeDoCreateForName( typeName, false, relType1 );
        long[] nodeIds = new long[3];
        for ( int i = 0; i < 3; i++ )
        {
            nodeIds[i] = nextId( Node.class );
            transactionState.nodeDoCreate( nodeIds[i] );
            nodeAddProperty( nodeIds[i], index( "nisse" ), 10 - i );
        }
        for ( int i = 0; i < 2; i++ )
        {
            transactionState.relationshipDoCreate( nextId( Relationship.class ),
                    relType1, nodeIds[i], nodeIds[i + 1] );
        }
        commitTx();
        startTx();
        for ( int i = 0; i < 3; i += 2 )
        {
            deleteRelationships( nodeIds[i] );
            transactionState.nodeDoDelete( nodeIds[i] );
        }
        commitTx();
    }

    private void relDelete( long id )
    {
        RelationshipVisitor<RuntimeException> visitor = ( relId, type, startNode, endNode ) ->
                transactionState.relationshipDoDelete( relId, type, startNode, endNode );
        if ( !transactionState.relationshipVisit( id, visitor ) )
        {
            try ( StorageRelationshipScanCursor cursor = storageReader.allocateRelationshipScanCursor( NULL ) )
            {
                cursor.single( id );
                if ( !cursor.next() )
                {
                    throw new RuntimeException( "Relationship " + id + " not found" );
                }
                visitor.visit( id, cursor.type(), cursor.sourceNodeReference(), cursor.targetNodeReference() );
            }
        }
    }

    @Test
    void testRels2() throws Exception
    {
        reinitializeStores( databaseLayout );
        startTx();
        int relType1 = (int) nextId( RelationshipType.class );
        String typeName = "relationshiptype1";
        transactionState.relationshipTypeDoCreateForName( typeName, false, relType1 );
        long[] nodeIds = new long[3];
        for ( int i = 0; i < 3; i++ )
        {
            nodeIds[i] = nextId( Node.class );
            transactionState.nodeDoCreate( nodeIds[i] );
            nodeAddProperty( nodeIds[i], index( "nisse" ), 10 - i );
        }
        for ( int i = 0; i < 2; i++ )
        {
            transactionState.relationshipDoCreate( nextId( Relationship.class ),
                    relType1, nodeIds[i], nodeIds[i + 1] );
        }
        transactionState.relationshipDoCreate( nextId( Relationship.class ),
                relType1, nodeIds[0], nodeIds[2] );
        commitTx();
        startTx();
        for ( int i = 0; i < 3; i++ )
        {
            deleteRelationships( nodeIds[i] );
            transactionState.nodeDoDelete( nodeIds[i] );
        }
        commitTx();
    }

    @Test
    void testRels3() throws Exception
    {
        // test linked list stuff during relationship delete
        reinitializeStores( databaseLayout );
        startTx();
        int relType1 = (int) nextId( RelationshipType.class );
        transactionState.relationshipTypeDoCreateForName( "relationshiptype1", false, relType1 );
        long[] nodeIds = new long[8];
        for ( int i = 0; i < nodeIds.length; i++ )
        {
            nodeIds[i] = nextId( Node.class );
            transactionState.nodeDoCreate( nodeIds[i] );
        }
        for ( int i = 0; i < nodeIds.length / 2; i++ )
        {
            transactionState.relationshipDoCreate( nextId( Relationship.class ),
                    relType1, nodeIds[i], nodeIds[i * 2] );
        }
        long rel5 = nextId( Relationship.class );
        transactionState.relationshipDoCreate( rel5, relType1, nodeIds[0], nodeIds[5] );
        long rel2 = nextId( Relationship.class );
        transactionState.relationshipDoCreate( rel2, relType1, nodeIds[1], nodeIds[2] );
        long rel3 = nextId( Relationship.class );
        transactionState.relationshipDoCreate( rel3, relType1, nodeIds[1], nodeIds[3] );
        long rel6 = nextId( Relationship.class );
        transactionState.relationshipDoCreate( rel6, relType1, nodeIds[1], nodeIds[6] );
        long rel1 = nextId( Relationship.class );
        transactionState.relationshipDoCreate( rel1, relType1, nodeIds[0], nodeIds[1] );
        long rel4 = nextId( Relationship.class );
        transactionState.relationshipDoCreate( rel4, relType1, nodeIds[0], nodeIds[4] );
        long rel7 = nextId( Relationship.class );
        transactionState.relationshipDoCreate( rel7, relType1, nodeIds[0], nodeIds[7] );
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
    }

    @Test
    void setVersion() throws Exception
    {
        File storeDir = dir.homeDir();
        createShutdownTestDatabase( fs, storeDir );
        assertEquals( 0, MetaDataStore.setRecord( pageCache, databaseLayout.metadataStore(), Position.LOG_VERSION, 10, NULL ) );
        assertEquals( 10, MetaDataStore.setRecord( pageCache, databaseLayout.metadataStore(), Position.LOG_VERSION, 12, NULL ) );

        Config config = Config.defaults();
        StoreFactory sf = getStoreFactory( config, databaseLayout, fs, LOG_PROVIDER );

        NeoStores neoStores = sf.openAllNeoStores();
        assertEquals( 12, neoStores.getMetaDataStore().getCurrentLogVersion() );
        neoStores.close();
    }

    @Test
    void shouldNotReadNonRecordDataAsRecord() throws Exception
    {
        StoreFactory factory = newStoreFactory( databaseLayout, pageCache, fs );
        long recordVersion = defaultStoreVersion();
        try ( NeoStores neoStores = factory.openAllNeoStores( true ) )
        {
            MetaDataStore metaDataStore = neoStores.getMetaDataStore();
            metaDataStore.setCreationTime( 3, NULL );
            metaDataStore.setRandomNumber( 4, NULL );
            metaDataStore.setCurrentLogVersion( 5, NULL );
            metaDataStore.setLastCommittedAndClosedTransactionId( 6, 0, 0, 43, 44, NULL );
            metaDataStore.setStoreVersion( recordVersion, NULL );

            metaDataStore.setLatestConstraintIntroducingTx( 9, NULL );
        }

        File file = databaseLayout.metadataStore();
        try ( StoreChannel channel = fs.write( file ) )
        {
            channel.position( 0 );
            channel.write( ByteBuffer.wrap( UTF8.encode( "This is some data that is not a record." ) ) );
        }

        MetaDataStore.setRecord( pageCache, file, Position.STORE_VERSION, recordVersion, NULL );

        try ( NeoStores neoStores = factory.openAllNeoStores() )
        {
            MetaDataStore metaDataStore = neoStores.getMetaDataStore();
            assertEquals( FIELD_NOT_PRESENT, metaDataStore.getCreationTime() );
            assertEquals( FIELD_NOT_PRESENT, metaDataStore.getRandomNumber() );
            assertEquals( FIELD_NOT_PRESENT, metaDataStore.getCurrentLogVersion() );
            assertEquals( FIELD_NOT_PRESENT, metaDataStore.getLastCommittedTransactionId() );
            assertEquals( FIELD_NOT_PRESENT, metaDataStore.getLastClosedTransactionId() );
            assertEquals( recordVersion, metaDataStore.getStoreVersion() );
            assertEquals( 9, metaDataStore.getLatestConstraintIntroducingTx() );
            assertArrayEquals( new long[]{FIELD_NOT_PRESENT, 44, 43}, metaDataStore.getLastClosedTransaction() );
        }
    }

    @Test
    void testSetLatestConstraintTx() throws IOException
    {
        // given
        Config config = Config.defaults();
        StoreFactory sf = new StoreFactory( databaseLayout, config, new DefaultIdGeneratorFactory( fs, immediate() ),
                pageCache, fs, LOG_PROVIDER, PageCacheTracer.NULL );

        // when
        NeoStores neoStores = sf.openAllNeoStores( true );
        MetaDataStore metaDataStore = neoStores.getMetaDataStore();

        // then the default is 0
        assertEquals( 0L, metaDataStore.getLatestConstraintIntroducingTx() );

        // when
        metaDataStore.setLatestConstraintIntroducingTx( 10L, NULL );

        // then
        assertEquals( 10L, metaDataStore.getLatestConstraintIntroducingTx() );

        // when
        neoStores.flush( IOLimiter.UNLIMITED, NULL );
        neoStores.close();
        neoStores = sf.openAllNeoStores();

        // then the value should have been stored
        assertEquals( 10L, neoStores.getMetaDataStore().getLatestConstraintIntroducingTx() );
        neoStores.close();
    }

    @Test
    void shouldInitializeTheTxIdToOne()
    {
        StoreFactory factory = getStoreFactory( Config.defaults(), databaseLayout, fs, LOG_PROVIDER );
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
    void shouldThrowUnderlyingStorageExceptionWhenFailingToLoadStorage()
    {
        FileSystemAbstraction fileSystem = fs;
        StoreFactory factory = getStoreFactory( Config.defaults(), databaseLayout, fileSystem, LOG_PROVIDER );

        try ( NeoStores neoStores = factory.openAllNeoStores( true ) )
        {
            neoStores.getMetaDataStore();
        }
        File file = databaseLayout.metadataStore();
        fileSystem.deleteFile( file );

        assertThrows( StoreNotFoundException.class, () ->
        {
            try ( NeoStores neoStores = factory.openAllNeoStores() )
            {
                neoStores.getMetaDataStore();
            }
        } );
    }

    @Test
    void shouldAddUpgradeFieldsToTheNeoStoreIfNotPresent() throws IOException
    {
        StoreFactory factory = newStoreFactory( databaseLayout, pageCache, fs );
        long recordVersion = defaultStoreVersion();
        try ( NeoStores neoStores = factory.openAllNeoStores( true ) )
        {
            MetaDataStore metaDataStore = neoStores.getMetaDataStore();
            metaDataStore.setCreationTime( 3, NULL );
            metaDataStore.setRandomNumber( 4, NULL );
            metaDataStore.setCurrentLogVersion( 5, NULL );
            metaDataStore.setLastCommittedAndClosedTransactionId( 6, 42, BASE_TX_COMMIT_TIMESTAMP, 43, 44, NULL );
            metaDataStore.setStoreVersion( recordVersion, NULL );

            metaDataStore.setLatestConstraintIntroducingTx( 9, NULL );
        }

        File file = databaseLayout.metadataStore();

        assertNotEquals( 10, MetaDataStore.getRecord( pageCache, file, Position.UPGRADE_TRANSACTION_ID, NULL ) );
        assertNotEquals( 11, MetaDataStore.getRecord( pageCache, file, Position.UPGRADE_TRANSACTION_CHECKSUM, NULL ) );

        MetaDataStore.setRecord( pageCache, file, Position.UPGRADE_TRANSACTION_ID, 10, NULL );
        MetaDataStore.setRecord( pageCache, file, Position.UPGRADE_TRANSACTION_CHECKSUM, 11, NULL );
        MetaDataStore.setRecord( pageCache, file, Position.UPGRADE_TIME, 12, NULL );

        try ( NeoStores neoStores = factory.openAllNeoStores() )
        {
            MetaDataStore metaDataStore = neoStores.getMetaDataStore();
            assertEquals( 3, metaDataStore.getCreationTime() );
            assertEquals( 4, metaDataStore.getRandomNumber() );
            assertEquals( 5, metaDataStore.getCurrentLogVersion() );
            assertEquals( 6, metaDataStore.getLastCommittedTransactionId() );
            assertEquals( recordVersion, metaDataStore.getStoreVersion() );
            assertEquals( 9, metaDataStore.getLatestConstraintIntroducingTx() );
            assertEquals( new TransactionId( 10, 11, BASE_TX_COMMIT_TIMESTAMP ),
                    metaDataStore.getUpgradeTransaction() );
            assertEquals( 12, metaDataStore.getUpgradeTime() );
            assertArrayEquals( new long[]{6, 44, 43}, metaDataStore.getLastClosedTransaction() );
        }

        MetaDataStore.setRecord( pageCache, file, Position.UPGRADE_TRANSACTION_COMMIT_TIMESTAMP, 13, NULL );

        try ( NeoStores neoStores = factory.openAllNeoStores() )
        {
            MetaDataStore metaDataStore = neoStores.getMetaDataStore();
            assertEquals( new TransactionId( 10, 11, 13 ),
                    metaDataStore.getUpgradeTransaction() );
        }
    }

    @Test
    void shouldSetHighestTransactionIdWhenNeeded()
    {
        // GIVEN
        StoreFactory factory = getStoreFactory( Config.defaults(), databaseLayout, fs, LOG_PROVIDER );

        try ( NeoStores neoStore = factory.openAllNeoStores( true ) )
        {
            MetaDataStore store = neoStore.getMetaDataStore();
            store.setLastCommittedAndClosedTransactionId( 40, 4444, BASE_TX_COMMIT_TIMESTAMP,
                    CURRENT_FORMAT_LOG_HEADER_SIZE, 0, NULL );

            // WHEN
            store.transactionCommitted( 42, 6666, BASE_TX_COMMIT_TIMESTAMP, NULL );

            // THEN
            assertEquals( new TransactionId( 42, 6666, BASE_TX_COMMIT_TIMESTAMP ),
                    store.getLastCommittedTransaction() );
            assertArrayEquals( new long[]{40, 0, CURRENT_FORMAT_LOG_HEADER_SIZE}, store.getLastClosedTransaction() );
        }
    }

    @Test
    void tracePageCacheAccessOnTransactionCloseCall()
    {
        StoreFactory factory = getStoreFactory( Config.defaults(), databaseLayout, fs, LOG_PROVIDER );

        try ( NeoStores neoStore = factory.openAllNeoStores( true ) )
        {
            MetaDataStore store = neoStore.getMetaDataStore();
            var cacheTracer = new DefaultPageCacheTracer();
            var cursorTracer = cacheTracer.createPageCursorTracer( "tracePageCacheAccessOnTransactionCloseCall" );
            store.transactionClosed( 0, 6666, 15, cursorTracer );

            assertEquals( 1, cursorTracer.pins() );
            assertEquals( 1, cursorTracer.hits() );
            assertEquals( 1, cursorTracer.unpins() );
        }
    }

    @Test
    void tracePageCacheAccessOnTransactionCommittedCall()
    {
        StoreFactory factory = getStoreFactory( Config.defaults(), databaseLayout, fs, LOG_PROVIDER );

        try ( NeoStores neoStore = factory.openAllNeoStores( true ) )
        {
            MetaDataStore store = neoStore.getMetaDataStore();
            var cacheTracer = new DefaultPageCacheTracer();
            var cursorTracer = cacheTracer.createPageCursorTracer( "tracePageCacheAccessOnTransactionCommittedCall" );
            store.transactionCommitted( 42, 6666, BASE_TX_COMMIT_TIMESTAMP, cursorTracer );

            assertEquals( 1, cursorTracer.pins() );
            assertEquals( 1, cursorTracer.hits() );
            assertEquals( 1, cursorTracer.unpins() );
        }
    }

    @Test
    void shouldNotSetHighestTransactionIdWhenNeeded()
    {
        // GIVEN
        StoreFactory factory = getStoreFactory( Config.defaults(), databaseLayout, fs, LOG_PROVIDER );

        try ( NeoStores neoStore = factory.openAllNeoStores( true ) )
        {
            MetaDataStore store = neoStore.getMetaDataStore();
            store.setLastCommittedAndClosedTransactionId( 40, 4444, BASE_TX_COMMIT_TIMESTAMP,
                    CURRENT_FORMAT_LOG_HEADER_SIZE, 0, NULL );

            // WHEN
            store.transactionCommitted( 39, 3333, BASE_TX_COMMIT_TIMESTAMP, NULL );

            // THEN
            assertEquals( new TransactionId( 40, 4444, BASE_TX_COMMIT_TIMESTAMP ),
                    store.getLastCommittedTransaction() );
            assertArrayEquals( new long[]{40, 0, CURRENT_FORMAT_LOG_HEADER_SIZE}, store.getLastClosedTransaction() );
        }
    }

    @Test
    void shouldCloseAllTheStoreEvenIfExceptionsAreThrown()
    {
        // given
        Config defaults = Config.defaults( counts_store_rotation_timeout, Duration.ofMinutes( 60 ) );
        String errorMessage = "Failing for the heck of it";
        StoreFactory factory = new StoreFactory( databaseLayout, defaults, new CloseFailingDefaultIdGeneratorFactory( fs, errorMessage ), pageCache,
                fs, NullLogProvider.getInstance(), PageCacheTracer.NULL );
        NeoStores neoStore = factory.openAllNeoStores( true );

        var ex = assertThrows( UnderlyingStorageException.class, neoStore::close );
        assertEquals( errorMessage, ex.getCause().getMessage() );
    }

    @Test
    void isPresentAfterCreatingAllStores() throws Exception
    {
        // given
        fs.deleteRecursively( databaseLayout.databaseDirectory() );
        DefaultIdGeneratorFactory idFactory = new DefaultIdGeneratorFactory( fs, immediate() );
        StoreFactory factory = new StoreFactory( databaseLayout, Config.defaults(), idFactory, pageCache, fs, LOG_PROVIDER, PageCacheTracer.NULL );

        // when
        try ( NeoStores ignore = factory.openAllNeoStores( true ) )
        {
            // then
            assertTrue( NeoStores.isStorePresent( fs, databaseLayout ) );
        }
    }

    @Test
    void isPresentFalseAfterCreatingAllButLastStoreType() throws Exception
    {
        // given
        fs.deleteRecursively( databaseLayout.databaseDirectory() );
        DefaultIdGeneratorFactory idFactory = new DefaultIdGeneratorFactory( fs, immediate() );
        StoreFactory factory = new StoreFactory( databaseLayout, Config.defaults(), idFactory, pageCache, fs, LOG_PROVIDER, PageCacheTracer.NULL );
        StoreType[] allStoreTypes = StoreType.values();
        StoreType[] allButLastStoreTypes = Arrays.copyOf( allStoreTypes, allStoreTypes.length - 1 );

        // when
        try ( NeoStores ignore = factory.openNeoStores( true, allButLastStoreTypes ) )
        {
            // then
            assertFalse( NeoStores.isStorePresent( fs, databaseLayout ) );
        }
    }

    private static long defaultStoreVersion()
    {
        return MetaDataStore.versionStringToLong( RecordFormatSelector.defaultFormat().storeVersion() );
    }

    private static StoreFactory newStoreFactory( DatabaseLayout databaseLayout, PageCache pageCache, FileSystemAbstraction fs )
    {
        RecordFormats recordFormats = RecordFormatSelector.defaultFormat();
        Config config = Config.defaults();
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs, immediate() );
        return new StoreFactory( databaseLayout, config, idGeneratorFactory, pageCache, fs, recordFormats, LOG_PROVIDER, PageCacheTracer.NULL,
                immutable.empty() );
    }

    private void reinitializeStores( DatabaseLayout databaseLayout )
    {
        Dependencies dependencies = new Dependencies();
        Config config = Config.defaults( GraphDatabaseSettings.fail_on_missing_files, false );
        dependencies.satisfyDependency( config );
        if ( managementService != null )
        {
            managementService.shutdown();
        }
        managementService = new TestDatabaseManagementServiceBuilder().setFileSystem( fs ).setExternalDependencies( dependencies )
                .setDatabaseRootDirectory( databaseLayout.databaseDirectory() ).build();
        final GraphDatabaseAPI databaseAPI = (GraphDatabaseAPI) managementService.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        database = databaseAPI.getDependencyResolver().resolveDependency( Database.class );

        NeoStores neoStores = database.getDependencyResolver()
                .resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        pStore = neoStores.getPropertyStore();
        nodeStore = neoStores.getNodeStore();
        storageReader = database.getDependencyResolver().resolveDependency( StorageEngine.class ).newReader();
    }

    private void startTx() throws TransactionFailureException
    {
        tx = database.getKernel().beginTransaction( KernelTransaction.Type.IMPLICIT, AUTH_DISABLED );
        transactionState = ((KernelTransactionImplementation) tx).txState();
    }

    private void commitTx() throws TransactionFailureException
    {
        tx.commit();
    }

    private int index( String key ) throws KernelException
    {
        return propertyKeyTokenHolder.getOrCreateId( key );
    }

    private long nextId( Class<?> clazz )
    {
        NeoStores neoStores = database.getDependencyResolver()
                .resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        if ( clazz.equals( PropertyKeyTokenRecord.class ) )
        {
            return neoStores.getPropertyKeyTokenStore().nextId( NULL );
        }
        if ( clazz.equals( RelationshipType.class ) )
        {
            return neoStores.getRelationshipTypeTokenStore().nextId( NULL );
        }
        if ( clazz.equals( Node.class ) )
        {
            return neoStores.getNodeStore().nextId( NULL );
        }
        if ( clazz.equals( Relationship.class ) )
        {
            return neoStores.getRelationshipStore().nextId( NULL );
        }
        throw new IllegalArgumentException( clazz.getName() );
    }

    private StorageRelationshipTraversalCursor allocateRelationshipTraversalCursor( StorageNodeCursor node )
    {
        StorageRelationshipTraversalCursor relationships = storageReader.allocateRelationshipTraversalCursor( NULL );
        node.relationships( relationships, ALL_RELATIONSHIPS );
        return relationships;
    }

    private StorageNodeCursor allocateNodeCursor( long nodeId )
    {
        StorageNodeCursor nodeCursor = storageReader.allocateNodeCursor( NULL );
        nodeCursor.single( nodeId );
        return nodeCursor;
    }

    private void deleteRelationships( long nodeId )
    {
        try ( StorageNodeCursor nodeCursor = allocateNodeCursor( nodeId ) )
        {
            assertTrue( nodeCursor.next() );
            try ( StorageRelationshipTraversalCursor relationships = allocateRelationshipTraversalCursor( nodeCursor ) )
            {
                while ( relationships.next() )
                {
                    relDelete( relationships.entityReference() );
                }
            }
        }
    }

    private static void createShutdownTestDatabase( FileSystemAbstraction fileSystem, File storeDir )
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( storeDir )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fileSystem ) )
                .impermanent()
                .build();
        managementService.shutdown();
    }

    private <RECEIVER extends PropertyReceiver<PropertyKeyValue>> void nodeLoadProperties( long nodeId, RECEIVER receiver )
    {
        NodeRecord nodeRecord = nodeStore.getRecord( nodeId, nodeStore.newRecord(), NORMAL, NULL );
        loadProperties( nodeRecord.getNextProp(), receiver );
    }

    private <RECEIVER extends PropertyReceiver<PropertyKeyValue>> void loadProperties( long nextProp, RECEIVER receiver )
    {
        PropertyRecord record = pStore.newRecord();
        while ( !Record.NULL_REFERENCE.is( nextProp ) )
        {
            pStore.getRecord( nextProp, record, NORMAL, NULL );
            for ( PropertyBlock propBlock : record )
            {
                receiver.receive( propBlock.newPropertyKeyValue( pStore, NULL ), record.getId() );
            }
            nextProp = record.getNextProp();
        }
    }

    private StoreFactory getStoreFactory( Config config, DatabaseLayout databaseLayout, FileSystemAbstraction fs,
            NullLogProvider logProvider )
    {
        return new StoreFactory( databaseLayout, config, new DefaultIdGeneratorFactory( fs, immediate() ), pageCache, fs, logProvider, PageCacheTracer.NULL );
    }

    private static class CloseFailingDefaultIdGeneratorFactory extends DefaultIdGeneratorFactory
    {
        private final String errorMessage;

        CloseFailingDefaultIdGeneratorFactory( FileSystemAbstraction fs, String errorMessage )
        {
            super( fs, immediate() );
            this.errorMessage = errorMessage;
        }

        @Override
        protected IndexedIdGenerator instantiate( FileSystemAbstraction fs, PageCache pageCache, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
                File fileName, LongSupplier highIdSupplier, long maxValue, IdType idType, boolean readOnly, PageCursorTracer cursorTracer,
                ImmutableSet<OpenOption> openOptions )
        {
            if ( idType == IdType.NODE )
            {
                // Return a special id generator which will throw exception on close
                return new IndexedIdGenerator( pageCache, fileName, immediate(), idType, allowLargeIdCaches, () -> 6 * 7, maxValue, readOnly, cursorTracer )
                {
                    @Override
                    public synchronized void close()
                    {
                        super.close();
                        throw new IllegalStateException( errorMessage );
                    }
                };
            }
            return super.instantiate( fs, pageCache, recoveryCleanupWorkCollector, fileName, highIdSupplier, maxValue, idType, readOnly, cursorTracer,
                    openOptions );
        }
    }
}
