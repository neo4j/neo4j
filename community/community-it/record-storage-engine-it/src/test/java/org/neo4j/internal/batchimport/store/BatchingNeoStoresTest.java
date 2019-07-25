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
package org.neo4j.internal.batchimport.store;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.id.DefaultIdController;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.format.ForcedSecondaryUnitRecordFormats;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.lock.LockService;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.monitoring.DatabaseEventListeners;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.DatabasePanicEventGenerator;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.storable.Values;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.internal.batchimport.store.BatchingNeoStores.DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD;
import static org.neo4j.internal.batchimport.store.BatchingNeoStores.batchingNeoStores;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForConfig;
import static org.neo4j.kernel.impl.store.format.standard.Standard.LATEST_RECORD_FORMATS;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

@PageCacheExtension
class BatchingNeoStoresTest
{
    private static final RelationshipType RELTYPE = RelationshipType.withName( "TEST" );

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private PageCache pageCache;
    @Inject
    private FileSystemAbstraction fileSystem;

    @Test
    void shouldNotOpenStoreWithNodesOrRelationshipsInIt() throws Exception
    {
        // GIVEN
        someDataInTheDatabase();

        // WHEN
        IllegalStateException exception = assertThrows( IllegalStateException.class, () ->
        {
            try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler() )
            {
                RecordFormats recordFormats = selectForConfig( Config.defaults(), NullLogProvider.getInstance() );
                try ( BatchingNeoStores store = batchingNeoStores( fileSystem, testDirectory.databaseLayout(), recordFormats, Configuration.DEFAULT,
                        NullLogService.getInstance(), EMPTY, Config.defaults(), jobScheduler ) )
                {
                    store.createNew();
                }
            }
        } );
        assertThat( exception.getMessage(), containsString( "already contains" ) );
    }

    @Test
    void shouldRespectDbConfig() throws Exception
    {
        // GIVEN
        int size = 10;
        Config config = Config.newBuilder()
                .set( GraphDatabaseSettings.array_block_size, size )
                .set( GraphDatabaseSettings.string_block_size, size )
                .build();

        // WHEN
        RecordFormats recordFormats = LATEST_RECORD_FORMATS;
        int headerSize = recordFormats.dynamic().getRecordHeaderSize();
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler();
              BatchingNeoStores store = batchingNeoStores( fileSystem, testDirectory.databaseLayout(),
              recordFormats, Configuration.DEFAULT, NullLogService.getInstance(), EMPTY, config, jobScheduler ) )
        {
            store.createNew();

            // THEN
            assertEquals( size + headerSize, store.getPropertyStore().getArrayStore().getRecordSize() );
            assertEquals( size + headerSize, store.getPropertyStore().getStringStore().getRecordSize() );
        }
    }

    @Test
    void shouldPruneAndOpenExistingDatabase() throws Exception
    {
        // given
        for ( StoreType typeToTest : relevantRecordStores() )
        {
            // given all the stores with some records in them
            testDirectory.cleanup();
            try ( BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( fileSystem, pageCache,
                    PageCacheTracer.NULL, testDirectory.databaseLayout(), LATEST_RECORD_FORMATS, Configuration.DEFAULT, NullLogService.getInstance(), EMPTY,
                    Config.defaults() ) )
            {
                stores.createNew();
                for ( StoreType type : relevantRecordStores() )
                {
                    createRecordIn( stores.getNeoStores().getRecordStore( type ) );
                }
            }

            // when opening and pruning all except the one we test
            try ( BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( fileSystem, pageCache,
                    PageCacheTracer.NULL, testDirectory.databaseLayout(), LATEST_RECORD_FORMATS, Configuration.DEFAULT, NullLogService.getInstance(), EMPTY,
                    Config.defaults() ) )
            {
                stores.pruneAndOpenExistingStore( type -> type == typeToTest, Predicates.alwaysFalse() );

                // then only the one we kept should have data in it
                for ( StoreType type : relevantRecordStores() )
                {
                    RecordStore<AbstractBaseRecord> store = stores.getNeoStores().getRecordStore( type );
                    if ( type == typeToTest )
                    {
                        assertThat( store.toString(), (int) store.getHighId(), greaterThan( store.getNumberOfReservedLowIds() ) );
                    }
                    else
                    {
                        assertEquals( store.getNumberOfReservedLowIds(), store.getHighId(), store.toString() );
                    }
                }
            }
        }
    }

    @Test
    void shouldDecideToAllocateDoubleRelationshipRecordUnitsOnLargeAmountOfRelationshipsOnSupportedFormat() throws Exception
    {
        // given
        RecordFormats formats = new ForcedSecondaryUnitRecordFormats( LATEST_RECORD_FORMATS );
        try ( BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( fileSystem,
                pageCache, PageCacheTracer.NULL, testDirectory.databaseLayout(), formats, Configuration.DEFAULT,
                NullLogService.getInstance(), EMPTY, Config.defaults() ) )
        {
            stores.createNew();
            Input.Estimates estimates = Input.knownEstimates( 0, DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD << 1, 0, 0, 0, 0, 0 );

            // when
            boolean doubleUnits = stores.determineDoubleRelationshipRecordUnits( estimates );

            // then
            assertTrue( doubleUnits );
        }
    }

    @Test
    void shouldNotDecideToAllocateDoubleRelationshipRecordUnitsonLowAmountOfRelationshipsOnSupportedFormat() throws Exception
    {
        // given
        RecordFormats formats = new ForcedSecondaryUnitRecordFormats( LATEST_RECORD_FORMATS );
        try ( BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( fileSystem,
                pageCache, PageCacheTracer.NULL, testDirectory.databaseLayout(), formats, Configuration.DEFAULT,
                NullLogService.getInstance(), EMPTY, Config.defaults() ) )
        {
            stores.createNew();
            Input.Estimates estimates = Input.knownEstimates( 0, DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD >> 1, 0, 0, 0, 0, 0 );

            // when
            boolean doubleUnits = stores.determineDoubleRelationshipRecordUnits( estimates );

            // then
            assertFalse( doubleUnits );
        }
    }

    @Test
    void shouldNotDecideToAllocateDoubleRelationshipRecordUnitsonLargeAmountOfRelationshipsOnUnsupportedFormat() throws Exception
    {
        // given
        RecordFormats formats = LATEST_RECORD_FORMATS;
        try ( BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( fileSystem,
                pageCache, PageCacheTracer.NULL, testDirectory.databaseLayout(), formats, Configuration.DEFAULT,
                NullLogService.getInstance(), EMPTY, Config.defaults() ) )
        {
            stores.createNew();
            Input.Estimates estimates = Input.knownEstimates( 0, DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD << 1, 0, 0, 0, 0, 0 );

            // when
            boolean doubleUnits = stores.determineDoubleRelationshipRecordUnits( estimates );

            // then
            assertFalse( doubleUnits );
        }
    }

    private StoreType[] relevantRecordStores()
    {
        return Stream.of( StoreType.values() )
                .filter( type -> type != StoreType.META_DATA ).toArray( StoreType[]::new );
    }

    private static <RECORD extends AbstractBaseRecord> void createRecordIn( RecordStore<RECORD> store )
    {
        RECORD record = store.newRecord();
        record.setId( store.nextId() );
        record.setInUse( true );
        if ( record instanceof PropertyRecord )
        {
            // Special hack for property store, since it's not enough to simply set a record as in use there
            PropertyBlock block = new PropertyBlock();
            ((PropertyStore)store).encodeValue( block, 0, Values.of( 10 ) );
            ((PropertyRecord) record).addPropertyBlock( block );
        }
        store.updateRecord( record );
    }

    private void someDataInTheDatabase() throws Exception
    {
        NullLog nullLog = NullLog.getInstance();
        try ( JobScheduler scheduler = JobSchedulerFactory.createInitialisedScheduler();
                PageCache pageCache = new ConfiguringPageCacheFactory( fileSystem, Config.defaults(), PageCacheTracer.NULL, PageCursorTracerSupplier.NULL,
                        nullLog, EmptyVersionContextSupplier.EMPTY, scheduler ).getOrCreatePageCache();
                Lifespan life = new Lifespan() )
        {
            // TODO this little dance with TokenHolders is really annoying and must be solved with a better abstraction
            DeferredInitializedTokenCreator propertyKeyTokenCreator = new DeferredInitializedTokenCreator()
            {
                @Override
                void create( String name, boolean internal, int id )
                {
                    txState.propertyKeyDoCreateForName( name, internal, id );
                }
            };
            DeferredInitializedTokenCreator labelTokenCreator = new DeferredInitializedTokenCreator()
            {
                @Override
                void create( String name, boolean internal, int id )
                {
                    txState.labelDoCreateForName( name, internal, id );
                }
            };
            DeferredInitializedTokenCreator relationshipTypeTokenCreator = new DeferredInitializedTokenCreator()
            {
                @Override
                void create( String name, boolean internal, int id )
                {
                    txState.relationshipTypeDoCreateForName( name, internal, id );
                }
            };
            TokenHolders tokenHolders = new TokenHolders(
                    new DelegatingTokenHolder( propertyKeyTokenCreator, TokenHolder.TYPE_PROPERTY_KEY ),
                    new DelegatingTokenHolder( labelTokenCreator, TokenHolder.TYPE_LABEL ),
                    new DelegatingTokenHolder( relationshipTypeTokenCreator, TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
            IndexConfigCompleter indexConfigCompleter = index -> index;
            RecordStorageEngine storageEngine = life.add(
                    new RecordStorageEngine( testDirectory.databaseLayout(), Config.defaults(), pageCache, fileSystem, NullLogProvider.getInstance(),
                            tokenHolders, new DatabaseSchemaState( NullLogProvider.getInstance() ),
                            new StandardConstraintSemantics(), indexConfigCompleter, LockService.NO_LOCK_SERVICE,
                            new DatabaseHealth( new DatabasePanicEventGenerator( new DatabaseEventListeners( nullLog ), DEFAULT_DATABASE_NAME ), nullLog ),
                            new DefaultIdGeneratorFactory( fileSystem, immediate() ), new DefaultIdController(),
                            EmptyVersionContextSupplier.EMPTY, true ) );
            // Create the relationship type token
            TxState txState = new TxState();
            NeoStores neoStores = storageEngine.testAccessNeoStores();
            CommandCreationContext commandCreationContext = storageEngine.newCommandCreationContext();
            propertyKeyTokenCreator.initialize( neoStores.getPropertyKeyTokenStore(), txState );
            labelTokenCreator.initialize( neoStores.getLabelTokenStore(), txState );
            relationshipTypeTokenCreator.initialize( neoStores.getRelationshipTypeTokenStore(), txState );
            int relTypeId = tokenHolders.relationshipTypeTokens().getOrCreateId( RELTYPE.name() );
            apply( txState, commandCreationContext, storageEngine );

            // Finally, we're initialized and ready to create two nodes and a relationship
            txState = new TxState();
            long node1 = commandCreationContext.reserveNode();
            long node2 = commandCreationContext.reserveNode();
            txState.nodeDoCreate( node1 );
            txState.nodeDoCreate( node2 );
            txState.relationshipDoCreate( commandCreationContext.reserveRelationship(), relTypeId, node1, node2 );
            apply( txState, commandCreationContext, storageEngine );
            neoStores.flush( IOLimiter.UNLIMITED );
        }
    }

    private void apply( TxState txState, CommandCreationContext commandCreationContext, RecordStorageEngine storageEngine ) throws Exception
    {
        List<StorageCommand> commands = new ArrayList<>();
        try ( RecordStorageReader storageReader = storageEngine.newReader() )
        {
            storageEngine.createCommands( commands, txState, storageReader, commandCreationContext, ResourceLocker.IGNORE, BASE_TX_ID, v -> v );
            CommandsToApply apply = new TransactionToApply( new PhysicalTransactionRepresentation( commands, new byte[0], 0, 0, 0, 0, 0, 0 ) );
            storageEngine.apply( apply, TransactionApplicationMode.INTERNAL );
        }
    }

    private abstract static class DeferredInitializedTokenCreator implements TokenCreator
    {
        TokenStore store;
        TransactionState txState;

        void initialize( TokenStore store, TransactionState txState )
        {
            this.store = store;
            this.txState = txState;
        }

        @Override
        public int createToken( String name, boolean internal )
        {
            int id = (int) store.nextId();
            create( name, internal, id );
            return id;
        }

        abstract void create( String name, boolean internal, int id );
    }
}
