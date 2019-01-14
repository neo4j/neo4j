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
package org.neo4j.unsafe.impl.batchimport.store;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.stream.Stream;

import org.neo4j.function.Predicates;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.ForcedSecondaryUnitRecordFormats;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.unsafe.impl.batchimport.input.Input.Estimates;
import org.neo4j.unsafe.impl.batchimport.input.Inputs;
import org.neo4j.values.storable.Values;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.store.format.standard.Standard.LATEST_RECORD_FORMATS;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores.DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD;

public class BatchingNeoStoresTest
{
    @Rule
    public final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule();

    @Test
    public void shouldNotOpenStoreWithNodesOrRelationshipsInIt() throws Exception
    {
        // GIVEN
        someDataInTheDatabase();

        // WHEN
        try
        {
            RecordFormats recordFormats = RecordFormatSelector.selectForConfig( Config.defaults(), NullLogProvider.getInstance() );
            try ( BatchingNeoStores store = BatchingNeoStores.batchingNeoStores( storage.fileSystem(), storage.directory().absolutePath(), recordFormats,
                    DEFAULT, NullLogService.getInstance(), EMPTY, Config.defaults() ) )
            {
                store.createNew();
                fail( "Should fail on existing data" );
            }
        }
        catch ( IllegalStateException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( "already contains" ) );
        }
    }

    @Test
    public void shouldRespectDbConfig() throws Exception
    {
        // GIVEN
        int size = 10;
        Config config = Config.defaults( stringMap(
                GraphDatabaseSettings.array_block_size.name(), String.valueOf( size ),
                GraphDatabaseSettings.string_block_size.name(), String.valueOf( size ) ) );

        // WHEN
        RecordFormats recordFormats = LATEST_RECORD_FORMATS;
        int headerSize = recordFormats.dynamic().getRecordHeaderSize();
        try ( BatchingNeoStores store = BatchingNeoStores.batchingNeoStores( storage.fileSystem(), storage.directory().absolutePath(),
                recordFormats, DEFAULT, NullLogService.getInstance(), EMPTY, config ) )
        {
            store.createNew();

            // THEN
            assertEquals( size + headerSize, store.getPropertyStore().getArrayStore().getRecordSize() );
            assertEquals( size + headerSize, store.getPropertyStore().getStringStore().getRecordSize() );
        }
    }

    @Test
    public void shouldPruneAndOpenExistingDatabase() throws Exception
    {
        // given
        for ( StoreType typeToTest : relevantRecordStores() )
        {
            // given all the stores with some records in them
            try ( PageCache pageCache = storage.pageCache() )
            {
                storage.directory().cleanup();
                try ( BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( storage.fileSystem(), pageCache,
                        PageCacheTracer.NULL, storage.directory().absolutePath(), LATEST_RECORD_FORMATS, DEFAULT, NullLogService.getInstance(), EMPTY,
                        Config.defaults() ) )
                {
                    stores.createNew();
                    for ( StoreType type : relevantRecordStores() )
                    {
                        createRecordIn( stores.getNeoStores().getRecordStore( type ) );
                    }
                }

                // when opening and pruning all except the one we test
                try ( BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( storage.fileSystem(), pageCache,
                        PageCacheTracer.NULL, storage.directory().absolutePath(), LATEST_RECORD_FORMATS, DEFAULT, NullLogService.getInstance(), EMPTY,
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
                            assertEquals( store.toString(), store.getNumberOfReservedLowIds(), store.getHighId() );
                        }
                    }
                }
            }
        }
    }

    @Test
    public void shouldDecideToAllocateDoubleRelationshipRecordUnitsOnLargeAmountOfRelationshipsOnSupportedFormat() throws Exception
    {
        // given
        RecordFormats formats = new ForcedSecondaryUnitRecordFormats( LATEST_RECORD_FORMATS );
        try ( PageCache pageCache = storage.pageCache();
              BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( storage.fileSystem(),
                pageCache, PageCacheTracer.NULL, storage.directory().absolutePath(), formats, DEFAULT,
                NullLogService.getInstance(), EMPTY, Config.defaults() ) )
        {
            stores.createNew();
            Estimates estimates = Inputs.knownEstimates( 0, DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD << 1, 0, 0, 0, 0, 0 );

            // when
            boolean doubleUnits = stores.determineDoubleRelationshipRecordUnits( estimates );

            // then
            assertTrue( doubleUnits );
        }
    }

    @Test
    public void shouldNotDecideToAllocateDoubleRelationshipRecordUnitsonLowAmountOfRelationshipsOnSupportedFormat() throws Exception
    {
        // given
        RecordFormats formats = new ForcedSecondaryUnitRecordFormats( LATEST_RECORD_FORMATS );
        try ( BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( storage.fileSystem(),
                storage.pageCache(), PageCacheTracer.NULL, storage.directory().absolutePath(), formats, DEFAULT,
                NullLogService.getInstance(), EMPTY, Config.defaults() ) )
        {
            stores.createNew();
            Estimates estimates = Inputs.knownEstimates( 0, DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD >> 1, 0, 0, 0, 0, 0 );

            // when
            boolean doubleUnits = stores.determineDoubleRelationshipRecordUnits( estimates );

            // then
            assertFalse( doubleUnits );
        }
    }

    @Test
    public void shouldNotDecideToAllocateDoubleRelationshipRecordUnitsonLargeAmountOfRelationshipsOnUnsupportedFormat() throws Exception
    {
        // given
        RecordFormats formats = LATEST_RECORD_FORMATS;
        try ( BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( storage.fileSystem(),
                storage.pageCache(), PageCacheTracer.NULL, storage.directory().absolutePath(), formats, DEFAULT,
                NullLogService.getInstance(), EMPTY, Config.defaults() ) )
        {
            stores.createNew();
            Estimates estimates = Inputs.knownEstimates( 0, DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD << 1, 0, 0, 0, 0, 0 );

            // when
            boolean doubleUnits = stores.determineDoubleRelationshipRecordUnits( estimates );

            // then
            assertFalse( doubleUnits );
        }
    }

    @Test
    public void shouldDeleteIdGeneratorsWhenOpeningExistingStore() throws IOException
    {
        // given
        long expectedHighId;
        try ( BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( storage.fileSystem(),
                storage.pageCache(), PageCacheTracer.NULL, storage.directory().absolutePath(), LATEST_RECORD_FORMATS, DEFAULT,
                NullLogService.getInstance(), EMPTY, Config.defaults() ) )
        {
            stores.createNew();
            RelationshipStore relationshipStore = stores.getRelationshipStore();
            RelationshipRecord record = relationshipStore.newRecord();
            long no = NULL_REFERENCE.longValue();
            record.initialize( true, no, 1, 2, 0, no, no, no, no, true, true );
            record.setId( relationshipStore.nextId() );
            expectedHighId = relationshipStore.getHighId();
            relationshipStore.updateRecord( record );
            // fiddle with the highId
            relationshipStore.setHighId( record.getId() + 999 );
        }

        // when
        try ( BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( storage.fileSystem(),
                storage.pageCache(), PageCacheTracer.NULL, storage.directory().absolutePath(), LATEST_RECORD_FORMATS, DEFAULT,
                NullLogService.getInstance(), EMPTY, Config.defaults() ) )
        {
            stores.pruneAndOpenExistingStore( Predicates.alwaysTrue(), Predicates.alwaysTrue() );

            // then
            assertEquals( expectedHighId, stores.getRelationshipStore().getHighId() );
        }
    }

    private StoreType[] relevantRecordStores()
    {
        return Stream.of( StoreType.values() )
                .filter( type -> type.isRecordStore() && type != StoreType.META_DATA ).toArray( StoreType[]::new );
    }

    private <RECORD extends AbstractBaseRecord> void createRecordIn( RecordStore<RECORD> store )
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

    private void someDataInTheDatabase()
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory()
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( storage.fileSystem() ) )
                .newImpermanentDatabase( storage.directory().absolutePath() );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }
}
