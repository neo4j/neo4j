/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.stream.Stream;

import org.neo4j.function.Predicates;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.MyRelTypes;
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
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.unsafe.impl.batchimport.input.Input.Estimates;
import org.neo4j.unsafe.impl.batchimport.input.Inputs;
import org.neo4j.values.storable.Values;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.store.format.standard.Standard.LATEST_RECORD_FORMATS;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores.DOUBLE_RELATIONSHIP_RECORD_UNIT_THRESHOLD;

@PageCacheExtension
class BatchingNeoStoresTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private PageCache pageCache;
    @Inject
    private FileSystemAbstraction fileSystem;

    @Test
    void shouldNotOpenStoreWithNodesOrRelationshipsInIt()
    {
        // GIVEN
        someDataInTheDatabase();

        // WHEN
        IllegalStateException exception = assertThrows( IllegalStateException.class, () ->
        {
            try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler() )
            {
                RecordFormats recordFormats = RecordFormatSelector.selectForConfig( Config.defaults(), NullLogProvider.getInstance() );
                try ( BatchingNeoStores store = BatchingNeoStores.batchingNeoStores( fileSystem, testDirectory.databaseDir(), recordFormats, DEFAULT,
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
        Config config = Config.defaults( stringMap(
                GraphDatabaseSettings.array_block_size.name(), String.valueOf( size ),
                GraphDatabaseSettings.string_block_size.name(), String.valueOf( size ) ) );

        // WHEN
        RecordFormats recordFormats = LATEST_RECORD_FORMATS;
        int headerSize = recordFormats.dynamic().getRecordHeaderSize();
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler();
              BatchingNeoStores store = BatchingNeoStores.batchingNeoStores( fileSystem, testDirectory.absolutePath(),
              recordFormats, DEFAULT, NullLogService.getInstance(), EMPTY, config, jobScheduler ) )
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
                    PageCacheTracer.NULL, testDirectory.absolutePath(), LATEST_RECORD_FORMATS, DEFAULT, NullLogService.getInstance(), EMPTY,
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
                    PageCacheTracer.NULL, testDirectory.absolutePath(), LATEST_RECORD_FORMATS, DEFAULT, NullLogService.getInstance(), EMPTY,
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
                pageCache, PageCacheTracer.NULL, testDirectory.absolutePath(), formats, DEFAULT,
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
    void shouldNotDecideToAllocateDoubleRelationshipRecordUnitsonLowAmountOfRelationshipsOnSupportedFormat() throws Exception
    {
        // given
        RecordFormats formats = new ForcedSecondaryUnitRecordFormats( LATEST_RECORD_FORMATS );
        try ( BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( fileSystem,
                pageCache, PageCacheTracer.NULL, testDirectory.absolutePath(), formats, DEFAULT,
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
    void shouldNotDecideToAllocateDoubleRelationshipRecordUnitsonLargeAmountOfRelationshipsOnUnsupportedFormat() throws Exception
    {
        // given
        RecordFormats formats = LATEST_RECORD_FORMATS;
        try ( BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( fileSystem,
                pageCache, PageCacheTracer.NULL, testDirectory.absolutePath(), formats, DEFAULT,
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
    void shouldDeleteIdGeneratorsWhenOpeningExistingStore() throws IOException
    {
        // given
        long expectedHighId;
        try ( BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( fileSystem,
                pageCache, PageCacheTracer.NULL, testDirectory.absolutePath(), LATEST_RECORD_FORMATS, DEFAULT,
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
        try ( BatchingNeoStores stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache( fileSystem,
                pageCache, PageCacheTracer.NULL, testDirectory.absolutePath(), LATEST_RECORD_FORMATS, DEFAULT,
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

    private void someDataInTheDatabase()
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory()
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fileSystem ) )
                .newImpermanentDatabase( testDirectory.databaseDir() );
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
