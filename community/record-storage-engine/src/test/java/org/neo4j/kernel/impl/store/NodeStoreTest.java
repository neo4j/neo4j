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

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.indexed.IndexedIdGenerator;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.DelegatingPageCache;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.store.allocator.ReusableRecordsAllocator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.exception.ExceptionUtils.indexOfThrowable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.kernel.impl.store.DynamicArrayStore.allocateFromNumbers;
import static org.neo4j.kernel.impl.store.NodeStore.readOwnerFromDynamicLabelsRecord;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

@EphemeralNeo4jLayoutExtension
class NodeStoreTest
{
    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private DatabaseLayout databaseLayout;

    private NodeStore nodeStore;
    private NeoStores neoStores;

    @AfterEach
    void tearDown()
    {
        if ( neoStores != null )
        {
            neoStores.close();
        }
    }

    @Test
    void shouldReadFirstFromSingleRecordDynamicLongArray()
    {
        // GIVEN
        Long expectedId = 12L;
        long[] ids = new long[]{expectedId, 23L, 42L};
        DynamicRecord firstRecord = new DynamicRecord( 0L );
        allocateFromNumbers( new ArrayList<>(), ids, new ReusableRecordsAllocator( 60, firstRecord ) );

        // WHEN
        Long firstId = readOwnerFromDynamicLabelsRecord( firstRecord );

        // THEN
        assertEquals( expectedId, firstId );
    }

    @Test
    void shouldReadFirstAsNullFromEmptyDynamicLongArray()
    {
        // GIVEN
        Long expectedId = null;
        long[] ids = new long[]{};
        DynamicRecord firstRecord = new DynamicRecord( 0L );
        allocateFromNumbers( new ArrayList<>(), ids, new ReusableRecordsAllocator( 60, firstRecord ) );

        // WHEN
        Long firstId = readOwnerFromDynamicLabelsRecord( firstRecord );

        // THEN
        assertEquals( expectedId, firstId );
    }

    @Test
    void shouldReadFirstFromTwoRecordDynamicLongArray()
    {
        // GIVEN
        Long expectedId = 12L;
        long[] ids = new long[]{expectedId, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L};
        DynamicRecord firstRecord = new DynamicRecord( 0L );
        allocateFromNumbers( new ArrayList<>(), ids,
                new ReusableRecordsAllocator( 8, firstRecord, new DynamicRecord( 1L ) ) );

        // WHEN
        Long firstId = readOwnerFromDynamicLabelsRecord( firstRecord );

        // THEN
        assertEquals( expectedId, firstId );
    }

    @Test
    void shouldCombineProperFiveByteLabelField()
    {
        // GIVEN
        // -- a store
        nodeStore = newNodeStore( fs );

        // -- a record with the msb carrying a negative value
        long nodeId = 0;
        long labels = 0x8000000001L;
        NodeRecord record = new NodeRecord( nodeId, false, NO_NEXT_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() );
        record.setInUse( true );
        record.setLabelField( labels, Collections.emptyList() );
        nodeStore.updateRecord( record );

        // WHEN
        // -- reading that record back
        NodeRecord readRecord = nodeStore.getRecord( nodeId, nodeStore.newRecord(), NORMAL );

        // THEN
        // -- the label field must be the same
        assertEquals( labels, readRecord.getLabelField() );
    }

    @Test
    void shouldKeepRecordLightWhenSettingLabelFieldWithoutDynamicRecords()
    {
        // GIVEN
        NodeRecord record = new NodeRecord( 0, false, NO_NEXT_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() );

        // WHEN
        record.setLabelField( 0, Collections.emptyList() );

        // THEN
        assertTrue( record.isLight() );
    }

    @Test
    void shouldMarkRecordHeavyWhenSettingLabelFieldWithDynamicRecords()
    {
        // GIVEN
        NodeRecord record = new NodeRecord( 0, false, NO_NEXT_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() );

        // WHEN
        DynamicRecord dynamicRecord = new DynamicRecord( 1 );
        record.setLabelField( 0x8000000001L, asList( dynamicRecord ) );

        // THEN
        assertFalse( record.isLight() );
    }

    @Test
    void shouldTellNodeInUse()
    {
        // Given
        NodeStore store = newNodeStore( fs );

        long exists = store.nextId();
        store.updateRecord( new NodeRecord( exists, false, 10, 20, true ) );

        long deleted = store.nextId();
        store.updateRecord( new NodeRecord( deleted, false, 10, 20, true ) );
        store.updateRecord( new NodeRecord( deleted, false, 10, 20, false ) );

        // When & then
        assertTrue( store.isInUse( exists ) );
        assertFalse( store.isInUse( deleted ) );
        assertFalse( store.isInUse( nodeStore.recordFormat.getMaxId() ) );
    }

    @Test
    void scanningRecordsShouldVisitEachInUseRecordOnce() throws IOException
    {
        // GIVEN we have a NodeStore with data that spans several pages...
        nodeStore = newNodeStore( fs );

        ThreadLocalRandom rng = ThreadLocalRandom.current();
        final MutableLongSet nextRelSet = new LongHashSet();
        for ( int i = 0; i < 10_000; i++ )
        {
            // Enough records to span several pages
            int nextRelCandidate = rng.nextInt( 0, Integer.MAX_VALUE );
            if ( nextRelSet.add( nextRelCandidate ) )
            {
                long nodeId = nodeStore.nextId();
                NodeRecord record = new NodeRecord( nodeId, false, nextRelCandidate, 20, true );
                nodeStore.updateRecord( record );
                if ( rng.nextInt( 0, 10 ) < 3 )
                {
                    nextRelSet.remove( nextRelCandidate );
                    record.setInUse( false );
                    nodeStore.updateRecord( record );
                }
            }
        }

        // ...WHEN we now have an interesting set of node records, and we
        // visit each and remove that node from our nextRelSet...

        Visitor<NodeRecord, IOException> scanner = record ->
        {
            // ...THEN we should observe that no nextRel is ever removed twice...
            assertTrue( nextRelSet.remove( record.getNextRel() ) );
            return false;
        };
        nodeStore.scanAllRecords( scanner );

        // ...NOR do we have anything left in the set afterwards.
        assertTrue( nextRelSet.isEmpty() );
    }

    @Test
    void shouldCloseStoreFileOnFailureToOpen()
    {
        // GIVEN
        final MutableBoolean fired = new MutableBoolean();

        // WHEN
        Exception exception = assertThrows( Exception.class, () ->
        {
            try ( PageCache pageCache = pageCacheExtension.getPageCache( fs ) )
            {
                PageCache customPageCache = new DelegatingPageCache( pageCache )
                {
                    @Override
                    public PagedFile map( File file, VersionContextSupplier versionContextSupplier, int pageSize, OpenOption... openOptions ) throws IOException
                    {
                        if ( file.getName().endsWith( ".id" ) )
                        {
                            fired.setTrue();
                            throw new IOException( "Proving a point here" );
                        }
                        return super.map( file, versionContextSupplier, pageSize, openOptions );
                    }
                };

                newNodeStore( fs, customPageCache );
            }
        } );
        assertTrue( indexOfThrowable( exception, IOException.class ) != -1 );
        assertTrue( fired.booleanValue() );
    }

    @Test
    void shouldFreeSecondaryUnitIdOfDeletedRecord()
    {
        // GIVEN
        nodeStore = newNodeStore( fs );
        NodeRecord record = new NodeRecord( 5L );
        record.setSecondaryUnitIdOnLoad( 10L );
        record.setInUse( true );
        nodeStore.updateRecord( record );
        nodeStore.setHighestPossibleIdInUse( 10L );

        // WHEN
        record.setInUse( false );
        IdUpdateListener idUpdateListener = mock( IdUpdateListener.class );
        nodeStore.updateRecord( record, idUpdateListener );

        // THEN
        verify( idUpdateListener ).markIdAsUnused( eq( IdType.NODE ), any(), eq( 5L ) );
        verify( idUpdateListener ).markIdAsUnused( eq( IdType.NODE ), any(), eq( 10L ) );
    }

    @Test
    void shouldFreeSecondaryUnitIdOfShrunkRecord()
    {
        // GIVEN
        nodeStore = newNodeStore( fs );
        NodeRecord record = new NodeRecord( 5L );
        record.setSecondaryUnitIdOnLoad( 10L );
        record.setInUse( true );
        nodeStore.updateRecord( record );
        nodeStore.setHighestPossibleIdInUse( 10L );

        // WHEN
        record.setRequiresSecondaryUnit( false );
        IdUpdateListener idUpdateListener = mock( IdUpdateListener.class );
        nodeStore.updateRecord( record, idUpdateListener );

        // THEN
        verify( idUpdateListener, never() ).markIdAsUnused( eq( IdType.NODE ), any(), eq( 5L ) );
        verify( idUpdateListener ).markIdAsUnused( eq( IdType.NODE ), any(), eq( 10L ) );
    }

    @Test
    void shouldMarkSecondaryUnitAsUsedOnCreatedAsBigRecord()
    {
        // given
        long primaryUnitId = 5L;
        long secondaryUnitId = 10L;
        nodeStore = newNodeStore( fs );
        NodeRecord record = new NodeRecord( primaryUnitId );
        record.setSecondaryUnitIdOnCreate( secondaryUnitId );
        record.setInUse( true );
        record.setCreated();

        // when
        IdUpdateListener idUpdateListener = mock( IdUpdateListener.class );
        nodeStore.updateRecord( record, idUpdateListener );

        // then
        verify( idUpdateListener ).markIdAsUsed( eq( IdType.NODE ), any(), eq( primaryUnitId ) );
        verify( idUpdateListener ).markIdAsUsed( eq( IdType.NODE ), any(), eq( secondaryUnitId ) );
    }

    @Test
    void shouldMarkSecondaryUnitAsUsedOnGrowing()
    {
        // given
        long primaryUnitId = 5L;
        long secondaryUnitId = 10L;
        nodeStore = newNodeStore( fs );
        NodeRecord record = new NodeRecord( primaryUnitId );
        record.setInUse( true );
        record.setCreated();
        nodeStore.updateRecord( record );

        // when
        nodeStore.getRecord( primaryUnitId, record, NORMAL );
        record.setSecondaryUnitIdOnCreate( secondaryUnitId );
        IdUpdateListener idUpdateListener = mock( IdUpdateListener.class );
        nodeStore.updateRecord( record, idUpdateListener );

        // then
        verify( idUpdateListener, never() ).markIdAsUsed( eq( IdType.NODE ), any(), eq( primaryUnitId ) );
        verify( idUpdateListener ).markIdAsUsed( eq( IdType.NODE ), any(), eq( secondaryUnitId ) );
    }

    private NodeStore newNodeStore( FileSystemAbstraction fs )
    {
        return newNodeStore( fs, pageCacheExtension.getPageCache( fs ) );
    }

    private NodeStore newNodeStore( FileSystemAbstraction fs, PageCache pageCache )
    {
        IdGeneratorFactory idGeneratorFactory = spy( new DefaultIdGeneratorFactory( fs, immediate() )
        {
            @Override
            protected IndexedIdGenerator instantiate( FileSystemAbstraction fs, PageCache pageCache, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
                    File fileName, LongSupplier highIdSupplier, long maxValue, IdType idType, boolean readOnly, OpenOption[] openOptions )
            {
                return spy(
                        super.instantiate( fs, pageCache, recoveryCleanupWorkCollector, fileName, highIdSupplier, maxValue, idType, readOnly, openOptions ) );
            }
        } );
        StoreFactory factory =
                new StoreFactory( databaseLayout, Config.defaults(), idGeneratorFactory, pageCache, fs, NullLogProvider.getInstance() );
        neoStores = factory.openAllNeoStores( true );
        nodeStore = neoStores.getNodeStore();
        return nodeStore;
    }
}
