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
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;
import java.util.stream.LongStream;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.DelegatingStoreChannel;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.allocator.ReusableRecordsAllocator;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.Exceptions.contains;
import static org.neo4j.kernel.impl.store.DynamicArrayStore.allocateFromNumbers;
import static org.neo4j.kernel.impl.store.NodeStore.readOwnerFromDynamicLabelsRecord;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

public class NodeStoreTest
{
    @ClassRule
    public static final PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public final EphemeralFileSystemRule efs = new EphemeralFileSystemRule();

    private NodeStore nodeStore;
    private NeoStores neoStores;
    private IdGeneratorFactory idGeneratorFactory;

    @After
    public void tearDown()
    {
        if ( neoStores != null )
        {
            neoStores.close();
        }
    }

    @Test
    public void shouldReadFirstFromSingleRecordDynamicLongArray()
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
    public void shouldReadFirstAsNullFromEmptyDynamicLongArray()
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
    public void shouldReadFirstFromTwoRecordDynamicLongArray()
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
    public void shouldCombineProperFiveByteLabelField() throws Exception
    {
        // GIVEN
        // -- a store
        EphemeralFileSystemAbstraction fs = efs.get();
        nodeStore = newNodeStore( fs );

        // -- a record with the msb carrying a negative value
        long nodeId = 0;
        long labels = 0x8000000001L;
        NodeRecord record =
                new NodeRecord( nodeId, false, NO_NEXT_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() );
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
    public void shouldKeepRecordLightWhenSettingLabelFieldWithoutDynamicRecords()
    {
        // GIVEN
        NodeRecord record = new NodeRecord( 0, false, NO_NEXT_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() );

        // WHEN
        record.setLabelField( 0, Collections.emptyList() );

        // THEN
        assertTrue( record.isLight() );
    }

    @Test
    public void shouldMarkRecordHeavyWhenSettingLabelFieldWithDynamicRecords()
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
    public void shouldTellNodeInUse() throws Exception
    {
        // Given
        EphemeralFileSystemAbstraction fs = efs.get();
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
    public void scanningRecordsShouldVisitEachInUseRecordOnce() throws IOException
    {
        // GIVEN we have a NodeStore with data that spans several pages...
        EphemeralFileSystemAbstraction fs = efs.get();
        nodeStore = newNodeStore( fs );

        ThreadLocalRandom rng = ThreadLocalRandom.current();
        final PrimitiveLongSet nextRelSet = Primitive.longSet();
        for ( int i = 0; i < 10_000; i++ )
        {
            // Enough records to span several pages
            int nextRelCandidate = rng.nextInt( 0, Integer.MAX_VALUE );
            if ( nextRelSet.add( nextRelCandidate ) )
            {
                long nodeId = nodeStore.nextId();
                NodeRecord record = new NodeRecord(
                        nodeId, false, nextRelCandidate, 20, true );
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

        Visitor<NodeRecord,IOException> scanner = record ->
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
    public void shouldCloseStoreFileOnFailureToOpen()
    {
        // GIVEN
        final MutableBoolean fired = new MutableBoolean();
        FileSystemAbstraction fs = new DelegatingFileSystemAbstraction( efs.get() )
        {
            @Override
            public StoreChannel open( File fileName, OpenMode openMode ) throws IOException
            {
                return new DelegatingStoreChannel( super.open( fileName, openMode ) )
                {
                    @Override
                    public void readAll( ByteBuffer dst ) throws IOException
                    {
                        fired.setValue( true );
                        throw new IOException( "Proving a point here" );
                    }
                };
            }
        };

        // WHEN
        try ( PageCache pageCache = pageCacheRule.getPageCache( fs ) )
        {
            newNodeStore( fs );
            fail( "Should fail" );
        }   // Close the page cache here so that we can see failure to close (due to still mapped files)
        catch ( Exception e )
        {
            // THEN
            assertTrue( contains( e, IOException.class ) );
            assertTrue( fired.booleanValue() );
        }
    }

    @Test
    public void shouldFreeSecondaryUnitIdOfDeletedRecord() throws Exception
    {
        // GIVEN
        EphemeralFileSystemAbstraction fs = efs.get();
        nodeStore = newNodeStore( fs );
        NodeRecord record = new NodeRecord( 5L );
        record.setRequiresSecondaryUnit( true );
        record.setSecondaryUnitId( 10L );
        record.setInUse( true );
        nodeStore.updateRecord( record );
        nodeStore.setHighestPossibleIdInUse( 10L );

        // WHEN
        record.setInUse( false );
        nodeStore.updateRecord( record );

        // THEN
        IdGenerator idGenerator = idGeneratorFactory.get( IdType.NODE );
        verify( idGenerator ).freeId( 5L );
        verify( idGenerator ).freeId( 10L );
    }

    @Test
    public void shouldFreeSecondaryUnitIdOfShrunkRecord() throws Exception
    {
        // GIVEN
        EphemeralFileSystemAbstraction fs = efs.get();
        nodeStore = newNodeStore( fs );
        NodeRecord record = new NodeRecord( 5L );
        record.setRequiresSecondaryUnit( true );
        record.setSecondaryUnitId( 10L );
        record.setInUse( true );
        nodeStore.updateRecord( record );
        nodeStore.setHighestPossibleIdInUse( 10L );

        // WHEN
        record.setRequiresSecondaryUnit( false );
        nodeStore.updateRecord( record );

        // THEN
        IdGenerator idGenerator = idGeneratorFactory.get( IdType.NODE );
        verify( idGenerator, never() ).freeId( 5L );
        verify( idGenerator ).freeId( 10L );
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void ensureHeavy()
    {
        long[] labels = LongStream.range( 1, 1000 ).toArray();
        NodeRecord node = new NodeRecord( 5 );
        node.setLabelField( 10, Collections.emptyList() );
        Collection<DynamicRecord> dynamicLabelRecords = DynamicNodeLabels.putSorted( node, labels,
                mock( NodeStore.class ), new StandaloneDynamicRecordAllocator() );
        assertThat( dynamicLabelRecords, not( empty() ) );
        RecordCursor<DynamicRecord> dynamicLabelCursor = mock( RecordCursor.class );
        when( dynamicLabelCursor.getAll() ).thenReturn( Iterables.asList( dynamicLabelRecords ) );

        NodeStore.ensureHeavy( node, dynamicLabelCursor );

        assertEquals( dynamicLabelRecords, node.getDynamicLabelRecords() );
    }

    private NodeStore newNodeStore( FileSystemAbstraction fs ) throws IOException
    {
        return newNodeStore( fs, pageCacheRule.getPageCache( fs ) );
    }

    private NodeStore newNodeStore( FileSystemAbstraction fs, PageCache pageCache ) throws IOException
    {
        File storeDir = new File( "dir" );
        fs.mkdirs( storeDir );
        idGeneratorFactory = spy( new DefaultIdGeneratorFactory( fs )
        {
            @Override
            protected IdGenerator instantiate( FileSystemAbstraction fs, File fileName, int grabSize, long maxValue,
                    boolean aggressiveReuse, IdType idType, LongSupplier highId )
            {
                return spy( super.instantiate( fs, fileName, grabSize, maxValue, aggressiveReuse, idType, highId ) );
            }
        } );
        StoreFactory factory = new StoreFactory( storeDir, Config.defaults(), idGeneratorFactory, pageCache, fs,
                NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );
        neoStores = factory.openAllNeoStores( true );
        nodeStore = neoStores.getNodeStore();
        return nodeStore;
    }
}
