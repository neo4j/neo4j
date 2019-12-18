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
package org.neo4j.internal.recordstorage;

import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.IdUpdateListener;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.util.Bits;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.Numbers.safeCastLongToInt;
import static org.neo4j.internal.helpers.collection.Iterables.addAll;
import static org.neo4j.util.Bits.bits;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
@ExtendWith( RandomExtension.class )
class NodeLabelsFieldTest
{
    @Inject
    private PageCache pageCache;
    @Inject
    private RandomRule random;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private DatabaseLayout databaseLayout;

    private NeoStores neoStores;
    private NodeStore nodeStore;

    @BeforeEach
    void startUp()
    {
        Config config = Config.defaults( GraphDatabaseSettings.label_block_size, 60 );
        StoreFactory storeFactory = new StoreFactory( databaseLayout, config,
                new DefaultIdGeneratorFactory( fs, immediate() ),
                pageCache, fs, NullLogProvider.getInstance() );
        neoStores = storeFactory.openAllNeoStores( true );
        nodeStore = neoStores.getNodeStore();
    }

    @AfterEach
    void cleanUp()
    {
        neoStores.close();
    }

    @Test
    void shouldInlineOneLabel()
    {
        // GIVEN
        long labelId = 10;
        NodeRecord node = nodeRecordWithInlinedLabels();
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        nodeLabels.add( labelId, null, null );

        // THEN
        assertEquals( inlinedLabelsLongRepresentation( labelId ), node.getLabelField() );
    }

    @Test
    void shouldInlineOneLabelWithHighId()
    {
        // GIVEN
        long labelId = 10000;
        NodeRecord node = nodeRecordWithInlinedLabels();
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        nodeLabels.add( labelId, null, null );

        // THEN
        assertEquals( inlinedLabelsLongRepresentation( labelId ), node.getLabelField() );
    }

    @Test
    void shouldInlineTwoSmallLabels()
    {
        // GIVEN
        long labelId1 = 10;
        long labelId2 = 30;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId1 );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        nodeLabels.add( labelId2, null, null );

        // THEN
        assertEquals( inlinedLabelsLongRepresentation( labelId1, labelId2 ), node.getLabelField() );
    }

    @Test
    void shouldInlineThreeSmallLabels()
    {
        // GIVEN
        long labelId1 = 10;
        long labelId2 = 30;
        long labelId3 = 4095;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId1, labelId2 );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        nodeLabels.add( labelId3, null, null );

        // THEN
        assertEquals( inlinedLabelsLongRepresentation( labelId1, labelId2, labelId3 ), node.getLabelField() );
    }

    @Test
    void shouldInlineFourSmallLabels()
    {
        // GIVEN
        long labelId1 = 10;
        long labelId2 = 30;
        long labelId3 = 45;
        long labelId4 = 60;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId1, labelId2, labelId3 );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        nodeLabels.add( labelId4, null, null );

        // THEN
        assertEquals( inlinedLabelsLongRepresentation( labelId1, labelId2, labelId3, labelId4 ), node.getLabelField() );
    }

    @Test
    void shouldInlineFiveSmallLabels()
    {
        // GIVEN
        long labelId1 = 10;
        long labelId2 = 30;
        long labelId3 = 45;
        long labelId4 = 60;
        long labelId5 = 61;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId1, labelId2, labelId3, labelId4 );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        nodeLabels.add( labelId5, null, null );

        // THEN
        assertEquals( inlinedLabelsLongRepresentation( labelId1, labelId2, labelId3, labelId4, labelId5 ),
            node.getLabelField() );
    }

    @Test
    void shouldSpillOverToDynamicRecordIfExceedsInlinedSpace()
    {
        // GIVEN -- the upper limit for a label ID for 3 labels would be 36b/3 - 1 = 12b - 1 = 4095
        long labelId1 = 10;
        long labelId2 = 30;
        long labelId3 = 4096;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId1, labelId2 );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        Collection<DynamicRecord> changedDynamicRecords = nodeLabels.add( labelId3, nodeStore,
            nodeStore.getDynamicLabelStore() );

        // THEN
        assertEquals( 1, Iterables.count( changedDynamicRecords ) );
        assertEquals( dynamicLabelsLongRepresentation( changedDynamicRecords ), node.getLabelField() );
        Assertions.assertArrayEquals( new long[]{labelId1, labelId2, labelId3},
            DynamicNodeLabels.getDynamicLabelsArray( changedDynamicRecords, nodeStore.getDynamicLabelStore() ) );
    }

    @Test
    void oneDynamicRecordShouldExtendIntoAnAdditionalIfTooManyLabels()
    {
        // GIVEN
        // will occupy 60B of data, i.e. one dynamic record
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, oneByteLongs( 56 ) );
        Collection<DynamicRecord> initialRecords = node.getDynamicLabelRecords();
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        Set<DynamicRecord> changedDynamicRecords = Iterables.asSet( nodeLabels.add( 1, nodeStore, nodeStore.getDynamicLabelStore() ) );

        // THEN
        assertTrue( changedDynamicRecords.containsAll( initialRecords ) );
        assertEquals( initialRecords.size() + 1, changedDynamicRecords.size() );
    }

    @Test
    void oneDynamicRecordShouldStoreItsOwner()
    {
        // GIVEN
        // will occupy 60B of data, i.e. one dynamic record
        Long nodeId = 24L;
        NodeRecord node = nodeRecordWithDynamicLabels( nodeId, nodeStore, oneByteLongs(56) );
        Collection<DynamicRecord> initialRecords = node.getDynamicLabelRecords();

        // WHEN
        Pair<Long,long[]> pair = DynamicNodeLabels.getDynamicLabelsArrayAndOwner( initialRecords,
            nodeStore.getDynamicLabelStore() );

        // THEN
        assertEquals( nodeId,  pair.first() );
    }

    @Test
    void twoDynamicRecordsShouldShrinkToOneWhenRemoving()
    {
        // GIVEN
        // will occupy 61B of data, i.e. just two dynamic records
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, oneByteLongs( 57 ) );
        Collection<DynamicRecord> initialRecords = node.getDynamicLabelRecords();
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        List<DynamicRecord> changedDynamicRecords = addAll(
            new ArrayList<>(),
            nodeLabels.remove( 255 /*Initial labels go from 255 and down to 255-58*/, nodeStore ) );

        // THEN
        assertEquals( initialRecords, changedDynamicRecords );
        assertTrue( changedDynamicRecords.get( 0 ).inUse() );
        Assertions.assertFalse( changedDynamicRecords.get( 1 ).inUse() );
    }

    @Test
    void twoDynamicRecordsShouldShrinkToOneWhenRemovingWithoutChangingItsOwner()
    {
        // GIVEN
        // will occupy 61B of data, i.e. just two dynamic records
        Long nodeId = 42L;
        NodeRecord node = nodeRecordWithDynamicLabels( nodeId, nodeStore, oneByteLongs( 57 ) );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        List<DynamicRecord> changedDynamicRecords = addAll( new ArrayList<>(),
            nodeLabels.remove( 255 /*Initial labels go from 255 and down to 255-58*/, nodeStore ) );

        // WHEN
        Pair<Long,long[]> changedPair = DynamicNodeLabels.getDynamicLabelsArrayAndOwner( changedDynamicRecords,
            nodeStore.getDynamicLabelStore() );

        // THEN
        assertEquals( nodeId,  changedPair.first() );
    }

    @Test
    void oneDynamicRecordShouldShrinkIntoInlinedWhenRemoving()
    {
        // GIVEN
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, oneByteLongs( 5 ) );
        Collection<DynamicRecord> initialRecords = node.getDynamicLabelRecords();
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        Collection<DynamicRecord> changedDynamicRecords = Iterables.asCollection( nodeLabels.remove( 255, nodeStore ) );

        // THEN
        assertEquals( initialRecords, changedDynamicRecords );
        Assertions.assertFalse( Iterables.single( changedDynamicRecords ).inUse() );
        assertEquals( inlinedLabelsLongRepresentation( 251, 252, 253, 254 ), node.getLabelField() );
    }

    @Test
    void shouldReadIdOfDynamicRecordFromDynamicLabelsField()
    {
        // GIVEN
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, oneByteLongs( 5 ) );
        DynamicRecord dynamicRecord = node.getDynamicLabelRecords().iterator().next();

        // WHEN
        long dynRecordId = NodeLabelsField.firstDynamicLabelRecordId( node.getLabelField() );

        // THEN
        assertEquals( dynamicRecord.getId(), dynRecordId );
    }

    @Test
    void shouldReadNullDynamicRecordFromInlineLabelsField()
    {
        // GIVEN
        NodeRecord node = nodeRecordWithInlinedLabels( 23L );

        // WHEN
        boolean isDynamicReference = NodeLabelsField.fieldPointsToDynamicRecordOfLabels( node.getLabelField() );

        // THEN
        Assertions.assertFalse( isDynamicReference );
    }

    @Test
    void maximumOfSevenInlinedLabels()
    {
        // GIVEN
        long[] labels = new long[] {0, 1, 2, 3, 4, 5, 6};
        NodeRecord node = nodeRecordWithInlinedLabels( labels );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        Iterable<DynamicRecord> changedDynamicRecords = nodeLabels.add( 23, nodeStore, nodeStore.getDynamicLabelStore() );

        // THEN
        assertEquals( dynamicLabelsLongRepresentation( changedDynamicRecords ), node.getLabelField() );
        assertEquals( 1, Iterables.count( changedDynamicRecords ) );
    }

    @Test
    void addingAnAlreadyAddedLabelWhenLabelsAreInlinedShouldFail()
    {
        // GIVEN
        int labelId = 1;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        try
        {
            nodeLabels.add( labelId, nodeStore, nodeStore.getDynamicLabelStore() );
            fail( "Should have thrown exception" );
        }
        catch ( IllegalStateException e )
        {
            // THEN
        }
    }

    @Test
    void addingAnAlreadyAddedLabelWhenLabelsAreInDynamicRecordsShouldFail()
    {
        // GIVEN
        long[] labels = oneByteLongs( 20 );
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, labels );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        try
        {
            nodeLabels.add( safeCastLongToInt( labels[0] ), nodeStore, nodeStore.getDynamicLabelStore() );
            fail( "Should have thrown exception" );
        }
        catch ( IllegalStateException e )
        {
            // THEN
        }
    }

    @Test
    void removingNonExistentInlinedLabelShouldFail()
    {
        // GIVEN
        int labelId1 = 1;
        int labelId2 = 2;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId1 );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        assertThrows( IllegalStateException.class, () -> nodeLabels.remove( labelId2, nodeStore ) );
    }

    @Test
    void removingNonExistentLabelInDynamicRecordsShouldFail()
    {
        // GIVEN
        long[] labels = oneByteLongs( 20 );
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, labels );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        assertThrows( IllegalStateException.class, () -> nodeLabels.remove( 123456, nodeStore ) );
    }

    @Test
    void shouldReallocateSomeOfPreviousDynamicRecords()
    {
        // GIVEN
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, oneByteLongs( 5 ) );
        Set<DynamicRecord> initialRecords = Iterables.asUniqueSet( node.getDynamicLabelRecords() );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        Set<DynamicRecord> reallocatedRecords = Iterables.asUniqueSet( nodeLabels.put( fourByteLongs( 100 ), nodeStore,
            nodeStore.getDynamicLabelStore() ) );

        // THEN
        assertTrue( reallocatedRecords.containsAll( initialRecords ) );
        assertTrue( reallocatedRecords.size() > initialRecords.size() );
    }

    @Test
    void shouldReallocateAllOfPreviousDynamicRecordsAndThenSome()
    {
        // GIVEN
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, fourByteLongs( 100 ) );
        Set<DynamicRecord> initialRecords = Iterables.asSet( cloned( node.getDynamicLabelRecords(), DynamicRecord.class ) );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        Set<DynamicRecord> reallocatedRecords = Iterables.asUniqueSet( nodeLabels.put( fourByteLongs( 5 ), nodeStore,
            nodeStore.getDynamicLabelStore() ) );

        // THEN
        assertTrue( idsOf( initialRecords ).containsAll( idsOf( used( reallocatedRecords ) ) ),
                "initial:" + initialRecords + ", reallocated:" + reallocatedRecords );
        assertTrue( used( reallocatedRecords ).size() < initialRecords.size() );
    }

    /*
     * There was this issue that DynamicNodeLabels#add would consider even unused dynamic records when
     * reading existing label ids before making the change. Previously this would create a duplicate
     * last label id (the one formerly being in the second record).
     *
     * This randomized test found this issue every time when it existed and it will potentially find other
     * unforeseen issues as well.
     */
    @Test
    void shouldHandleRandomAddsAndRemoves()
    {
        // GIVEN
        Set<Integer> key = new HashSet<>();
        NodeRecord node = new NodeRecord( 0 );
        node.setInUse( true );

        // WHEN
        for ( int i = 0; i < 100_000; i++ )
        {
            NodeLabels labels = NodeLabelsField.parseLabelsField( node );
            int labelId = random.nextInt( 200 );
            if ( random.nextBoolean() )
            {
                if ( !key.contains( labelId ) )
                {
                    labels.add( labelId, nodeStore, nodeStore.getDynamicLabelStore() );
                    key.add( labelId );
                }
            }
            else
            {
                if ( key.remove( labelId ) )
                {
                    labels.remove( labelId, nodeStore );
                }
            }
        }

        // THEN
        NodeLabels labels = NodeLabelsField.parseLabelsField( node );
        long[] readLabelIds = labels.get( nodeStore );
        for ( long labelId : readLabelIds )
        {
            assertTrue( key.remove( (int) labelId ), "Found an unexpected label " + labelId );
        }
        assertTrue( key.isEmpty() );
    }

    private long dynamicLabelsLongRepresentation( Iterable<DynamicRecord> records )
    {
        return 0x8000000000L | Iterables.first( records ).getId();
    }

    private long inlinedLabelsLongRepresentation( long... labelIds )
    {
        long header = (long) labelIds.length << 36;
        byte bitsPerLabel = (byte) (36 / labelIds.length);
        Bits bits = bits( 5 );
        for ( long labelId : labelIds )
        {
            bits.put( labelId, bitsPerLabel );
        }
        return header | bits.getLongs()[0];
    }

    private NodeRecord nodeRecordWithInlinedLabels( long... labels )
    {
        NodeRecord node = new NodeRecord( 0, false, 0, 0 );
        if ( labels.length > 0 )
        {
            node.setLabelField( inlinedLabelsLongRepresentation( labels ), Collections.emptyList() );
        }
        return node;
    }

    private NodeRecord nodeRecordWithDynamicLabels( NodeStore nodeStore, long... labels )
    {
        return nodeRecordWithDynamicLabels( 0, nodeStore, labels );
    }

    private NodeRecord nodeRecordWithDynamicLabels( long nodeId, NodeStore nodeStore, long... labels )
    {
        NodeRecord node = new NodeRecord( nodeId, false, 0, 0 );
        Collection<DynamicRecord> initialRecords = allocateAndApply( nodeStore, node.getId(), labels );
        node.setLabelField( dynamicLabelsLongRepresentation( initialRecords ), initialRecords );
        return node;
    }

    private Collection<DynamicRecord> allocateAndApply( NodeStore nodeStore, long nodeId, long[] longs )
    {
        Collection<DynamicRecord> records = DynamicNodeLabels.allocateRecordsForDynamicLabels( nodeId, longs, nodeStore.getDynamicLabelStore() );
        nodeStore.updateDynamicLabelRecords( records, IdUpdateListener.DIRECT );
        return records;
    }

    private long[] oneByteLongs( int numberOfLongs )
    {
        long[] result = new long[numberOfLongs];
        for ( int i = 0; i < numberOfLongs; i++ )
        {
            result[i] = 255 - i;
        }
        Arrays.sort( result );
        return result;
    }

    private long[] fourByteLongs( int numberOfLongs )
    {
        long[] result = new long[numberOfLongs];
        for ( int i = 0; i < numberOfLongs; i++ )
        {
            result[i] = Integer.MAX_VALUE - i;
        }
        Arrays.sort( result );
        return result;
    }

    private Set<DynamicRecord> used( Set<DynamicRecord> reallocatedRecords )
    {
        Set<DynamicRecord> used = new HashSet<>();
        for ( DynamicRecord record : reallocatedRecords )
        {
            if ( record.inUse() )
            {
                used.add( record );
            }
        }
        return used;
    }

    private static <T extends AbstractBaseRecord> Iterable<T> cloned( Iterable<T> items, final Class<T> itemClass )
    {
        return Iterables.map( obj -> itemClass.cast( obj.copy() ), items );
    }

    private static LongSet idsOf( Set<DynamicRecord> records )
    {
        MutableLongSet ids = LongSets.mutable.empty();
        for ( DynamicRecord record : records )
        {
            ids.add( record.getId() );
        }
        return ids;
    }
}
