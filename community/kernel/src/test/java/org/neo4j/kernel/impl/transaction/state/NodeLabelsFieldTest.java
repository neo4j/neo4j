/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.asUniqueSet;
import static org.neo4j.helpers.collection.IteratorUtil.cloned;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.helpers.collection.IteratorUtil.single;

import static org.neo4j.kernel.impl.util.Bits.bits;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;

public class NodeLabelsFieldTest
{

    private NeoStores neoStores;

    @Test
    public void shouldInlineOneLabel() throws Exception
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
    public void shouldInlineOneLabelWithHighId() throws Exception
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
    public void shouldInlineTwoSmallLabels() throws Exception
    {
        // GIVEN
        long labelId1 = 10, labelId2 = 30;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId1 );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        nodeLabels.add( labelId2, null, null );

        // THEN
        assertEquals( inlinedLabelsLongRepresentation( labelId1, labelId2 ), node.getLabelField() );
    }

    @Test
    public void shouldInlineThreeSmallLabels() throws Exception
    {
        // GIVEN
        long labelId1 = 10, labelId2 = 30, labelId3 = 4095;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId1, labelId2 );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        nodeLabels.add( labelId3, null, null );

        // THEN
        assertEquals( inlinedLabelsLongRepresentation( labelId1, labelId2, labelId3 ), node.getLabelField() );
    }

    @Test
    public void shouldInlineFourSmallLabels() throws Exception
    {
        // GIVEN
        long labelId1 = 10, labelId2 = 30, labelId3 = 45, labelId4 = 60;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId1, labelId2, labelId3 );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        nodeLabels.add( labelId4, null, null );

        // THEN
        assertEquals( inlinedLabelsLongRepresentation( labelId1, labelId2, labelId3, labelId4 ), node.getLabelField() );
    }

    @Test
    public void shouldInlineFiveSmallLabels() throws Exception
    {
        // GIVEN
        long labelId1 = 10, labelId2 = 30, labelId3 = 45, labelId4 = 60, labelId5 = 61;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId1, labelId2, labelId3, labelId4 );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        nodeLabels.add( labelId5, null, null );

        // THEN
        assertEquals( inlinedLabelsLongRepresentation( labelId1, labelId2, labelId3, labelId4, labelId5 ),
                node.getLabelField() );
    }

    @Test
    public void shouldSpillOverToDynamicRecordIfExceedsInlinedSpace() throws Exception
    {
        // GIVEN -- the upper limit for a label ID for 3 labels would be 36b/3 - 1 = 12b - 1 = 4095
        long labelId1 = 10, labelId2 = 30, labelId3 = 4096;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId1, labelId2 );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        Collection<DynamicRecord> changedDynamicRecords = nodeLabels.add( labelId3, nodeStore,
                nodeStore.getDynamicLabelStore() );

        // THEN
        assertEquals( 1, count( changedDynamicRecords ) );
        assertEquals( dynamicLabelsLongRepresentation( changedDynamicRecords ), node.getLabelField() );
        assertTrue( Arrays.equals( new long[] {labelId1, labelId2, labelId3},
                nodeStore.getDynamicLabelsArray( changedDynamicRecords ) ) );
    }

    @Test
    public void oneDynamicRecordShouldExtendIntoAnAdditionalIfTooManyLabels() throws Exception
    {
        // GIVEN
        // will occupy 60B of data, i.e. one dynamic record
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, oneByteLongs( 56 ) );
        Collection<DynamicRecord> initialRecords = node.getDynamicLabelRecords();
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );


        // WHEN
        Set<DynamicRecord> changedDynamicRecords = asSet( nodeLabels.add( 1, nodeStore, nodeStore.getDynamicLabelStore() ) );

        // THEN
        assertTrue( changedDynamicRecords.containsAll( initialRecords ) );
        assertEquals( initialRecords.size()+1, changedDynamicRecords.size() );
    }

    @Test
    public void oneDynamicRecordShouldStoreItsOwner() throws Exception
    {
        // GIVEN
        // will occupy 60B of data, i.e. one dynamic record
        Long nodeId = 24l;
        NodeRecord node = nodeRecordWithDynamicLabels( nodeId, nodeStore, oneByteLongs(56) );
        Collection<DynamicRecord> initialRecords = node.getDynamicLabelRecords();

        // WHEN
        Pair<Long,long[]> pair = nodeStore.getDynamicLabelsArrayAndOwner( initialRecords );

        // THEN
        assertEquals( nodeId,  pair.first() );
    }

    @Test
    public void twoDynamicRecordsShouldShrinkToOneWhenRemoving() throws Exception
    {
        // GIVEN
        // will occupy 61B of data, i.e. just two dynamic records
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, oneByteLongs( 57 ) );
        Collection<DynamicRecord> initialRecords = node.getDynamicLabelRecords();
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        List<DynamicRecord> changedDynamicRecords = addToCollection(
                nodeLabels.remove( 255 /*Initial labels go from 255 and down to 255-58*/, nodeStore ),
                new ArrayList<DynamicRecord>() );

        // THEN
        assertEquals( initialRecords, changedDynamicRecords );
        assertTrue( changedDynamicRecords.get( 0 ).inUse() );
        assertFalse( changedDynamicRecords.get( 1 ).inUse() );
    }

    @Test
    public void twoDynamicRecordsShouldShrinkToOneWhenRemovingWithoutChangingItsOwner() throws Exception
    {
        // GIVEN
        // will occupy 61B of data, i.e. just two dynamic records
        Long nodeId = 42l;
        NodeRecord node = nodeRecordWithDynamicLabels( nodeId, nodeStore, oneByteLongs( 57 ) );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        List<DynamicRecord> changedDynamicRecords = addToCollection(
                nodeLabels.remove( 255 /*Initial labels go from 255 and down to 255-58*/, nodeStore ),
                new ArrayList<DynamicRecord>() );

        // WHEN
        Pair<Long,long[]> changedPair = nodeStore.getDynamicLabelsArrayAndOwner( changedDynamicRecords );

        // THEN
        assertEquals( nodeId,  changedPair.first() );
    }

    @Test
    public void oneDynamicRecordShouldShrinkIntoInlinedWhenRemoving() throws Exception
    {
        // GIVEN
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, oneByteLongs( 5 ) );
        Collection<DynamicRecord> initialRecords = node.getDynamicLabelRecords();
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        Collection<DynamicRecord> changedDynamicRecords = asCollection( nodeLabels.remove( 255, nodeStore ) );

        // THEN
        assertEquals( initialRecords, changedDynamicRecords );
        assertFalse( single( changedDynamicRecords ).inUse() );
        assertEquals( inlinedLabelsLongRepresentation( 251, 252, 253, 254 ), node.getLabelField() );
    }

    @Test
    public void shouldReadIdOfDynamicRecordFromDynamicLabelsField() throws Exception
    {
        // GIVEN
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, oneByteLongs( 5 ) );
        DynamicRecord dynamicRecord = node.getDynamicLabelRecords().iterator().next();

        // WHEN
        long dynRecordId = NodeLabelsField.firstDynamicLabelRecordId( node.getLabelField() );

        // THEN
        assertEquals( dynamicRecord.getLongId(), dynRecordId );
    }

    @Test
    public void shouldReadNullDynamicRecordFromInlineLabelsField() throws Exception
    {
        // GIVEN
        NodeRecord node = nodeRecordWithInlinedLabels( 23l );

        // WHEN
        boolean isDynamicReference = NodeLabelsField.fieldPointsToDynamicRecordOfLabels( node.getLabelField() );

        // THEN
        assertFalse( isDynamicReference );
    }

    @Test
    public void maximumOfSevenInlinedLabels() throws Exception
    {
        // GIVEN
        long[] labels = new long[] {0, 1, 2, 3, 4, 5, 6};
        NodeRecord node = nodeRecordWithInlinedLabels( labels );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        Iterable<DynamicRecord> changedDynamicRecords = nodeLabels.add( 23, nodeStore, nodeStore.getDynamicLabelStore() );

        // THEN
        assertEquals( dynamicLabelsLongRepresentation( changedDynamicRecords ), node.getLabelField() );
        assertEquals( 1, count( changedDynamicRecords ) );
    }

    @Test
    public void addingAnAlreadyAddedLabelWhenLabelsAreInlinedShouldFail() throws Exception
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
    public void addingAnAlreadyAddedLabelWhenLabelsAreInDynamicRecordsShouldFail() throws Exception
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
    public void removingNonExistentInlinedLabelShouldFail() throws Exception
    {
        // GIVEN
        int labelId1 = 1, labelId2 = 2;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId1 );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        try
        {
            nodeLabels.remove( labelId2, nodeStore );
            fail( "Should have thrown exception" );
        }
        catch ( IllegalStateException e )
        {
            // THEN
        }
    }

    @Test
    public void removingNonExistentLabelInDynamicRecordsShouldFail() throws Exception
    {
        // GIVEN
        long[] labels = oneByteLongs( 20 );
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, labels );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        try
        {
            nodeLabels.remove( 123456, nodeStore );
            fail( "Should have thrown exception" );
        }
        catch ( IllegalStateException e )
        {
            // THEN
        }
    }

    @Test
    public void shouldReallocateSomeOfPreviousDynamicRecords() throws Exception
    {
        // GIVEN
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, oneByteLongs( 5 ) );
        Set<DynamicRecord> initialRecords = asUniqueSet( node.getDynamicLabelRecords() );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        Set<DynamicRecord> reallocatedRecords = asUniqueSet( nodeLabels.put( fourByteLongs( 100 ), nodeStore,
                nodeStore.getDynamicLabelStore() ) );

        // THEN
        assertTrue( reallocatedRecords.containsAll( initialRecords ) );
        assertTrue( reallocatedRecords.size() > initialRecords.size() );
    }

    @Test
    public void shouldReallocateAllOfPreviousDynamicRecordsAndThenSome() throws Exception
    {
        // GIVEN
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, fourByteLongs( 100 ) );
        Set<DynamicRecord> initialRecords = asSet( cloned( node.getDynamicLabelRecords(), DynamicRecord.class ) );
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );

        // WHEN
        Set<DynamicRecord> reallocatedRecords = asUniqueSet( nodeLabels.put( fourByteLongs( 5 ), nodeStore,
                nodeStore.getDynamicLabelStore() ) );

        // THEN
        assertTrue( "initial:" + initialRecords + ", reallocated:" + reallocatedRecords ,
                initialRecords.containsAll( used( reallocatedRecords ) ) );
        assertTrue( used( reallocatedRecords ).size() < initialRecords.size() );
    }

    private long dynamicLabelsLongRepresentation( Iterable<DynamicRecord> records )
    {
        return 0x8000000000L|first( records ).getId();
    }

    private long inlinedLabelsLongRepresentation( long... labelIds )
    {
        long header = (long)labelIds.length << 36;
        byte bitsPerLabel = (byte) (36/labelIds.length);
        Bits bits = bits( 5 );
        for ( long labelId : labelIds )
        {
            bits.put( labelId, bitsPerLabel );
        }
        return header|bits.getLongs()[0];
    }

    @ClassRule
    public static PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private NodeStore nodeStore;

    @Before
    public void startUp()
    {
        File storeDir = new File( "dir" );
        fs.get().mkdirs( storeDir );
        StoreFactory storeFactory = new StoreFactory( storeDir, new Config(), new DefaultIdGeneratorFactory( fs.get() ),
                pageCacheRule.getPageCache( fs.get() ), fs.get(), NullLogProvider.getInstance() );
        neoStores = storeFactory.openAllNeoStores( true );
        nodeStore = neoStores.getNodeStore();
    }

    @After
    public void cleanUp()
    {
        neoStores.close();
    }

    private NodeRecord nodeRecordWithInlinedLabels( long... labels )
    {
        NodeRecord node = new NodeRecord( 0, false, 0, 0 );
        if ( labels.length > 0 )
        {
            node.setLabelField( inlinedLabelsLongRepresentation( labels ), Collections.<DynamicRecord>emptyList() );
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
        Collection<DynamicRecord> records = nodeStore.allocateRecordsForDynamicLabels( nodeId, longs,
                IteratorUtil.<DynamicRecord>emptyIterator() );
        nodeStore.updateDynamicLabelRecords( records );
        return records;
    }

    private long[] oneByteLongs( int numberOfLongs )
    {
        long[] result = new long[numberOfLongs];
        for ( int i = 0; i < numberOfLongs; i++ )
        {
            result[i] = 255-i;
        }
        Arrays.sort( result );
        return result;
    }

    private long[] fourByteLongs( int numberOfLongs )
    {
        long[] result = new long[numberOfLongs];
        for ( int i = 0; i < numberOfLongs; i++ )
        {
            result[i] = Integer.MAX_VALUE-i;
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
}
