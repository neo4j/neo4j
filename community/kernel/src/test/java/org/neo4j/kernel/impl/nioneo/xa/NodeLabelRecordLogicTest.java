/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.EphemeralFileSystemRule;

public class NodeLabelRecordLogicTest
{
    @Test
    public void shouldInlineOneLabel() throws Exception
    {
        // GIVEN
        long labelId = 10;
        NodeRecord node = nodeRecordWithInlinedLabels();
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( node, null );
        
        // WHEN
        manipulator.add( labelId );

        // THEN
        assertEquals( inlinedLabelsLongRepresentation( labelId ), node.getLabelField() );
    }

    @Test
    public void shouldInlineTwoSmallLabels() throws Exception
    {
        // GIVEN
        long labelId1 = 10, labelId2 = 30;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId1 );
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( node, null );
        
        // WHEN
        manipulator.add( labelId2 );

        // THEN
        assertEquals( inlinedLabelsLongRepresentation( labelId1, labelId2 ), node.getLabelField() );
    }

    @Test
    public void shouldInlineThreeSmallLabels() throws Exception
    {
        // GIVEN
        long labelId1 = 10, labelId2 = 30, labelId3 = 4095;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId1, labelId2 );
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( node, null );
        
        // WHEN
        manipulator.add( labelId3 );

        // THEN
        assertEquals( inlinedLabelsLongRepresentation( labelId1, labelId2, labelId3 ), node.getLabelField() );
    }
    
    @Test
    public void shouldInlineFourSmallLabels() throws Exception
    {
        // GIVEN
        long labelId1 = 10, labelId2 = 30, labelId3 = 45, labelId4 = 60;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId1, labelId2, labelId3 );
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( node, null );
        
        // WHEN
        manipulator.add( labelId4 );

        // THEN
        assertEquals( inlinedLabelsLongRepresentation( labelId1, labelId2, labelId3, labelId4 ), node.getLabelField() );
    }
    
    @Test
    public void shouldInlineFiveSmallLabels() throws Exception
    {
        // GIVEN
        long labelId1 = 10, labelId2 = 30, labelId3 = 45, labelId4 = 60, labelId5 = 61;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId1, labelId2, labelId3, labelId4 );
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( node, null );
        
        // WHEN
        manipulator.add( labelId5 );

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
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( node, nodeStore );
        
        // WHEN
        Iterable<DynamicRecord> changedDynamicRecords = manipulator.add( labelId3 );

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
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, oneByteLongs( 57 ) );
        Collection<DynamicRecord> initialRecords = node.getDynamicLabelRecords();
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( node, nodeStore );

        // WHEN
        Set<DynamicRecord> changedDynamicRecords = asSet( manipulator.add( 1 ) );

        // THEN
        assertTrue( changedDynamicRecords.containsAll( initialRecords ) );
        assertEquals( initialRecords.size()+1, changedDynamicRecords.size() );
    }
    
    @Test
    public void twoDynamicRecordsShouldShrinkToOneWhenRemoving() throws Exception
    {
        // GIVEN
        // will occupy 61B of data, i.e. just two dynamic records
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, oneByteLongs( 58 ) );
        Collection<DynamicRecord> initialRecords = node.getDynamicLabelRecords();
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( node, nodeStore );

        // WHEN
        List<DynamicRecord> changedDynamicRecords = addToCollection(
                manipulator.remove( 255 /*Initial labels go from 255 and down to 255-58*/ ),
                new ArrayList<DynamicRecord>() );

        // THEN
        assertEquals( initialRecords, changedDynamicRecords );
        assertTrue( changedDynamicRecords.get( 0 ).inUse() );
        assertFalse( changedDynamicRecords.get( 1 ).inUse() );
    }
    
    @Test
    public void oneDynamicRecordShouldShrinkIntoInlinedWhenRemoving() throws Exception
    {
        // GIVEN
        NodeRecord node = nodeRecordWithDynamicLabels( nodeStore, oneByteLongs( 5 ) );
        Collection<DynamicRecord> initialRecords = node.getDynamicLabelRecords();
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( node, nodeStore );

        // WHEN
        Collection<DynamicRecord> changedDynamicRecords = asCollection( manipulator.remove( 255 ) );

        // THEN
        assertEquals( initialRecords, changedDynamicRecords );
        assertFalse( single( changedDynamicRecords ).inUse() );
        assertEquals( inlinedLabelsLongRepresentation( 251, 252, 253, 254 ), node.getLabelField() );
    }
    
    @Test
    public void maximumOfEightInlinedLabels() throws Exception
    {
        // GIVEN
        long[] labels = new long[] {0, 1, 2, 3, 4, 5, 6};
        NodeRecord node = nodeRecordWithInlinedLabels( labels );
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( node, nodeStore );

        // WHEN
        Iterable<DynamicRecord> changedDynamicRecords = manipulator.add( 23 );

        // THEN
        assertEquals( dynamicLabelsLongRepresentation( changedDynamicRecords ), node.getLabelField() );
        assertEquals( 1, count( changedDynamicRecords ) );
    }
    
    @Test
    public void addingAnAlreadyAddedLabelWhenLabelsAreInlinedShouldFail() throws Exception
    {
        // GIVEN
        long labelId = 1;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId );
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( node, nodeStore );

        // WHEN
        try
        {
            manipulator.add( labelId );
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
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( node, nodeStore );

        // WHEN
        try
        {
            manipulator.add( labels[0] );
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
        long labelId1 = 1, labelId2 = 2;
        NodeRecord node = nodeRecordWithInlinedLabels( labelId1 );
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( node, nodeStore );

        // WHEN
        try
        {
            manipulator.remove( labelId2 );
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
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( node, nodeStore );

        // WHEN
        try
        {
            manipulator.remove( 123456L );
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
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( node, nodeStore );

        // WHEN
        Set<DynamicRecord> reallocatedRecords = asUniqueSet( manipulator.set( fourByteLongs( 100 ) ) );

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
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( node, nodeStore );
        
        // WHEN
        Set<DynamicRecord> reallocatedRecords = asUniqueSet( manipulator.set( fourByteLongs( 5 ) ) );

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
            bits.put( labelId, bitsPerLabel );
        return header|bits.getLongs()[0];
    }
    
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private NodeStore nodeStore;
    
    @Before
    public void startUp()
    {
        StoreFactory storeFactory = new StoreFactory( new Config(), new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), fs.get(), StringLogger.SYSTEM,
                new DefaultTxHook() );
        File storeFile = new File( "store" );
        storeFactory.createNodeStore( storeFile );
        nodeStore = storeFactory.newNodeStore( storeFile );
    }
    
    @After
    public void cleanUp()
    {
        if ( nodeStore != null )
            nodeStore.close();
    }

    private NodeRecord nodeRecordWithInlinedLabels( long... labels )
    {
        NodeRecord node = new NodeRecord( 0, 0, 0 );
        if ( labels.length > 0 )
            node.setLabelField( inlinedLabelsLongRepresentation( labels ) );
        return node;
    }
    
    private NodeRecord nodeRecordWithDynamicLabels( NodeStore nodeStore, long... labels )
    {
        Collection<DynamicRecord> initialRecords = allocateAndApply( nodeStore, labels );
        NodeRecord node = new NodeRecord( 0, 0, 0 );
        node.setLabelField( dynamicLabelsLongRepresentation( initialRecords ), initialRecords );
        return node;
    }
    
    private Collection<DynamicRecord> allocateAndApply( NodeStore nodeStore, long[] longs )
    {
        Collection<DynamicRecord> records = nodeStore.allocateRecordsForDynamicLabels( longs );
        nodeStore.updateDynamicLabelRecords( records );
        return records;
    }

    private long[] oneByteLongs( int numberOfLongs )
    {
        long[] result = new long[numberOfLongs];
        for ( int i = 0; i < numberOfLongs; i++ )
            result[i] = 255-i;
        Arrays.sort( result );
        return result;
    }

    private long[] fourByteLongs( int numberOfLongs )
    {
        long[] result = new long[numberOfLongs];
        for ( int i = 0; i < numberOfLongs; i++ )
            result[i] = Integer.MAX_VALUE-i;
        Arrays.sort( result );
        return result;
    }

    private Set<DynamicRecord> used( Set<DynamicRecord> reallocatedRecords )
    {
        Set<DynamicRecord> used = new HashSet<DynamicRecord>();
        for ( DynamicRecord record : reallocatedRecords )
            if ( record.inUse() )
                used.add( record );
        return used;
    }
}
