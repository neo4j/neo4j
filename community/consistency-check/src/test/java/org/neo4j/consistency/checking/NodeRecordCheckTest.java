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
package org.neo4j.consistency.checking;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.consistency.checking.NodeRecordCheck.LabelsField;
import org.neo4j.consistency.checking.NodeRecordCheck.RelationshipField;
import org.neo4j.consistency.checking.full.MandatoryProperties;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.function.Functions;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PreAllocatedRecords;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static java.util.Arrays.asList;

public class NodeRecordCheckTest
        extends RecordCheckTestBase<NodeRecord, ConsistencyReport.NodeConsistencyReport, NodeRecordCheck>
{
    public NodeRecordCheckTest()
    {
        super( new NodeRecordCheck( RelationshipField.NEXT_REL, LabelsField.LABELS,
                new PropertyChain<>( Functions.<NodeRecord,MandatoryProperties.Check<NodeRecord,ConsistencyReport.NodeConsistencyReport>>nullFunction() ) ),
                ConsistencyReport.NodeConsistencyReport.class, new int[0] );
    }

    @Test
    public void shouldNotReportAnythingForNodeNotInUse() throws Exception
    {
        // given
        NodeRecord node = notInUse( new NodeRecord( 42, false, 0, 0 ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingForNodeThatDoesNotReferenceOtherRecords() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, false, NONE, NONE ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldNotReportAnythingForNodeWithConsistentReferences() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, false, 7, 11 ) );
        add( inUse( new RelationshipRecord( 7, 42, 0, 0 ) ) );
        add( inUse( new PropertyRecord( 11 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportRelationshipNotInUse() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, false, 7, 11 ) );
        RelationshipRecord relationship = add( notInUse( new RelationshipRecord( 7, 0, 0, 0 ) ) );
        add( inUse( new PropertyRecord( 11 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).relationshipNotInUse( relationship );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyNotInUse() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, false, NONE, 11 ) );
        PropertyRecord property = add( notInUse( new PropertyRecord( 11 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).propertyNotInUse( property );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportPropertyNotFirstInChain() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, false, NONE, 11 ) );
        PropertyRecord property = add( inUse( new PropertyRecord( 11 ) ) );
        property.setPrevProp( 6 );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).propertyNotFirstInChain( property );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportRelationshipForOtherNodes() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, false, 7, NONE ) );
        RelationshipRecord relationship = add( inUse( new RelationshipRecord( 7, 1, 2, 0 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).relationshipForOtherNode( relationship );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportRelationshipNotFirstInSourceChain() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, false, 7, NONE ) );
        RelationshipRecord relationship = add( inUse( new RelationshipRecord( 7, 42, 0, 0 ) ) );
        relationship.setFirstPrevRel( 6 );
        relationship.setFirstInFirstChain( false );
        relationship.setSecondPrevRel( 8 );
        relationship.setFirstInSecondChain( false );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).relationshipNotFirstInSourceChain( relationship );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportRelationshipNotFirstInTargetChain() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, false, 7, NONE ) );
        RelationshipRecord relationship = add( inUse( new RelationshipRecord( 7, 0, 42, 0 ) ) );
        relationship.setFirstPrevRel( 6 );
        relationship.setFirstInFirstChain( false );
        relationship.setSecondPrevRel( 8 );
        relationship.setFirstInSecondChain( false );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).relationshipNotFirstInTargetChain( relationship );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportLoopRelationshipNotFirstInTargetAndSourceChains() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, false, 7, NONE ) );
        RelationshipRecord relationship = add( inUse( new RelationshipRecord( 7, 42, 42, 0 ) ) );
        relationship.setFirstPrevRel( 8 );
        relationship.setFirstInFirstChain( false );
        relationship.setSecondPrevRel( 8 );
        relationship.setFirstInSecondChain( false );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).relationshipNotFirstInSourceChain( relationship );
        verify( report ).relationshipNotFirstInTargetChain( relationship );
        verifyNoMoreInteractions( report );
    }

    @Test
    public void shouldReportLabelNotInUse() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, false, NONE, NONE ) );
        new InlineNodeLabels( node.getLabelField(), node ).add( 1, null, null );
        LabelTokenRecord labelRecordNotInUse = notInUse( new LabelTokenRecord( 1 ) );

        add( labelRecordNotInUse );
        add( node );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).labelNotInUse( labelRecordNotInUse );
    }

    @Test
    public void shouldReportDynamicLabelsNotInUse() throws Exception
    {
        // given
        long[] labelIds = createLabels( 100 );

        LabelTokenRecord labelRecordNotInUse = notInUse( new LabelTokenRecord( labelIds.length ) );
        add( labelRecordNotInUse );

        NodeRecord node = inUse( new NodeRecord( 42, false, NONE, NONE ) );
        add( node );

        DynamicRecord labelsRecord1 = inUse( array( new DynamicRecord( 1 ) ) );
        DynamicRecord labelsRecord2 = inUse( array( new DynamicRecord( 2 ) ) );
        Collection<DynamicRecord> labelRecords = asList( labelsRecord1, labelsRecord2 );

        labelIds[12] = labelIds.length;
        DynamicArrayStore.allocateFromNumbers( new ArrayList<DynamicRecord>(), labelIds, labelRecords.iterator(),
                new PreAllocatedRecords( 52 ) );
        assertDynamicRecordChain( labelsRecord1, labelsRecord2 );
        node.setLabelField( DynamicNodeLabels.dynamicPointer( labelRecords ), labelRecords );

        addNodeDynamicLabels( labelsRecord1 );
        addNodeDynamicLabels( labelsRecord2 );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).labelNotInUse( labelRecordNotInUse );
    }

    @Test
    public void shouldReportDuplicateLabels() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, false, NONE, NONE ) );
        new InlineNodeLabels( node.getLabelField(), node ).put( new long[]{1, 2, 1}, null, null );
        LabelTokenRecord label1 = inUse( new LabelTokenRecord( 1 ) );
        LabelTokenRecord label2 = inUse( new LabelTokenRecord( 2 ) );

        add( label1 );
        add( label2 );
        add( node );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).labelDuplicate( 1 );
    }

    @Test
    public void shouldReportDuplicateDynamicLabels() throws Exception
    {
        // given
        long[] labelIds = createLabels( 100 );

        NodeRecord node = inUse( new NodeRecord( 42, false, NONE, NONE ) );
        add( node );

        DynamicRecord labelsRecord1 = inUse( array( new DynamicRecord( 1 ) ) );
        DynamicRecord labelsRecord2 = inUse( array( new DynamicRecord( 2 ) ) );
        Collection<DynamicRecord> labelRecords = asList( labelsRecord1, labelsRecord2 );

        labelIds[12] = 11;
        DynamicArrayStore.allocateFromNumbers( new ArrayList<DynamicRecord>(), labelIds, labelRecords.iterator(),
                new PreAllocatedRecords( 52 ) );
        assertDynamicRecordChain( labelsRecord1, labelsRecord2 );
        node.setLabelField( DynamicNodeLabels.dynamicPointer( labelRecords ), labelRecords );

        addNodeDynamicLabels( labelsRecord1 );
        addNodeDynamicLabels( labelsRecord2 );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).labelDuplicate( 11 );
    }

    @Test
    public void shouldReportOutOfOrderLabels() throws Exception
    {
        // given
        final NodeRecord node = inUse( new NodeRecord( 42, false, NONE, NONE ) );
        // We need to do this override so we can put the labels unsorted, since InlineNodeLabels always sorts on insert
        new InlineNodeLabels( node.getLabelField(), node )
        {
            @Override
            public Collection<DynamicRecord> put( long[] labelIds, NodeStore nodeStore, DynamicRecordAllocator
                    allocator )
            {
                return putSorted(  node, labelIds, nodeStore, allocator );
            }
        }.put( new long[]{3, 1, 2}, null, null );
        LabelTokenRecord label1 = inUse( new LabelTokenRecord( 1 ) );
        LabelTokenRecord label2 = inUse( new LabelTokenRecord( 2 ) );
        LabelTokenRecord label3 = inUse( new LabelTokenRecord( 3 ) );

        add( label1 );
        add( label2 );
        add( label3 );
        add( node );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).labelsOutOfOrder( 3, 1 );
    }

    @Test
    public void shouldProperlyReportOutOfOrderLabelsThatAreFarAway() throws Exception
    {
        // given
        final NodeRecord node = inUse( new NodeRecord( 42, false, NONE, NONE ) );
        // We need to do this override so we can put the labels unsorted, since InlineNodeLabels always sorts on insert
        new InlineNodeLabels( node.getLabelField(), node )
        {
            @Override
            public Collection<DynamicRecord> put( long[] labelIds, NodeStore nodeStore, DynamicRecordAllocator
                    allocator )
            {
                return putSorted( node, labelIds, nodeStore, allocator );
            }
        }.put( new long[]{1, 18, 13, 14, 15, 16, 12}, null, null );
        LabelTokenRecord label1 = inUse( new LabelTokenRecord( 1 ) );
        LabelTokenRecord label12 = inUse( new LabelTokenRecord( 12 ) );
        LabelTokenRecord label13 = inUse( new LabelTokenRecord( 13 ) );
        LabelTokenRecord label14 = inUse( new LabelTokenRecord( 14 ) );
        LabelTokenRecord label15 = inUse( new LabelTokenRecord( 15 ) );
        LabelTokenRecord label16 = inUse( new LabelTokenRecord( 16 ) );
        LabelTokenRecord label18 = inUse( new LabelTokenRecord( 18 ) );

        add( label1 );
        add( label12 );
        add( label13 );
        add( label14 );
        add( label15 );
        add( label16 );
        add( label18 );
        add( node );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).labelsOutOfOrder( 18, 13 );
        verify( report ).labelsOutOfOrder( 16, 12 );
    }

    @Test
    public void shouldReportOutOfOrderDynamicLabels() throws Exception
    {
        // given
        long[] labelIds = createLabels( 100 );

        NodeRecord node = inUse( new NodeRecord( 42, false, NONE, NONE ) );
        add( node );

        DynamicRecord labelsRecord1 = inUse( array( new DynamicRecord( 1 ) ) );
        DynamicRecord labelsRecord2 = inUse( array( new DynamicRecord( 2 ) ) );
        Collection<DynamicRecord> labelRecords = asList( labelsRecord1, labelsRecord2 );

        long temp = labelIds[12];
        labelIds[12] = labelIds[11];
        labelIds[11] = temp;
        DynamicArrayStore.allocateFromNumbers( new ArrayList<DynamicRecord>(), labelIds, labelRecords.iterator(),
                new PreAllocatedRecords( 52 ) );
        assertDynamicRecordChain( labelsRecord1, labelsRecord2 );
        node.setLabelField( DynamicNodeLabels.dynamicPointer( labelRecords ), labelRecords );

        addNodeDynamicLabels( labelsRecord1 );
        addNodeDynamicLabels( labelsRecord2 );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).labelsOutOfOrder( labelIds[11], labelIds[12] );
    }

    @Test
    public void shouldDynamicLabelRecordsNotInUse() throws Exception
    {
        // given
        long[] labelIds = createLabels( 100 );

        NodeRecord node = inUse( new NodeRecord( 42, false, NONE, NONE ) );
        add( node );

        DynamicRecord labelsRecord1 = notInUse( array( new DynamicRecord( 1 ) ) );
        DynamicRecord labelsRecord2 = notInUse( array( new DynamicRecord( 2 ) ) );
        Collection<DynamicRecord> labelRecords = asList( labelsRecord1, labelsRecord2 );

        DynamicArrayStore.allocateFromNumbers( new ArrayList<DynamicRecord>(), labelIds,
                labelRecords.iterator(), new PreAllocatedRecords( 52 ) );
        assertDynamicRecordChain( labelsRecord1, labelsRecord2 );
        node.setLabelField( DynamicNodeLabels.dynamicPointer( labelRecords ), labelRecords );

        addNodeDynamicLabels( labelsRecord1 );
        addNodeDynamicLabels( labelsRecord2 );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).dynamicLabelRecordNotInUse( labelsRecord1 );
        verify( report ).dynamicLabelRecordNotInUse( labelsRecord2 );
    }

    private long[] createLabels( int labelCount )
    {
        long[] labelIds = new long[labelCount];
        for ( int i = 0; i < labelIds.length; i++ )
        {
            labelIds[i] = i;
            add( inUse( new LabelTokenRecord( i ) ) );
        }
        return labelIds;
    }

    private void assertDynamicRecordChain( DynamicRecord... records )
    {
        if ( records.length > 0)
        {
            for ( int i = 1; i < records.length; i++ )
            {
                assertEquals( records[i].getId(), records[i - 1].getNextBlock() );
            }
            assertTrue( Record.NO_NEXT_BLOCK.is( records[records.length - 1].getNextBlock() ) );
        }
    }

}
