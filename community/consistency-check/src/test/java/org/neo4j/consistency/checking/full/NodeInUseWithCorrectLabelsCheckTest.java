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
package org.neo4j.consistency.checking.full;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccessStub;
import org.neo4j.consistency.store.synthetic.LabelScanDocument;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.allocator.ReusableRecordsAllocator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.consistency.checking.RecordCheckTestBase.inUse;
import static org.neo4j.consistency.checking.RecordCheckTestBase.notInUse;
import static org.neo4j.kernel.impl.store.DynamicNodeLabels.dynamicPointer;
import static org.neo4j.kernel.impl.store.LabelIdArray.prependNodeId;

public class NodeInUseWithCorrectLabelsCheckTest
{
    @Test
    public void shouldReportNodeNotInUse()
    {
        // given
        int nodeId = 42;
        long labelId = 7;

        ConsistencyReport.LabelScanConsistencyReport report =
                mock( ConsistencyReport.LabelScanConsistencyReport.class );
        NodeRecord node = notInUse( new NodeRecord( nodeId, false, 0, 0 ) );

        // when
        checker( new long[]{labelId}, true ).checkReference( null, node, engineFor( report ), null );

        // then
        verify( report ).nodeNotInUse( node );
    }

    @Test
    public void shouldReportNodeWithoutExpectedLabelWhenLabelsAreInlineBothDirections()
    {
        // given
        int nodeId = 42;
        long[] storeLabelIds = new long[] {7, 9};
        long[] indexLabelIds = new long[] {   9, 10};

        NodeRecord node = inUse( withInlineLabels( new NodeRecord( nodeId, false, 0, 0 ), storeLabelIds ) );

        ConsistencyReport.LabelScanConsistencyReport report =
                mock( ConsistencyReport.LabelScanConsistencyReport.class );

        // when
        checker( indexLabelIds, true ).checkReference( null, node, engineFor( report ), null );

        // then
        verify( report ).nodeDoesNotHaveExpectedLabel( node, 10 );
    }

    @Test
    public void shouldReportNodeWithoutExpectedLabelWhenLabelsAreInlineIndexToStore()
    {
        // given
        int nodeId = 42;
        long[] storeLabelIds = new long[] {7, 9};
        long[] indexLabelIds = new long[] {   9, 10};

        NodeRecord node = inUse( withInlineLabels( new NodeRecord( nodeId, false, 0, 0 ), storeLabelIds ) );

        ConsistencyReport.LabelScanConsistencyReport report =
                mock( ConsistencyReport.LabelScanConsistencyReport.class );

        // when
        checker( indexLabelIds, false ).checkReference( null, node, engineFor( report ), null );

        // then
        verify( report ).nodeDoesNotHaveExpectedLabel( node, 10 );
    }

    @Test
    public void shouldReportNodeWithoutExpectedLabelWhenLabelsAreDynamicBothDirections()
    {
        // given
        int nodeId = 42;
        long[] indexLabelIds = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        long[] storeLabelIds = {1, 2, 3, 4, 5, 6,    8, 9, 10, 11};

        RecordAccessStub recordAccess = new RecordAccessStub();
        NodeRecord node = inUse( withDynamicLabels( recordAccess, new NodeRecord( nodeId, false, 0, 0 ), storeLabelIds ) );

        ConsistencyReport.LabelScanConsistencyReport report =
                mock( ConsistencyReport.LabelScanConsistencyReport.class );

        // when
        CheckerEngine<LabelScanDocument, ConsistencyReport.LabelScanConsistencyReport> engine = recordAccess.engine( null, report );
        checker( indexLabelIds, true ).checkReference( null, node, engine, recordAccess );
        recordAccess.checkDeferred();

        // then
        verify( report ).nodeDoesNotHaveExpectedLabel( node, 7 );
        verify( report ).nodeLabelNotInIndex( node, 11 );
    }

    @Test
    public void shouldReportNodeWithoutExpectedLabelWhenLabelsAreDynamicIndexToStore()
    {
        // given
        int nodeId = 42;
        long[] indexLabelIds = {      3,          7,    9, 10};
        long[] storeLabelIds = {1, 2, 3, 4, 5, 6,    8, 9, 10};
        long missingLabelId = 7;

        RecordAccessStub recordAccess = new RecordAccessStub();
        NodeRecord node = inUse( withDynamicLabels( recordAccess, new NodeRecord( nodeId, false, 0, 0 ), storeLabelIds ) );

        ConsistencyReport.LabelScanConsistencyReport report =
                mock( ConsistencyReport.LabelScanConsistencyReport.class );

        // when
        CheckerEngine<LabelScanDocument, ConsistencyReport.LabelScanConsistencyReport> engine = recordAccess.engine( null, report );
        checker( indexLabelIds, true ).checkReference( null, node, engine, recordAccess );
        recordAccess.checkDeferred();

        // then
        verify( report ).nodeDoesNotHaveExpectedLabel( node, missingLabelId );
    }

    @Test
    public void reportNodeWithoutLabelsWhenLabelsAreInlined()
    {
        int nodeId = 42;
        long[] indexLabelIds = {3};
        long[] storeLabelIds = {};
        long missingLabelId = 3;

        RecordAccessStub recordAccess = new RecordAccessStub();
        NodeRecord node = inUse( withInlineLabels( new NodeRecord( nodeId, false, 0, 0 ), storeLabelIds ) );

        ConsistencyReport.LabelScanConsistencyReport report = mock( ConsistencyReport.LabelScanConsistencyReport.class );

        // when
        CheckerEngine<LabelScanDocument, ConsistencyReport.LabelScanConsistencyReport> engine = recordAccess.engine( null, report );
        checker( indexLabelIds, true ).checkReference( null, node, engine, recordAccess );
        recordAccess.checkDeferred();

        // then
        verify( report ).nodeDoesNotHaveExpectedLabel( node, missingLabelId );
    }

    @Test
    public void reportNodeWithoutLabelsWhenLabelsAreDynamic()
    {
        int nodeId = 42;
        long[] indexLabelIds = {3, 7, 9, 10};
        long[] storeLabelIds = {};
        long[] missingLabelIds = {3, 7, 9, 10};

        RecordAccessStub recordAccess = new RecordAccessStub();
        NodeRecord node = inUse( withDynamicLabels( recordAccess, new NodeRecord( nodeId, false, 0, 0 ), storeLabelIds ) );

        ConsistencyReport.LabelScanConsistencyReport report =
                mock( ConsistencyReport.LabelScanConsistencyReport.class );

        // when
        CheckerEngine<LabelScanDocument, ConsistencyReport.LabelScanConsistencyReport> engine = recordAccess.engine( null, report );
        checker( indexLabelIds, true ).checkReference( null, node, engine, recordAccess );
        recordAccess.checkDeferred();

        // then
        for ( long missingLabelId : missingLabelIds )
        {
            verify( report ).nodeDoesNotHaveExpectedLabel( node, missingLabelId );
        }
    }

    @Test
    public void shouldRemainSilentWhenEverythingIsInOrder()
    {
        // given
        int nodeId = 42;
        int labelId = 7;

        NodeRecord node = withInlineLabels( inUse( new NodeRecord( nodeId, false, 0, 0 ) ), labelId );

        ConsistencyReport.LabelScanConsistencyReport report =
                mock( ConsistencyReport.LabelScanConsistencyReport.class );

        // when
        checker( new long[]{labelId}, true ).checkReference( null, node, engineFor( report ), null );

        // then
        verifyNoMoreInteractions( report );
    }

    private NodeRecord withInlineLabels( NodeRecord nodeRecord, long... labelIds )
    {
        new InlineNodeLabels( nodeRecord ).put( labelIds, null, null );
        return nodeRecord;
    }

    private NodeRecord withDynamicLabels( RecordAccessStub recordAccess, NodeRecord nodeRecord, long... labelIds )
    {
        List<DynamicRecord> preAllocatedRecords = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            preAllocatedRecords.add( inUse( new DynamicRecord( i ) ) );
        }
        Collection<DynamicRecord> dynamicRecords = new ArrayList<>();
        DynamicArrayStore.allocateFromNumbers( dynamicRecords, prependNodeId( nodeRecord.getId(), labelIds ),
                new ReusableRecordsAllocator( 4, preAllocatedRecords ) );
        for ( DynamicRecord dynamicRecord : dynamicRecords )
        {
            recordAccess.addNodeDynamicLabels( dynamicRecord );
        }

        nodeRecord.setLabelField( dynamicPointer( dynamicRecords ), dynamicRecords );
        return nodeRecord;
    }

    private Engine engineFor( ConsistencyReport.LabelScanConsistencyReport report )
    {
        Engine engine = mock( Engine.class );
        when( engine.report() ).thenReturn( report );
        return engine;
    }

    private NodeInUseWithCorrectLabelsCheck<LabelScanDocument,ConsistencyReport.LabelScanConsistencyReport> checker(
            long[] expectedLabels, boolean checkStoreToIndex )
    {
        return new NodeInUseWithCorrectLabelsCheck<>( expectedLabels, checkStoreToIndex );
    }

    interface Engine extends CheckerEngine<LabelScanDocument, ConsistencyReport.LabelScanConsistencyReport>
    {
    }
}
