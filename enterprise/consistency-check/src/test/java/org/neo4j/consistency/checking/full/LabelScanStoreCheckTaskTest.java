/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking.full;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;

import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccessStub;
import org.neo4j.consistency.store.synthetic.LabelScanDocument;
import org.neo4j.kernel.api.impl.index.LuceneNodeLabelRange;
import org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PreAllocatedRecords;
import org.neo4j.kernel.impl.nioneo.store.labels.InlineNodeLabels;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.consistency.checking.RecordCheckTestBase.inUse;
import static org.neo4j.consistency.checking.RecordCheckTestBase.notInUse;
import static org.neo4j.kernel.impl.nioneo.store.labels.DynamicNodeLabels.dynamicPointer;
import static org.neo4j.kernel.impl.nioneo.store.labels.LabelIdArray.prependNodeId;

public class LabelScanStoreCheckTaskTest
{
    @Test
    public void shouldReportNodeNotInUse() throws Exception
    {
        // given
        int nodeId = 42;

        ConsistencyReport.LabelScanConsistencyReport report =
                mock( ConsistencyReport.LabelScanConsistencyReport.class );
        LabelScanStoreCheckTask.NodeRecordCheck check = new LabelScanStoreCheckTask.NodeRecordCheck();
        NodeRecord node = notInUse( new NodeRecord( nodeId, 0, 0 ) );

        // when
        check.checkReference( null, node, engineFor( report ), null );

        // then
        verify( report ).nodeNotInUse( node );
    }

    @Test
    public void shouldReportNodeWithoutExpectedLabelWhenLabelsAreInline() throws Exception
    {
        // given
        int nodeId = 42;
        int documentId = 13;
        int labelId1 = 7;
        int labelId2 = 9;

        NodeRecord node = inUse( withInlineLabels( new NodeRecord( nodeId, 0, 0 ), labelId1 ) );
        LabelScanDocument document = document( documentId, new long[]{nodeId}, new long[][]{{labelId1, labelId2}} );

        ConsistencyReport.LabelScanConsistencyReport report =
                mock( ConsistencyReport.LabelScanConsistencyReport.class );
        LabelScanStoreCheckTask.NodeRecordCheck check = new LabelScanStoreCheckTask.NodeRecordCheck();

        // when
        check.checkReference( document, node, engineFor( report ), null );

        // then
        verify( report ).nodeDoesNotHaveExpectedLabel( node, labelId2 );
    }

    @Test
    public void shouldReportNodeWithoutExpectedLabelWhenLabelsAreDynamic() throws Exception
    {
        // given
        int nodeId = 42;
        int documentId = 13;
        long[] expectedLabelIds = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        long[] presentLabelIds = {1, 2, 3, 4, 5, 6, 8, 9, 10};
        long missingLabelId = 7;

        RecordAccessStub recordAccess = new RecordAccessStub();
        NodeRecord node = inUse( withDynamicLabels( recordAccess, new NodeRecord( nodeId, 0, 0 ), presentLabelIds ) );
        LabelScanDocument document = document( documentId, new long[]{nodeId}, new long[][] {expectedLabelIds} );

        ConsistencyReport.LabelScanConsistencyReport report =
                mock( ConsistencyReport.LabelScanConsistencyReport.class );
        LabelScanStoreCheckTask.NodeRecordCheck check = new LabelScanStoreCheckTask.NodeRecordCheck();

        // when
        CheckerEngine<LabelScanDocument, ConsistencyReport.LabelScanConsistencyReport> engine = recordAccess.engine( document, report );
        check.checkReference( document, node, engine, recordAccess );
        recordAccess.checkDeferred();

        // then
        verify( report ).nodeDoesNotHaveExpectedLabel( node, missingLabelId );
    }

    @Test
    public void shouldRemainSilentWhenEverythingIsInOrder() throws Exception
    {
        // given
        int nodeId = 42;
        int documentId = 13;
        int labelId = 7;

        NodeRecord node = withInlineLabels( inUse( new NodeRecord( nodeId, 0, 0 ) ), labelId );
        LabelScanDocument document = document( documentId, new long[]{nodeId}, new long[][]{{labelId}} );

        ConsistencyReport.LabelScanConsistencyReport report =
                mock( ConsistencyReport.LabelScanConsistencyReport.class );
        LabelScanStoreCheckTask.NodeRecordCheck check = new LabelScanStoreCheckTask.NodeRecordCheck();

        // when
        check.checkReference( document, node, engineFor( report ), null );

        // then
        verifyNoMoreInteractions( report );
    }

    private LabelScanDocument document( int documentId, long[] nodeIds, long[][] labelIds )
    {
        return new LabelScanDocument( new LuceneNodeLabelRange( documentId, nodeIds, labelIds ) );
    }

    private NodeRecord withInlineLabels( NodeRecord nodeRecord, long... labelIds )
    {
        new InlineNodeLabels( nodeRecord.getLabelField(), nodeRecord ).put( labelIds, null );
        return nodeRecord;
    }

    private NodeRecord withDynamicLabels( RecordAccessStub recordAccess, NodeRecord nodeRecord, long... labelIds )
    {
        List<DynamicRecord> preAllocatedRecords = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            preAllocatedRecords.add( inUse( new DynamicRecord( i ) ) );
        }
        Collection<DynamicRecord> dynamicRecords =
                DynamicArrayStore.allocateFromNumbers( prependNodeId( nodeRecord.getId(), labelIds ),
                        preAllocatedRecords.iterator(), new PreAllocatedRecords( 4 ) );
        for ( DynamicRecord dynamicRecord : dynamicRecords )
        {
            recordAccess.addNodeDynamicLabels( dynamicRecord );
        }

        nodeRecord.setLabelField( dynamicPointer( dynamicRecords ), dynamicRecords );
        return nodeRecord;
    }

    @SuppressWarnings("unchecked")
    private Engine engineFor( ConsistencyReport.LabelScanConsistencyReport report )
    {
        Engine engine = mock( Engine.class );
        when( engine.report() ).thenReturn( report );
        return engine;
    }

    interface Engine extends CheckerEngine<LabelScanDocument, ConsistencyReport.LabelScanConsistencyReport>
    {
    }
}
