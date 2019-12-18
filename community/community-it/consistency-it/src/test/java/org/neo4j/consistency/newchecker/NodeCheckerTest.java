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
package org.neo4j.consistency.newchecker;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;

import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.DynamicConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.LabelScanConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.NodeConsistencyReport;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.index.label.LabelScanWriter;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.internal.helpers.collection.Iterables.first;
import static org.neo4j.internal.helpers.collection.Iterables.last;
import static org.neo4j.storageengine.api.NodeLabelUpdate.labelChanges;

class NodeCheckerTest extends CheckerTestBase
{
    private int label1;
    private int label2;
    private int label3;
    private int[] otherLabels;
    private int unusedLabel;

    @Override
    void initialData( KernelTransaction tx ) throws KernelException
    {
        TokenWrite tokenWrite = tx.tokenWrite();
        int[] labelIds = new int[300];
        for ( int i = 0; i < labelIds.length; i++ )
        {
            labelIds[i] = tokenWrite.labelGetOrCreateForName( String.valueOf( i ) );
        }
        Arrays.sort( labelIds );
        label1 = labelIds[0];
        label2 = labelIds[1];
        label3 = labelIds[2];
        otherLabels = Arrays.copyOfRange( labelIds, 3, labelIds.length );
        unusedLabel = labelIds[labelIds.length - 1] + 99;
    }

    void testReportLabelInconsistency( Consumer<NodeConsistencyReport> report, int... labels ) throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            // (N) w/ some labels
            node( nodeStore.nextId(), NULL, NULL, labels );
        }

        // when
        check();

        // then
        expect( NodeConsistencyReport.class, report );
    }

    @Test
    void shouldReportLabelNotInUse() throws Exception
    {
        testReportLabelInconsistency( report -> report.labelNotInUse( any() ), label1, unusedLabel );
    }

    @Test
    void shouldReportLabelDuplicate() throws Exception
    {
        testReportLabelInconsistency( report -> report.labelDuplicate( anyInt() ), label1, label1, label2 );
    }

    @Test
    void shouldReportLabelsOutOfOrder() throws Exception
    {
        testReportLabelInconsistency( report -> report.labelsOutOfOrder( anyLong(), anyLong() ), label3, label1, label2 );
    }

    @Test
    void shouldReportNodeNotInUseOnEmptyStore() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            // Label index having (N) which is not in use in the store
            try ( LabelScanWriter writer = labelIndex.newWriter() )
            {
                writer.write( labelChanges( nodeStore.nextId(), EMPTY_LONG_ARRAY, new long[]{label1} ) );
            }
        }

        // when
        check();

        // then
        expect( LabelScanConsistencyReport.class, report -> report.nodeNotInUse( any() ) );
    }

    @Test
    void shouldReportNodeNotInUse() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            // A couple of nodes w/ correct label indexing
            try ( LabelScanWriter writer = labelIndex.newWriter() )
            {
                for ( int i = 0; i < 10; i++ )
                {
                    long nodeId = node( nodeStore.nextId(), NULL, NULL, label1 );
                    writer.write( labelChanges( nodeId, EMPTY_LONG_ARRAY, new long[]{label1} ) );
                }
            }

            // Label index having (N) which is not in use in the store
            try ( LabelScanWriter writer = labelIndex.newWriter() )
            {
                writer.write( labelChanges( nodeStore.nextId(), EMPTY_LONG_ARRAY, new long[]{label1} ) );
            }
        }

        // when
        check();

        // then
        expect( LabelScanConsistencyReport.class, report -> report.nodeNotInUse( any() ) );
    }

    @Test
    void shouldReportDynamicRecordChainCycle() throws Exception
    {
        // (N)────>(P1)──...──>(P2)
        //           ▲──────────┘
        testDynamicRecordChain( node ->
        {
            Collection<DynamicRecord> dynamicLabelRecords = node.getDynamicLabelRecords();
            last( dynamicLabelRecords ).setNextBlock( first( dynamicLabelRecords ).getId() );
        }, NodeConsistencyReport.class, report -> report.dynamicRecordChainCycle( any() ) );
    }

    @Test
    void shouldReportFirstDynamicLabelRecordNotInUse() throws Exception
    {
        // (N)────> X
        testDynamicRecordChain( node ->
        {
            Collection<DynamicRecord> dynamicLabelRecords = node.getDynamicLabelRecords();
            first( dynamicLabelRecords ).setInUse( false );
        }, NodeConsistencyReport.class, report -> report.dynamicLabelRecordNotInUse( any() ) );
    }

    @Test
    void shouldReportConsecutiveDynamicLabelRecordNotInUse() throws Exception
    {
        // (N)────>(L)──...──> X
        testDynamicRecordChain( node ->
        {
            Collection<DynamicRecord> dynamicLabelRecords = node.getDynamicLabelRecords();
            last( dynamicLabelRecords ).setInUse( false );
        }, NodeConsistencyReport.class, report -> report.dynamicLabelRecordNotInUse( any() ) );
    }

    @Test
    void shouldReportEmptyDynamicLabelRecord() throws Exception
    {
        // (N)────>(L1)─...─>(LN)
        //                    *empty
        testDynamicRecordChain( node ->
                last( node.getDynamicLabelRecords() ).setData( DynamicRecord.NO_DATA ), DynamicConsistencyReport.class,
                DynamicConsistencyReport::emptyBlock );
    }

    @Test
    void shouldReportRecordNotFullReferencesNext() throws Exception
    {
        // (N)────>(L1)───────>(L2)
        //           *not full
        testDynamicRecordChain( node ->
        {
            DynamicRecord first = first( node.getDynamicLabelRecords() );
            first.setData( Arrays.copyOf( first.getData(), first.getLength() / 2 ) );
        }, DynamicConsistencyReport.class, DynamicConsistencyReport::recordNotFullReferencesNext );
    }

    private <T extends ConsistencyReport> void testDynamicRecordChain( Consumer<NodeRecord> vandal, Class<T> expectedReportClass, Consumer<T> report )
            throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            long nodeId = nodeStore.nextId();
            NodeRecord node = new NodeRecord( nodeId ).initialize( true, NULL, false, NULL, 0 );
            new InlineNodeLabels( node ).put( toLongs( otherLabels ), nodeStore, nodeStore.getDynamicLabelStore() );
            assertThat( node.getDynamicLabelRecords().size() ).isGreaterThanOrEqualTo( 2 );
            nodeStore.updateRecord( node );
            vandal.accept( node );
            nodeStore.updateRecord( node );
        }

        // when
        check();

        // then
        expect( expectedReportClass, report );
    }

    @Test
    void shouldReportNodeDoesNotHaveExpectedLabel() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            // (N) w/ label L
            // LabelIndex does not have the N:L entry
            long nodeId = node( nodeStore.nextId(), NULL, NULL );
            try ( LabelScanWriter writer = labelIndex.newWriter() )
            {
                writer.write( labelChanges( nodeId, EMPTY_LONG_ARRAY, new long[]{label1} ) );
            }
        }

        // when
        check();

        // then
        expect( LabelScanConsistencyReport.class, report -> report.nodeDoesNotHaveExpectedLabel( any(), anyLong() ) );
    }

    @Test
    void shouldReportNodeLabelNotInIndexFirstNode() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            // (N) w/ label L
            // LabelIndex does not have the N:L entry
            node( nodeStore.nextId(), NULL, NULL, label1 );
        }

        // when
        check();

        // then
        expect( LabelScanConsistencyReport.class, report -> report.nodeLabelNotInIndex( any(), anyLong() ) );
    }

    @Test
    void shouldReportNodeLabelNotInIndexMiddleNode() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            try ( LabelScanWriter writer = labelIndex.newWriter() )
            {
                for ( int i = 0; i < 20; i++ )
                {
                    long nodeId = node( nodeStore.nextId(), NULL, NULL, label1, label2 );
                    // node 10 missing label2 in index
                    writer.write( labelChanges( nodeId, EMPTY_LONG_ARRAY,
                            i == 10 ? new long[]{label1} : new long[]{label1, label2} ) );
                }
            }
        }

        // when
        check();

        // then
        expect( LabelScanConsistencyReport.class, report -> report.nodeLabelNotInIndex( any(), anyLong() ) );
    }

    @Test
    void shouldReportNodeLabelHigherThanInt() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            NodeRecord node = new NodeRecord( nodeStore.nextId() ).initialize( true, NULL, false, NULL, 0x171f5bd081L );
            nodeStore.updateRecord( node );
        }

        // when
        check();

        // then
        expect( NodeConsistencyReport.class, NodeConsistencyReport::illegalLabel );
    }

    // invalidLength of dynamic label record: (impossible, right?)

    private void check() throws Exception
    {
        NodeChecker checker = new NodeChecker( context(), noMandatoryProperties );
        checker.check( LongRange.range( 0, nodeStore.getHighId() ), true, true );
    }
}
