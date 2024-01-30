/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checker;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.neo4j.internal.helpers.collection.Iterables.first;
import static org.neo4j.internal.helpers.collection.Iterables.last;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.DynamicConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.LabelScanConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.NodeConsistencyReport;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.cursor.StoreCursors;

class NodeCheckerTest extends CheckerTestBase {
    private int label1;
    private int label2;
    private int label3;
    private int[] otherLabels;
    private int unusedLabel;
    private int negativeLabel;

    @Override
    void initialData(KernelTransaction tx) throws KernelException {
        TokenWrite tokenWrite = tx.tokenWrite();
        int[] labelIds = new int[300];
        for (int i = 0; i < labelIds.length; i++) {
            labelIds[i] = tokenWrite.labelGetOrCreateForName(String.valueOf(i));
        }
        Arrays.sort(labelIds);
        label1 = labelIds[0];
        label2 = labelIds[1];
        label3 = labelIds[2];
        otherLabels = Arrays.copyOfRange(labelIds, 3, labelIds.length);
        unusedLabel = labelIds[labelIds.length - 1] + 99;
        negativeLabel = -1234;
    }

    void testReportLabelInconsistency(Consumer<NodeConsistencyReport> report, int... labels) throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            // (N) w/ some labels
            node(nodeStore.getIdGenerator().nextId(NULL_CONTEXT), NULL, NULL, labels);
        }

        // when
        check();

        // then
        expect(NodeConsistencyReport.class, report);
    }

    @Test
    void shouldReportLabelNotInUse() throws Exception {
        testReportLabelInconsistency(report -> report.labelNotInUse(any()), label1, unusedLabel);
    }

    @Test
    void shouldReportLabelDuplicate() throws Exception {
        testReportLabelInconsistency(report -> report.labelDuplicate(anyInt()), label1, label1, label2);
    }

    @Test
    void shouldReportLabelsOutOfOrder() throws Exception {
        testReportLabelInconsistency(report -> report.labelsOutOfOrder(anyInt(), anyInt()), label3, label1, label2);
    }

    @Test
    void shouldReportLabelNegativeLabel() throws Exception {
        testReportLabelInconsistency(NodeConsistencyReport::illegalLabel, negativeLabel);
    }

    @Test
    void shouldReportNodeNotInUseOnEmptyStore() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            // Label index having (N) which is not in use in the store
            try (IndexUpdater writer = labelIndexWriter()) {
                writer.process(IndexEntryUpdate.change(
                        nodeStore.getIdGenerator().nextId(NULL_CONTEXT),
                        IndexDescriptor.NO_INDEX,
                        EMPTY_INT_ARRAY,
                        new int[] {label1}));
            }
        }

        // when
        check();

        // then
        expect(LabelScanConsistencyReport.class, report -> report.nodeNotInUse(any()));
    }

    @Test
    void shouldReportNodeNotInUse() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            // A couple of nodes w/ correct label indexing
            IdGenerator idGenerator = nodeStore.getIdGenerator();
            try (IndexUpdater writer = labelIndexWriter()) {
                for (int i = 0; i < 10; i++) {
                    long nodeId = node(idGenerator.nextId(NULL_CONTEXT), NULL, NULL, label1);
                    writer.process(IndexEntryUpdate.change(
                            nodeId, IndexDescriptor.NO_INDEX, EMPTY_INT_ARRAY, new int[] {label1}));
                }
            }

            // Label index having (N) which is not in use in the store
            try (IndexUpdater writer = labelIndexWriter()) {
                writer.process(IndexEntryUpdate.change(
                        idGenerator.nextId(NULL_CONTEXT), IndexDescriptor.NO_INDEX, EMPTY_INT_ARRAY, new int[] {label1
                        }));
            }
        }

        // when
        check();

        // then
        expect(LabelScanConsistencyReport.class, report -> report.nodeNotInUse(any()));
    }

    @Test
    void shouldReportDynamicRecordChainCycle() throws Exception {
        // (N)────>(P1)──...──>(P2)
        //           ▲──────────┘
        testDynamicRecordChain(
                node -> {
                    Collection<DynamicRecord> dynamicLabelRecords = node.getDynamicLabelRecords();
                    last(dynamicLabelRecords)
                            .setNextBlock(first(dynamicLabelRecords).getId());
                },
                NodeConsistencyReport.class,
                report -> report.dynamicRecordChainCycle(any()));
    }

    @Test
    void shouldReportFirstDynamicLabelRecordNotInUse() throws Exception {
        // (N)────> X
        testDynamicRecordChain(
                node -> {
                    Collection<DynamicRecord> dynamicLabelRecords = node.getDynamicLabelRecords();
                    first(dynamicLabelRecords).setInUse(false);
                },
                NodeConsistencyReport.class,
                report -> report.dynamicLabelRecordNotInUse(any()));
    }

    @Test
    void shouldReportConsecutiveDynamicLabelRecordNotInUse() throws Exception {
        // (N)────>(L)──...──> X
        testDynamicRecordChain(
                node -> {
                    Collection<DynamicRecord> dynamicLabelRecords = node.getDynamicLabelRecords();
                    last(dynamicLabelRecords).setInUse(false);
                },
                NodeConsistencyReport.class,
                report -> report.dynamicLabelRecordNotInUse(any()));
    }

    @Test
    void shouldReportEmptyDynamicLabelRecord() throws Exception {
        // (N)────>(L1)─...─>(LN)
        //                    *empty
        testDynamicRecordChain(
                node -> last(node.getDynamicLabelRecords()).setData(DynamicRecord.NO_DATA),
                DynamicConsistencyReport.class,
                DynamicConsistencyReport::emptyBlock);
    }

    @Test
    void shouldReportRecordNotFullReferencesNext() throws Exception {
        // (N)────>(L1)───────>(L2)
        //           *not full
        testDynamicRecordChain(
                node -> {
                    DynamicRecord first = first(node.getDynamicLabelRecords());
                    first.setData(Arrays.copyOf(first.getData(), first.getLength() / 2));
                },
                DynamicConsistencyReport.class,
                DynamicConsistencyReport::recordNotFullReferencesNext);
    }

    private <T extends ConsistencyReport> void testDynamicRecordChain(
            Consumer<NodeRecord> vandal, Class<T> expectedReportClass, Consumer<T> report) throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            long nodeId = nodeStore.getIdGenerator().nextId(NULL_CONTEXT);
            NodeRecord node = new NodeRecord(nodeId).initialize(true, NULL, false, NULL, 0);
            new InlineNodeLabels(node)
                    .put(
                            otherLabels,
                            nodeStore,
                            allocatorProvider.allocator(StoreType.NODE_LABEL),
                            NULL_CONTEXT,
                            StoreCursors.NULL,
                            INSTANCE);
            assertThat(node.getDynamicLabelRecords().size()).isGreaterThanOrEqualTo(2);
            try (var storeCursor = storeCursors.writeCursor(NODE_CURSOR)) {
                nodeStore.updateRecord(node, storeCursor, NULL_CONTEXT, storeCursors);
            }
            vandal.accept(node);
            try (var storeCursor = storeCursors.writeCursor(NODE_CURSOR)) {
                nodeStore.updateRecord(node, storeCursor, NULL_CONTEXT, storeCursors);
            }
        }

        // when
        check();

        // then
        expect(expectedReportClass, report);
    }

    @Test
    void shouldReportNodeDoesNotHaveExpectedLabel() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            // (N) w/ label L
            // LabelIndex does not have the N:L entry
            long nodeId = node(nodeStore.getIdGenerator().nextId(NULL_CONTEXT), NULL, NULL);
            try (IndexUpdater writer = labelIndexWriter()) {
                writer.process(
                        IndexEntryUpdate.change(nodeId, IndexDescriptor.NO_INDEX, EMPTY_INT_ARRAY, new int[] {label1}));
            }
        }

        // when
        check();

        // then
        expect(LabelScanConsistencyReport.class, report -> report.nodeDoesNotHaveExpectedLabel(any(), anyInt()));
    }

    @Test
    void shouldReportNodeLabelNotInIndexFirstNode() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            // (N) w/ label L
            // LabelIndex does not have the N:L entry
            node(nodeStore.getIdGenerator().nextId(NULL_CONTEXT), NULL, NULL, label1);
        }

        // when
        check();

        // then
        expect(LabelScanConsistencyReport.class, report -> report.nodeLabelNotInIndex(any(), anyInt()));
    }

    @Test
    void shouldReportNodeLabelNotInIndexMiddleNode() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            try (IndexUpdater writer = labelIndexWriter()) {
                for (int i = 0; i < 20; i++) {
                    long nodeId = node(nodeStore.getIdGenerator().nextId(NULL_CONTEXT), NULL, NULL, label1, label2);
                    // node 10 missing label2 in index
                    writer.process(IndexEntryUpdate.change(
                            nodeId,
                            IndexDescriptor.NO_INDEX,
                            EMPTY_INT_ARRAY,
                            i == 10 ? new int[] {label1} : new int[] {label1, label2}));
                }
            }
        }

        // when
        check();

        // then
        expect(LabelScanConsistencyReport.class, report -> report.nodeLabelNotInIndex(any(), anyInt()));
    }

    @Test
    void shouldReportIllegalNodeLabel() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            // labels magic field is encoded into inlined NodeLabelField one label with id -1.
            NodeRecord node = new NodeRecord(nodeStore.getIdGenerator().nextId(NULL_CONTEXT))
                    .initialize(true, NULL, false, NULL, 137438953471L);
            try (var storeCursor = storeCursors.writeCursor(NODE_CURSOR)) {
                nodeStore.updateRecord(node, storeCursor, NULL_CONTEXT, storeCursors);
            }
        }

        // when
        check();

        // then
        expect(NodeConsistencyReport.class, NodeConsistencyReport::illegalLabel);
    }

    @Test
    void shouldReportNodeWithIdForReuse() throws Exception {
        // Given
        long nodeId;
        try (AutoCloseable ignored = tx()) {
            nodeId = node(nodeStore.getIdGenerator().nextId(NULL_CONTEXT), NULL, NULL, label1);
        }

        markAsDeletedId(nodeStore, nodeId);

        // when
        check();

        // then
        expect(NodeConsistencyReport.class, NodeConsistencyReport::idIsFreed);
    }

    @Test
    void shouldReportDeletedNodeWithIdNotForReuse() throws Exception {
        // Given
        long nodeId;
        try (AutoCloseable ignored = tx()) {
            nodeId = node(nodeStore.getIdGenerator().nextId(NULL_CONTEXT), NULL, NULL, label1);
        }
        try (AutoCloseable ignored = tx()) {
            try (var storeCursor = storeCursors.writeCursor(NODE_CURSOR)) {
                nodeStore.updateRecord(new NodeRecord(nodeId), storeCursor, CursorContext.NULL_CONTEXT, storeCursors);
            }
        }

        markAsUsedId(nodeStore, nodeId);

        // when
        check();

        // then
        expect(NodeConsistencyReport.class, NodeConsistencyReport::idIsNotFreed);
    }

    // invalidLength of dynamic label record: (impossible, right?)

    private void check() throws Exception {
        NodeChecker checker = new NodeChecker(context(), noMandatoryProperties, noAllowedTypes);
        checker.check(LongRange.range(0, nodeStore.getIdGenerator().getHighId()), true, true);
    }
}
