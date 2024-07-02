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
package org.neo4j.kernel.impl.newapi;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.kernel.impl.newapi.IndexTxStateUpdater.LabelChangeType.ADDED_LABEL;
import static org.neo4j.kernel.impl.newapi.IndexTxStateUpdater.LabelChangeType.REMOVED_LABEL;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class NodeIndexTxStateUpdaterTest extends IndexTxStateUpdaterTestBase {
    private static final int LABEL_ID_1 = 10;
    private static final int LABEL_ID_2 = 11;

    private final IndexDescriptor indexOn1_1 = TestIndexDescriptorFactory.forLabel(LABEL_ID_1, PROP_ID_1);
    private final IndexDescriptor indexOn2_new = TestIndexDescriptorFactory.forLabel(LABEL_ID_2, NEW_PROP_ID);
    private final IndexDescriptor uniqueOn1_2 = TestIndexDescriptorFactory.uniqueForLabel(LABEL_ID_1, PROP_ID_2);
    private final IndexDescriptor indexOn1_1_new =
            TestIndexDescriptorFactory.forLabel(LABEL_ID_1, PROP_ID_1, NEW_PROP_ID);
    private final IndexDescriptor uniqueOn2_2_3 =
            TestIndexDescriptorFactory.uniqueForLabel(LABEL_ID_2, PROP_ID_2, PROP_ID_3);
    private final List<IndexDescriptor> indexes =
            Arrays.asList(indexOn1_1, indexOn2_new, uniqueOn1_2, indexOn1_1_new, uniqueOn2_2_3);
    private StubNodeCursor node;

    @BeforeEach
    void setup() throws IndexNotFoundKernelException {
        setUp(indexes);

        Map<Integer, Value> map = new HashMap<>();
        map.put(PROP_ID_1, Values.of("hi1"));
        map.put(PROP_ID_2, Values.of("hi2"));
        map.put(PROP_ID_3, Values.of("hi3"));
        node = new StubNodeCursor().withNode(0, new int[] {LABEL_ID_1, LABEL_ID_2}, map);
        node.next();
    }

    // LABELS

    @Test
    void shouldNotUpdateIndexesOnChangedIrrelevantLabel() {
        // WHEN
        indexTxUpdater.onLabelChange(node, propertyCursor, ADDED_LABEL, Collections.emptyList());
        indexTxUpdater.onLabelChange(node, propertyCursor, REMOVED_LABEL, Collections.emptyList());

        // THEN
        verify(txState, never()).indexDoUpdateEntry(any(), anyInt(), any(), any());
    }

    @Test
    void shouldUpdateIndexesOnAddedLabel() {
        // WHEN
        indexTxUpdater.onLabelChange(
                node,
                propertyCursor,
                ADDED_LABEL,
                storageReader.valueIndexesGetRelated(new int[] {LABEL_ID_1}, PROPS, NODE));

        // THEN
        verifyIndexUpdate(indexOn1_1.schema(), node.nodeReference(), null, values("hi1"));
        verifyIndexUpdate(uniqueOn1_2.schema(), node.nodeReference(), null, values("hi2"));
        verify(txState, times(2)).indexDoUpdateEntry(any(), anyLong(), isNull(), any());
    }

    @Test
    void shouldUpdateIndexesOnRemovedLabel() {
        // WHEN
        indexTxUpdater.onLabelChange(
                node,
                propertyCursor,
                REMOVED_LABEL,
                storageReader.valueIndexesGetRelated(new int[] {LABEL_ID_2}, PROPS, NODE));

        // THEN
        verifyIndexUpdate(uniqueOn2_2_3.schema(), node.nodeReference(), values("hi2", "hi3"), null);
        verify(txState).indexDoUpdateEntry(any(), anyLong(), any(), isNull());
    }

    @Test
    void shouldNotUpdateIndexesOnChangedIrrelevantProperty() {
        // WHEN
        indexTxUpdater.onPropertyAdd(
                node, propertyCursor, node.labels().all(), UN_INDEXED_PROP_ID, PROPS, Values.of("whAt"));
        indexTxUpdater.onPropertyRemove(
                node, propertyCursor, node.labels().all(), UN_INDEXED_PROP_ID, PROPS, Values.of("whAt"));
        indexTxUpdater.onPropertyChange(
                node,
                propertyCursor,
                node.labels().all(),
                UN_INDEXED_PROP_ID,
                PROPS,
                Values.of("whAt"),
                Values.of("whAt2"));

        // THEN
        verify(txState, never()).indexDoUpdateEntry(any(), anyInt(), any(), any());
    }

    @Test
    void shouldUpdateIndexesOnAddedProperty() {
        // WHEN
        indexTxUpdater.onPropertyAdd(node, propertyCursor, node.labels().all(), NEW_PROP_ID, PROPS, Values.of("newHi"));

        // THEN
        verifyIndexUpdate(indexOn2_new.schema(), node.nodeReference(), null, values("newHi"));
        verifyIndexUpdate(indexOn1_1_new.schema(), node.nodeReference(), null, values("hi1", "newHi"));
        verify(txState, times(2)).indexDoUpdateEntry(any(), anyLong(), isNull(), any());
    }

    @Test
    void shouldUpdateIndexesOnRemovedProperty() {
        // WHEN
        indexTxUpdater.onPropertyRemove(node, propertyCursor, node.labels().all(), PROP_ID_2, PROPS, Values.of("hi2"));

        // THEN
        verifyIndexUpdate(uniqueOn1_2.schema(), node.nodeReference(), values("hi2"), null);
        verifyIndexUpdate(uniqueOn2_2_3.schema(), node.nodeReference(), values("hi2", "hi3"), null);
        verify(txState, times(2)).indexDoUpdateEntry(any(), anyLong(), any(), isNull());
    }

    @Test
    void shouldUpdateIndexesOnChangedProperty() {
        // WHEN
        indexTxUpdater.onPropertyChange(
                node, propertyCursor, node.labels().all(), PROP_ID_2, PROPS, Values.of("hi2"), Values.of("new2"));

        // THEN
        verifyIndexUpdate(uniqueOn1_2.schema(), node.nodeReference(), values("hi2"), values("new2"));
        verifyIndexUpdate(uniqueOn2_2_3.schema(), node.nodeReference(), values("hi2", "hi3"), values("new2", "hi3"));
        verify(txState, times(2)).indexDoUpdateEntry(any(), anyLong(), any(), any());
    }
}
