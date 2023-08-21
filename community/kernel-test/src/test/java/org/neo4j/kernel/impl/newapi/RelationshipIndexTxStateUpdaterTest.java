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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.helpers.StubPropertyCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class RelationshipIndexTxStateUpdaterTest extends IndexTxStateUpdaterTestBase {
    private static final int REL_ID = 1234;
    private static final int TYPE_ID = 10;
    private static final int UN_INDEXED_TYPE_ID = 11;

    private final IndexDescriptor indexOn_1 = TestIndexDescriptorFactory.forRelType(TYPE_ID, PROP_ID_1);
    private final IndexDescriptor indexOn_new = TestIndexDescriptorFactory.forRelType(TYPE_ID, NEW_PROP_ID);
    private final IndexDescriptor indexOn_2 = TestIndexDescriptorFactory.forRelType(TYPE_ID, PROP_ID_2);
    private final IndexDescriptor indexOn_1_new =
            TestIndexDescriptorFactory.forRelType(TYPE_ID, PROP_ID_1, NEW_PROP_ID);
    private final IndexDescriptor indexOn_2_3 = TestIndexDescriptorFactory.forRelType(TYPE_ID, PROP_ID_2, PROP_ID_3);
    private final List<IndexDescriptor> indexes =
            Arrays.asList(indexOn_1, indexOn_new, indexOn_2, indexOn_1_new, indexOn_2_3);
    private final RelationshipScanCursor relationship = mock(RelationshipScanCursor.class);

    @BeforeEach
    void setup() throws IndexNotFoundKernelException {
        setUp(indexes);

        Map<Integer, Value> map = new HashMap<>();
        map.put(PROP_ID_1, Values.of("hi1"));
        map.put(PROP_ID_2, Values.of("hi2"));
        map.put(PROP_ID_3, Values.of("hi3"));

        when(relationship.reference()).thenReturn((long) REL_ID);
        doAnswer(invocationOnMock -> {
                    invocationOnMock
                            .getArgument(0, StubPropertyCursor.class)
                            .init(map, invocationOnMock.getArgument(1, PropertySelection.class));
                    return null;
                })
                .when(relationship)
                .properties(any(), any());

        relationship.next();
    }

    @Test
    void shouldNotUpdateIndexesOnChangedIrrelevantProperty() {
        // WHEN
        indexTxUpdater.onPropertyAdd(
                relationship, propertyCursor, TYPE_ID, UN_INDEXED_PROP_ID, PROPS, Values.of("whAt"));
        indexTxUpdater.onPropertyRemove(
                relationship, propertyCursor, TYPE_ID, UN_INDEXED_PROP_ID, PROPS, Values.of("whAt"));
        indexTxUpdater.onPropertyChange(
                relationship,
                propertyCursor,
                TYPE_ID,
                UN_INDEXED_PROP_ID,
                PROPS,
                Values.of("whAt"),
                Values.of("whAt2"));

        // THEN
        verify(txState, never()).indexDoUpdateEntry(any(), anyInt(), any(), any());
    }

    @Test
    void shouldNotUpdateIndexesOnChangedChangedPropertyOnIrrelevantType() {
        // WHEN
        indexTxUpdater.onPropertyAdd(
                relationship, propertyCursor, UN_INDEXED_TYPE_ID, PROP_ID_1, PROPS, Values.of("whAt"));
        indexTxUpdater.onPropertyRemove(
                relationship, propertyCursor, UN_INDEXED_TYPE_ID, PROP_ID_1, PROPS, Values.of("whAt"));
        indexTxUpdater.onPropertyChange(
                relationship,
                propertyCursor,
                UN_INDEXED_TYPE_ID,
                PROP_ID_1,
                PROPS,
                Values.of("whAt"),
                Values.of("whAt2"));

        // THEN
        verify(txState, never()).indexDoUpdateEntry(any(), anyInt(), any(), any());
    }

    @Test
    void shouldUpdateIndexesOnAddedProperty() {
        // WHEN
        indexTxUpdater.onPropertyAdd(relationship, propertyCursor, TYPE_ID, NEW_PROP_ID, PROPS, Values.of("newHi"));

        // THEN
        verifyIndexUpdate(indexOn_new.schema(), REL_ID, null, values("newHi"));
        verifyIndexUpdate(indexOn_1_new.schema(), REL_ID, null, values("hi1", "newHi"));
        verify(txState, times(2)).indexDoUpdateEntry(any(), anyLong(), isNull(), any());
    }

    @Test
    void shouldUpdateIndexesOnRemovedProperty() {
        // WHEN
        indexTxUpdater.onPropertyRemove(relationship, propertyCursor, TYPE_ID, PROP_ID_2, PROPS, Values.of("hi2"));

        // THEN
        verifyIndexUpdate(indexOn_2.schema(), REL_ID, values("hi2"), null);
        verifyIndexUpdate(indexOn_2_3.schema(), REL_ID, values("hi2", "hi3"), null);
        verify(txState, times(2)).indexDoUpdateEntry(any(), anyLong(), any(), isNull());
    }

    @Test
    void shouldUpdateIndexesOnChangedProperty() {
        // WHEN
        indexTxUpdater.onPropertyChange(
                relationship, propertyCursor, TYPE_ID, PROP_ID_2, PROPS, Values.of("hi2"), Values.of("new2"));

        // THEN
        verifyIndexUpdate(indexOn_2.schema(), REL_ID, values("hi2"), values("new2"));
        verifyIndexUpdate(indexOn_2_3.schema(), REL_ID, values("hi2", "hi3"), values("new2", "hi3"));
        verify(txState, times(2)).indexDoUpdateEntry(any(), anyLong(), any(), any());
    }
}
