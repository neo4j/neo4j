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
package org.neo4j.kernel.impl.index.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;
import static org.neo4j.storageengine.api.IndexEntryUpdate.change;
import static org.neo4j.storageengine.api.IndexEntryUpdate.remove;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.storageengine.api.UpdateMode;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class DeferredConflictCheckingIndexUpdaterTest {
    private static final int labelId = 1;
    private final int[] propertyKeyIds = {2, 3};
    private final IndexDescriptor descriptor = TestIndexDescriptorFactory.forLabel(labelId, propertyKeyIds);

    @Test
    void shouldQueryAboutAddedAndChangedValueTuples() throws Exception {
        // given
        IndexUpdater actual = mock(IndexUpdater.class);
        ValueIndexReader reader = mock(ValueIndexReader.class);
        doAnswer(new NodeIdsIndexReaderQueryAnswer(descriptor, 0))
                .when(reader)
                .query(any(), any(), any(), any(), any(), any());
        long nodeId = 0;
        List<ValueIndexEntryUpdate<IndexDescriptor>> updates = new ArrayList<>();
        updates.add(add(nodeId++, descriptor, tuple(10, 11)));
        updates.add(change(nodeId++, descriptor, tuple("abc", "def"), tuple("ghi", "klm")));
        updates.add(remove(nodeId++, descriptor, tuple(1001L, 1002L)));
        updates.add(change(nodeId++, descriptor, tuple((byte) 2, (byte) 3), tuple((byte) 4, (byte) 5)));
        updates.add(add(nodeId, descriptor, tuple(5, "5")));
        try (DeferredConflictCheckingIndexUpdater updater =
                new DeferredConflictCheckingIndexUpdater(actual, () -> reader, descriptor, NULL_CONTEXT)) {
            // when
            for (ValueIndexEntryUpdate<IndexDescriptor> update : updates) {
                updater.process(update);
                verify(actual).process(update);
            }
        }

        // then
        for (ValueIndexEntryUpdate<IndexDescriptor> update : updates) {
            if (update.updateMode() == UpdateMode.ADDED || update.updateMode() == UpdateMode.CHANGED) {
                Value[] tuple = update.values();
                PropertyIndexQuery[] query = new PropertyIndexQuery[tuple.length];
                for (int i = 0; i < tuple.length; i++) {
                    query[i] = PropertyIndexQuery.exact(propertyKeyIds[i], tuple[i]);
                }
                verify(reader).query(any(), any(), any(), any(), eq(query[0]), eq(query[1]));
            }
        }
        verify(reader).close();
        verifyNoMoreInteractions(reader);
    }

    @Test
    void shouldThrowOnIndexEntryConflict() throws Exception {
        // given
        IndexUpdater actual = mock(IndexUpdater.class);
        ValueIndexReader reader = mock(ValueIndexReader.class);
        doAnswer(new NodeIdsIndexReaderQueryAnswer(descriptor, 101, 202))
                .when(reader)
                .query(any(), any(), any(), any(), any(PropertyIndexQuery[].class));
        DeferredConflictCheckingIndexUpdater updater =
                new DeferredConflictCheckingIndexUpdater(actual, () -> reader, descriptor, NULL_CONTEXT);

        // when
        updater.process(add(0, descriptor, tuple(10, 11)));
        var e = assertThrows(IndexEntryConflictException.class, updater::close);
        assertThat(e.getMessage()).contains("101");
        assertThat(e.getMessage()).contains("202");
    }

    private static Value[] tuple(Object... values) {
        Value[] result = new Value[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = Values.of(values[i]);
        }
        return result;
    }
}
