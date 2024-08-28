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
package org.neo4j.internal.batchimport.input;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.neo4j.batchimport.api.ReadBehaviour;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.common.EntityType;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.Values;

class LenientStoreInputChunkTest {
    private final PropertyStore propertyStore = mock(PropertyStore.class);
    private final ReadBehaviour readBehaviour = mock(ReadBehaviour.class);
    private final TokenHolders tokenHolders = mock(TokenHolders.class);
    private final Group group = new Group(0, "group", null);

    @BeforeEach
    void setUp() {
        when(propertyStore.newRecord()).thenAnswer(invocationOnMock -> new PropertyRecord(-1));
    }

    @Test
    void shouldHandleCircularPropertyChain() {
        // given
        MutableLongObjectMap<PropertyRecord> propertyRecords = LongObjectMaps.mutable.empty();
        long[] propertyRecordIds = new long[] {12, 13, 14, 15};
        for (int i = 0; i < propertyRecordIds.length; i++) {
            long prev = i == 0 ? NULL_REFERENCE.longValue() : propertyRecordIds[i - 1];
            long id = propertyRecordIds[i];
            long next = i == propertyRecordIds.length - 1 ? propertyRecordIds[1] : propertyRecordIds[i + 1];
            propertyRecords.put(id, new PropertyRecord(id).initialize(true, prev, next));
        }
        when(propertyStore.getRecordByCursor(anyLong(), any(), any(), any(), any()))
                .thenAnswer(mapBackedPropertyStore(propertyRecords));

        try (LenientStoreInputChunk chunk = inputChunkReader()) {
            // when
            NodeRecord primitiveRecord = new NodeRecord(9);
            primitiveRecord.initialize(
                    true, propertyRecordIds[0], false, NULL_REFERENCE.longValue(), NULL_REFERENCE.longValue());
            InputEntityVisitor visitor = mock(InputEntityVisitor.class);
            chunk.visitPropertyChainNoThrow(
                    visitor, primitiveRecord, EntityType.NODE, EMPTY_STRING_ARRAY, EmptyMemoryTracker.INSTANCE);

            // then
            verify(readBehaviour)
                    .error(argThat(format -> format.contains("circular property chain")), any(Object[].class));
        }
    }

    @Test
    void ignoreDuplicatePropertyKeys() throws TokenNotFoundException {
        // given
        MutableLongObjectMap<PropertyRecord> propertyRecords = LongObjectMaps.mutable.empty();
        propertyRecords.put(10, new PropertyRecord(10).initialize(true, NULL_REFERENCE.longValue(), 11));
        propertyRecords.put(11, new PropertyRecord(11).initialize(true, 10, NULL_REFERENCE.longValue()));

        when(propertyStore.getRecordByCursor(anyLong(), any(), any(), any(), any()))
                .thenAnswer(mapBackedPropertyStore(propertyRecords));

        // Property key: id=3 name=age
        NamedToken token = new NamedToken("age", 3);
        TokenHolder tokenHolder = mock(TokenHolder.class);
        when(tokenHolders.propertyKeyTokens()).thenReturn(tokenHolder);
        when(tokenHolder.getTokenById(token.id())).thenReturn(token);

        addBlockToProperty(propertyRecords.get(10), token, 4);
        IntValue intValue11 = addBlockToProperty(propertyRecords.get(11), token, 5);

        try (LenientStoreInputChunk chunk = inputChunkReader()) {
            // when
            NodeRecord nodeRecord = new NodeRecord(9);
            nodeRecord.initialize(true, 10, false, NULL_REFERENCE.longValue(), NULL_REFERENCE.longValue());
            InputEntityVisitor visitor = mock(InputEntityVisitor.class);
            chunk.visitPropertyChainNoThrow(
                    visitor, nodeRecord, EntityType.NODE, EMPTY_STRING_ARRAY, EmptyMemoryTracker.INSTANCE);

            // then
            verify(readBehaviour)
                    .error(
                            argThat(format -> format.contains("Discarding duplicate property key")),
                            eq(EntityType.NODE),
                            eq(9L),
                            eq(token.name()),
                            eq(token.id()),
                            eq(intValue11));
        }
    }

    @Test
    void breakOnBrokenPropertyChain() {
        // given
        MutableLongObjectMap<PropertyRecord> propertyRecords = LongObjectMaps.mutable.empty();
        propertyRecords.put(10, new PropertyRecord(10).initialize(true, NULL_REFERENCE.longValue(), 11));
        propertyRecords.put(11, new PropertyRecord(11).initialize(true, 5, NULL_REFERENCE.longValue()));

        when(propertyStore.getRecordByCursor(anyLong(), any(), any(), any(), any()))
                .thenAnswer(mapBackedPropertyStore(propertyRecords));

        try (LenientStoreInputChunk chunk = inputChunkReader()) {
            // when
            NodeRecord nodeRecord = new NodeRecord(9);
            nodeRecord.initialize(true, 10, false, NULL_REFERENCE.longValue(), NULL_REFERENCE.longValue());
            InputEntityVisitor visitor = mock(InputEntityVisitor.class);
            chunk.visitPropertyChainNoThrow(
                    visitor, nodeRecord, EntityType.NODE, EMPTY_STRING_ARRAY, EmptyMemoryTracker.INSTANCE);

            // then
            verify(readBehaviour)
                    .error(argThat(format -> format.contains("Ignoring broken property chain.")), any(Object[].class));
        }
    }

    private Answer<Object> mapBackedPropertyStore(MutableLongObjectMap<PropertyRecord> propertyRecords) {
        return invocationOnMock -> {
            PropertyRecord sourceRecord = propertyRecords.get(invocationOnMock.getArgument(0));
            PropertyRecord targetRecord = invocationOnMock.getArgument(1);
            targetRecord.setId(sourceRecord.getId());
            targetRecord.initialize(true, sourceRecord.getPrevProp(), sourceRecord.getNextProp());
            for (PropertyBlock propertyBlock : sourceRecord.propertyBlocksArray()) {
                if (propertyBlock != null) {
                    targetRecord.addPropertyBlock(propertyBlock);
                }
            }
            return targetRecord;
        };
    }

    private LenientStoreInputChunk inputChunkReader() {
        return new LenientStoreInputChunk(
                readBehaviour,
                propertyStore,
                tokenHolders,
                NULL_CONTEXT_FACTORY,
                StoreCursors.NULL,
                mock(PageCursor.class),
                group,
                INSTANCE) {
            @Override
            void readAndVisit(
                    long id, InputEntityVisitor visitor, StoreCursors storeCursors, MemoryTracker memoryTracker) {}

            @Override
            String recordType() {
                return null;
            }

            @Override
            boolean shouldIncludeProperty(ReadBehaviour readBehaviour, String key, String[] owningEntityTokens) {
                return true;
            }
        };
    }

    private IntValue addBlockToProperty(PropertyRecord propertyRecord, NamedToken token, int value) {
        PropertyBlock block = new PropertyBlock();
        IntValue intValue = Values.intValue(value);
        PropertyStore.encodeValue(block, token.id(), intValue, null, null, NULL_CONTEXT, INSTANCE);
        propertyRecord.addPropertyBlock(block);
        return intValue;
    }
}
