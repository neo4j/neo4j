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
package org.neo4j.internal.batchimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.id.IdValidator.INTEGER_MINUS_ONE;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;

import java.util.Arrays;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.batchimport.stats.Keys;
import org.neo4j.internal.batchimport.stats.Stat;
import org.neo4j.internal.batchimport.store.StorePrepareIdSequence;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

class UpdateRecordsStepTest {
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

    @Test
    void ioThroughputStatDoesNotOverflow() {
        // store with huge record size to force overflow and not create huge batch of records
        RecordStore<NodeRecord> store = mock(RecordStore.class);
        when(store.getRecordSize()).thenReturn(Integer.MAX_VALUE / 2);

        Configuration configuration = mock(Configuration.class);
        StageControl stageControl = mock(StageControl.class);
        UpdateRecordsStep<NodeRecord> step = new UpdateRecordsStep<>(
                stageControl,
                configuration,
                store,
                new StorePrepareIdSequence(),
                CONTEXT_FACTORY,
                getCursorsCreator(),
                NODE_CURSOR);

        NodeRecord record = new NodeRecord(1);
        record.setInUse(true);
        NodeRecord[] batch = new NodeRecord[11];
        Arrays.fill(batch, record);

        step.process(batch, mock(BatchSender.class), NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);

        Stat stat = step.stat(Keys.io_throughput);

        assertThat(stat.asLong()).isGreaterThan(0L);
    }

    @Test
    void recordWithReservedIdIsSkipped() {
        RecordStore<NodeRecord> store = mock(NodeStore.class);
        when(store.getIdGenerator()).thenReturn(mock(IdGenerator.class));
        StageControl stageControl = mock(StageControl.class);
        UpdateRecordsStep<NodeRecord> step = new UpdateRecordsStep<>(
                stageControl,
                Configuration.DEFAULT,
                store,
                new StorePrepareIdSequence(),
                CONTEXT_FACTORY,
                getCursorsCreator(),
                NODE_CURSOR);

        NodeRecord node1 = new NodeRecord(1);
        node1.setInUse(true);
        NodeRecord node2 = new NodeRecord(2);
        node2.setInUse(true);
        NodeRecord nodeWithReservedId = new NodeRecord(INTEGER_MINUS_ONE);
        NodeRecord[] batch = {node1, node2, nodeWithReservedId};

        step.process(batch, mock(BatchSender.class), NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);

        verify(store).prepareForCommit(eq(node1), any(IdSequence.class), any(CursorContext.class));
        verify(store).updateRecord(eq(node1), any(), any(), any(), any());
        verify(store).prepareForCommit(eq(node2), any(IdSequence.class), any(CursorContext.class));
        verify(store).updateRecord(eq(node2), any(), any(), any(), any());
        verify(store, never())
                .prepareForCommit(eq(nodeWithReservedId), any(IdSequence.class), any(CursorContext.class));
        verify(store, never()).updateRecord(eq(nodeWithReservedId), any(), any(), any());
    }

    private static Function<CursorContext, StoreCursors> getCursorsCreator() {
        return any -> new CachedStoreCursors(mock(NeoStores.class, RETURNS_MOCKS), any);
    }
}
