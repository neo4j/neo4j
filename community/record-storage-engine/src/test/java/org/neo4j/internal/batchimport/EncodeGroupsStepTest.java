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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.ReadGroupsFromCacheStepTest.Group;
import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.SimpleStageControl;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.batchimport.staging.Step;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.memory.EmptyMemoryTracker;

class EncodeGroupsStepTest {
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);
    private final StageControl control = new SimpleStageControl();
    private final RecordStore<RelationshipGroupRecord> store = mock(RecordStore.class);
    private IdGenerator idGenerator;

    @BeforeEach
    void setUp() {
        idGenerator = mock(IdGenerator.class);
        when(store.getIdGenerator()).thenReturn(idGenerator);
    }

    @Test
    void shouldEncodeGroupChains() throws Throwable {
        final AtomicLong nextId = new AtomicLong();
        when(idGenerator.nextId(NULL_CONTEXT)).thenAnswer(invocation -> nextId.incrementAndGet());
        doAnswer(invocation -> {
                    // our own way of marking that this has record been prepared (firstOut=1)
                    invocation.<RelationshipGroupRecord>getArgument(0).setFirstOut(1);
                    return null;
                })
                .when(store)
                .prepareForCommit(any(RelationshipGroupRecord.class), any(IdSequence.class), any(CursorContext.class));
        Configuration config = Configuration.withBatchSize(Configuration.DEFAULT, 10);
        EncodeGroupsStep encoder = new EncodeGroupsStep(control, config, store, CONTEXT_FACTORY);

        // WHEN
        encoder.start(Step.ORDER_SEND_DOWNSTREAM);
        Catcher catcher = new Catcher();
        encoder.process(
                batch(new Group(1, 3), new Group(2, 3), new Group(3, 4)),
                catcher,
                NULL_CONTEXT,
                EmptyMemoryTracker.INSTANCE);
        encoder.process(batch(new Group(4, 2), new Group(5, 10)), catcher, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        encoder.process(batch(new Group(6, 35)), catcher, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        encoder.process(batch(new Group(7, 2)), catcher, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        encoder.endOfUpstream();
        encoder.awaitCompleted();
        encoder.close();

        // THEN
        assertEquals(4, catcher.batches.size());
        long lastOwningNodeLastBatch = -1;
        for (RelationshipGroupRecord[] batch : catcher.batches) {
            assertBatch(batch, lastOwningNodeLastBatch);
            lastOwningNodeLastBatch = batch[batch.length - 1].getOwningNode();
        }
    }

    @Test
    void tracePageCacheAccessOnEncode() throws Exception {
        when(idGenerator.nextId(any(CursorContext.class))).thenAnswer(invocation -> {
            CursorContext cursorContext = invocation.getArgument(0);
            var swapper = mock(PageSwapper.class, RETURNS_MOCKS);
            try (var event = cursorContext.getCursorTracer().beginPin(false, 1, swapper)) {
                event.hit();
            }
            cursorContext.getCursorTracer().unpin(1, swapper);
            return 1L;
        });
        var cacheTracer = new DefaultPageCacheTracer();
        var cursorContext = CONTEXT_FACTORY.create(cacheTracer.createPageCursorTracer("tracePageCacheAccessOnEncode"));
        Configuration config = Configuration.withBatchSize(Configuration.DEFAULT, 10);
        try (EncodeGroupsStep encoder = new EncodeGroupsStep(control, config, store, CONTEXT_FACTORY)) {
            encoder.start(Step.ORDER_SEND_DOWNSTREAM);
            Catcher catcher = new Catcher();
            encoder.process(
                    batch(new Group(1, 3), new Group(2, 3), new Group(3, 4)),
                    catcher,
                    cursorContext,
                    EmptyMemoryTracker.INSTANCE);
            encoder.endOfUpstream();
            encoder.awaitCompleted();
        }

        assertThat(cursorContext.getCursorTracer().pins()).isEqualTo(10);
        assertThat(cursorContext.getCursorTracer().hits()).isEqualTo(10);
        assertThat(cursorContext.getCursorTracer().unpins()).isEqualTo(10);
    }

    private static void assertBatch(RelationshipGroupRecord[] batch, long lastOwningNodeLastBatch) {
        for (int i = 0; i < batch.length; i++) {
            RelationshipGroupRecord record = batch[i];
            assertTrue(record.getId() > Record.NULL_REFERENCE.longValue());
            assertTrue(record.getOwningNode() > lastOwningNodeLastBatch);
            assertEquals(1, record.getFirstOut()); // the mark our store mock sets when preparing
            if (record.getNext() == Record.NULL_REFERENCE.longValue()) {
                // This is the last in the chain, verify that this is either:
                assertTrue(
                        // - the last one in the batch, or
                        i == batch.length - 1
                                ||
                                // - the last one for this node
                                batch[i + 1].getOwningNode() > record.getOwningNode());
            }
        }
    }

    private static RelationshipGroupRecord[] batch(Group... groups) {
        return ReadGroupsFromCacheStepTest.groups(groups).toArray(new RelationshipGroupRecord[0]);
    }

    private static class Catcher implements BatchSender {
        private final List<RelationshipGroupRecord[]> batches = new ArrayList<>();

        @Override
        public void send(Object batch) {
            batches.add((RelationshipGroupRecord[]) batch);
        }
    }
}
