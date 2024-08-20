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
package org.neo4j.storageengine.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.values.storable.Values.intValue;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.test.Race;

class IndexUpdatesWorkSyncTest {

    private final CursorContextFactory contextFactory = NULL_CONTEXT_FACTORY;

    @RepeatedTest(10)
    void shouldApplyIndexUpdatesSingleThreadedIfToldTo() {
        // ensure context factory creates non-equal contexts
        assertThat(NULL_CONTEXT_FACTORY.create("0")).isNotEqualTo(contextFactory.create("1"));
        // given
        IndexDescriptor index = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2))
                .withName("index")
                .materialise(1L);
        int threads = 10;
        Set<UpdateAndContext> appliedUpdates = Collections.newSetFromMap(new ConcurrentHashMap<>());
        AtomicInteger concurrentlyApplyingThreads = new AtomicInteger();
        IndexUpdateListener updateListener = new IndexUpdateListener.Adapter() {
            @Override
            public void applyUpdates(
                    Iterable<IndexEntryUpdate<IndexDescriptor>> updates,
                    CursorContext cursorContext,
                    boolean parallel) {
                assertThat(concurrentlyApplyingThreads.incrementAndGet()).isOne();
                assertThat(parallel).isFalse();
                updates.forEach(u -> appliedUpdates.add(new UpdateAndContext(u, cursorContext)));
                concurrentlyApplyingThreads.decrementAndGet();
            }
        };

        // when
        IndexUpdatesWorkSync workSync = new IndexUpdatesWorkSync(updateListener, false);
        var sentUpdates = queueUpdatesInParallel(index, threads, workSync, contextFactory);

        // then
        assertThat(appliedUpdates).isEqualTo(sentUpdates);
        assertThat(concurrentlyApplyingThreads.get()).isZero();
    }

    @Test
    void shouldApplyIndexUpdatesInParallelIfToldTo() {
        // given
        IndexDescriptor index = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2))
                .withName("index")
                .materialise(1L);
        int threads = 4;
        CountDownLatch latch = new CountDownLatch(threads);
        Set<UpdateAndContext> appliedUpdates = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Set<Thread> applyingThreads = Collections.newSetFromMap(new ConcurrentHashMap<>());
        IndexUpdateListener updateListener = new IndexUpdateListener.Adapter() {
            @Override
            public void applyUpdates(
                    Iterable<IndexEntryUpdate<IndexDescriptor>> updates,
                    CursorContext cursorContext,
                    boolean parallel) {
                assertThat(parallel).isTrue();
                applyingThreads.add(Thread.currentThread());
                updates.forEach(u -> appliedUpdates.add(new UpdateAndContext(u, cursorContext)));
                latch.countDown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        // when
        IndexUpdatesWorkSync workSync = new IndexUpdatesWorkSync(updateListener, true);
        var sentUpdates = queueUpdatesInParallel(index, threads, workSync, contextFactory);

        // then
        assertThat(appliedUpdates).isEqualTo(sentUpdates);
        assertThat(applyingThreads.size()).isEqualTo(threads);
    }

    private Set<UpdateAndContext> queueUpdatesInParallel(
            IndexDescriptor index, int threads, IndexUpdatesWorkSync workSync, CursorContextFactory contextFactory) {
        Set<UpdateAndContext> sentUpdates = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Race race = new Race();
        race.addContestants(
                threads,
                i -> throwing(() -> {
                    var cursorContext = contextFactory.create(Integer.toString(i));
                    try (IndexUpdatesWorkSync.Batch batch = workSync.newBatch(cursorContext)) {
                        ValueIndexEntryUpdate<IndexDescriptor> update =
                                IndexEntryUpdate.add(i, index, intValue(10 + i));
                        sentUpdates.add(new UpdateAndContext(update, cursorContext));
                        batch.indexUpdate(update);
                    }
                }),
                1);
        race.goUnchecked();
        return sentUpdates;
    }

    record UpdateAndContext(IndexEntryUpdate<IndexDescriptor> update, CursorContext context) {}
}
