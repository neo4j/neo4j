/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.storageengine.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.values.storable.Values.intValue;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.test.Race;

class IndexUpdatesWorkSyncTest {
    @Test
    void shouldApplyIndexUpdatesSingleThreadedIfToldTo() {
        // given
        IndexDescriptor index = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2))
                .withName("index")
                .materialise(1L);
        int threads = 4;
        Set<IndexEntryUpdate<IndexDescriptor>> appliedUpdates = Collections.newSetFromMap(new ConcurrentHashMap<>());
        AtomicInteger concurrentlyApplyingThreads = new AtomicInteger();
        IndexUpdateListener updateListener = new IndexUpdateListener.Adapter() {
            @Override
            public void applyUpdates(
                    Iterable<IndexEntryUpdate<IndexDescriptor>> updates,
                    CursorContext cursorContext,
                    boolean parallel) {
                assertThat(concurrentlyApplyingThreads.incrementAndGet()).isOne();
                assertThat(parallel).isFalse();
                updates.forEach(appliedUpdates::add);
                concurrentlyApplyingThreads.decrementAndGet();
            }
        };

        // when
        IndexUpdatesWorkSync workSync = new IndexUpdatesWorkSync(updateListener, false);
        Set<IndexEntryUpdate<IndexDescriptor>> sentUpdates = queueUpdatesInParallel(index, threads, workSync);

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
        Set<IndexEntryUpdate<IndexDescriptor>> appliedUpdates = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Set<Thread> applyingThreads = Collections.newSetFromMap(new ConcurrentHashMap<>());
        IndexUpdateListener updateListener = new IndexUpdateListener.Adapter() {
            @Override
            public void applyUpdates(
                    Iterable<IndexEntryUpdate<IndexDescriptor>> updates,
                    CursorContext cursorContext,
                    boolean parallel) {
                assertThat(parallel).isTrue();
                applyingThreads.add(Thread.currentThread());
                updates.forEach(appliedUpdates::add);
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
        Set<IndexEntryUpdate<IndexDescriptor>> sentUpdates = queueUpdatesInParallel(index, threads, workSync);

        // then
        assertThat(appliedUpdates).isEqualTo(sentUpdates);
        assertThat(applyingThreads.size()).isEqualTo(threads);
    }

    private Set<IndexEntryUpdate<IndexDescriptor>> queueUpdatesInParallel(
            IndexDescriptor index, int threads, IndexUpdatesWorkSync workSync) {
        Set<IndexEntryUpdate<IndexDescriptor>> sentUpdates = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Race race = new Race();
        race.addContestants(
                threads,
                i -> throwing(() -> {
                    IndexUpdatesWorkSync.Batch batch = workSync.newBatch();
                    ValueIndexEntryUpdate<IndexDescriptor> update = IndexEntryUpdate.add(i, index, intValue(10 + i));
                    sentUpdates.add(update);
                    batch.add(update);
                    batch.apply(CursorContext.NULL_CONTEXT);
                }),
                1);
        race.goUnchecked();
        return sentUpdates;
    }
}
