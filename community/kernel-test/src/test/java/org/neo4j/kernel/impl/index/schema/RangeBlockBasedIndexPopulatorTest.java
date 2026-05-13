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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.values.storable.Values.longValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.schema.SchemaTestUtil;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.test.Race;
import org.neo4j.values.ElementIdMapper;

class RangeBlockBasedIndexPopulatorTest extends GenericBlockBasedIndexPopulatorTest<RangeKey> {
    @Override
    IndexType indexType() {
        return IndexType.RANGE;
    }

    @Override
    BlockBasedIndexPopulator<RangeKey> instantiatePopulator(
            BlockBasedIndexPopulator.Monitor monitor,
            ByteBufferFactory bufferFactory,
            MemoryTracker memoryTracker,
            IndexPopulator.Configuration configuration)
            throws IOException {
        RangeLayout layout = layout();
        Config config = Config.defaults(GraphDatabaseInternalSettings.index_populator_merge_factor, 2);
        RangeBlockBasedIndexPopulator populator = new RangeBlockBasedIndexPopulator(
                databaseIndexContext,
                indexFiles,
                layout,
                INDEX_DESCRIPTOR,
                false,
                bufferFactory,
                config,
                memoryTracker,
                TOKEN_NAME_LOOKUP,
                ElementIdMapper.PLACEHOLDER,
                monitor,
                Sets.immutable.empty(),
                NullLogProvider.getInstance(),
                configuration);
        populator.create();
        return populator;
    }

    @Override
    RangeLayout layout() {
        return new RangeLayout(1);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4})
    void shouldLetMultipleThreadsAddData(int threadSharingFactor) throws IOException, IndexEntryConflictException {
        AtomicLong entriesMerged = new AtomicLong();
        var monitor = new BlockBasedIndexPopulator.Monitor.Adapter() {
            @Override
            public void entriesMerged(int entries) {
                entriesMerged.addAndGet(entries);
            }
        };
        BlockBasedIndexPopulator<RangeKey> populator = instantiatePopulator(
                monitor,
                SchemaTestUtil.defaultHeapBufferFactory(),
                INSTANCE,
                new IndexPopulator.Configuration(threadSharingFactor));
        try {
            // given
            int numThreads = 8;
            int numEntriesPerThread = 1_000;
            Race race = new Race();
            race.addContestants(
                    numThreads,
                    contestantId -> throwing(() -> {
                        List<EagerValueIndexEntryUpdate> batch = new ArrayList<>();
                        long dataId = contestantId;
                        for (int i = 0; i < numEntriesPerThread; i++, dataId += numThreads) {
                            batch.add(EagerValueIndexEntryUpdate.add(dataId, INDEX_DESCRIPTOR, longValue(dataId)));
                            if (ThreadLocalRandom.current().nextInt(20) == 0) {
                                populator.add(batch, NULL_CONTEXT);
                                batch = new ArrayList<>();
                            }
                        }
                        if (!batch.isEmpty()) {
                            populator.add(batch, NULL_CONTEXT);
                        }
                    }));
            race.goUnchecked();

            // when
            populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);

            // then
            RangeKey from = layout().newKey();
            RangeKey to = layout().newKey();
            layout().initializeAsLowest(from);
            layout().initializeAsHighest(to);
            try (Seeker<RangeKey, NullValue> seek = populator.tree.seek(from, to, NULL_CONTEXT)) {
                long max = numThreads * numEntriesPerThread;
                //                assertThat(entriesMerged.longValue()).isEqualTo(max);
                for (long nextExpected = 0; nextExpected < max; nextExpected++) {
                    assertThat(seek.next()).isEqualTo(true);
                    RangeKey key = seek.key();
                    assertThat(key.getEntityId()).isEqualTo(nextExpected);
                    assertThat(key.asValues()[0]).isEqualTo(longValue(nextExpected));
                }
                assertThat(seek.next()).isEqualTo(false);
            }
        } finally {
            populator.close(true, NULL_CONTEXT);
        }
    }
}
