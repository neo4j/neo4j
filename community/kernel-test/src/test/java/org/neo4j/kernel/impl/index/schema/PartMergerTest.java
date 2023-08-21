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

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.index.schema.BlockEntryMergerTestUtils.assertMergedPartStream;
import static org.neo4j.kernel.impl.index.schema.BlockEntryMergerTestUtils.buildParts;
import static org.neo4j.kernel.impl.index.schema.BlockStorage.Cancellation.NOT_CANCELLABLE;
import static org.neo4j.scheduler.JobMonitoringParams.NOT_MONITORED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RawBytes;
import org.neo4j.index.internal.gbptree.SimpleByteArrayLayout;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexPopulator.PopulationWorkScheduler;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

@ExtendWith(RandomExtension.class)
class PartMergerTest {
    @Inject
    private RandomSupport random;

    private JobScheduler scheduler;
    private PopulationWorkScheduler populationWorkScheduler;

    @BeforeEach
    void setUp() {
        scheduler = new ThreadPoolJobScheduler();
        populationWorkScheduler = new PopulationWorkScheduler() {
            @Override
            public <T> JobHandle<T> schedule(
                    IndexPopulator.JobDescriptionSupplier descriptionSupplier, Callable<T> job) {
                return scheduler.schedule(Group.INDEX_POPULATION_WORK, NOT_MONITORED, job);
            }
        };
    }

    @AfterEach
    void tearDown() throws Exception {
        scheduler.close();
    }

    @Test
    void shouldMergeParts() throws IOException {
        // given
        Layout<RawBytes, RawBytes> layout = new SimpleByteArrayLayout();
        List<BlockEntry<RawBytes, RawBytes>> allData = new ArrayList<>();
        List<BlockEntryCursor<RawBytes, RawBytes>> parts = buildParts(random, layout, allData);
        PartMerger<RawBytes, RawBytes> merger =
                new PartMerger<>(populationWorkScheduler, parts, layout, null, NOT_CANCELLABLE, 10);

        // when
        try (BlockEntryCursor<RawBytes, RawBytes> stream = merger.startMerge()) {
            // then
            assertMergedPartStream(allData, stream);
        }
    }

    @Test
    void shouldMergeZeroParts() throws IOException {
        // given
        Layout<RawBytes, RawBytes> layout = new SimpleByteArrayLayout();
        PartMerger<RawBytes, RawBytes> merger =
                new PartMerger<>(populationWorkScheduler, emptyList(), layout, null, NOT_CANCELLABLE, 10);

        // when
        try (BlockEntryCursor<RawBytes, RawBytes> stream = merger.startMerge()) {
            // then
            assertThat(stream.next()).isFalse();
        }
    }
}
