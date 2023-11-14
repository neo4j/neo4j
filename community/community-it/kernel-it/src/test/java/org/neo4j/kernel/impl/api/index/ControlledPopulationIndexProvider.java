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
package org.neo4j.kernel.impl.api.index;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.test.DoubleLatch.awaitLatch;

import java.nio.file.OpenOption;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.MinimalIndexAccessor;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.Barrier;

public class ControlledPopulationIndexProvider extends BaseTestingIndexProvider {
    private IndexPopulator mockedPopulator = new IndexPopulator.Adapter();
    private final IndexAccessor mockedWriter = mock(IndexAccessor.class);
    private final CountDownLatch writerLatch = new CountDownLatch(1);
    private InternalIndexState initialIndexState = POPULATING;
    final AtomicInteger populatorCallCount = new AtomicInteger();
    final AtomicInteger writerCallCount = new AtomicInteger();

    public static final IndexProviderDescriptor PROVIDER_DESCRIPTOR =
            new IndexProviderDescriptor("controlled-population", "1.0");

    public ControlledPopulationIndexProvider() {
        super(PROVIDER_DESCRIPTOR, IndexDirectoryStructure.NONE);
        setInitialIndexState(initialIndexState);
        when(mockedWriter.newValueReader(any())).thenReturn(ValueIndexReader.EMPTY);
    }

    public Barrier.Control installPopulationLatch(PopulationLatchMethod method) {
        Barrier.Control barrier = new Barrier.Control();
        mockedPopulator = new IndexPopulator.Adapter() {
            @Override
            public void create() {
                if (method == PopulationLatchMethod.CREATE) {
                    barrier.reached();
                }
                super.create();
            }

            @Override
            public void add(Collection<? extends IndexEntryUpdate<?>> updates, CursorContext cursorContext) {
                if (method == PopulationLatchMethod.ADD_BATCH) {
                    barrier.reached();
                }
                super.add(updates, cursorContext);
            }
        };
        return barrier;
    }

    public void awaitFullyPopulated() {
        awaitLatch(writerLatch);
    }

    @Override
    public IndexPopulator getPopulator(
            IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig,
            ByteBufferFactory bufferFactory,
            MemoryTracker memoryTracker,
            TokenNameLookup tokenNameLookup,
            ImmutableSet<OpenOption> openOptions,
            StorageEngineIndexingBehaviour indexingBehaviour) {
        populatorCallCount.incrementAndGet();
        return mockedPopulator;
    }

    @Override
    public IndexAccessor getOnlineAccessor(
            IndexDescriptor indexConfig,
            IndexSamplingConfig samplingConfig,
            TokenNameLookup tokenNameLookup,
            ImmutableSet<OpenOption> openOptions,
            boolean readOnly,
            StorageEngineIndexingBehaviour indexingBehaviour) {
        writerCallCount.incrementAndGet();
        writerLatch.countDown();
        return mockedWriter;
    }

    @Override
    public MinimalIndexAccessor getMinimalIndexAccessor(IndexDescriptor descriptor, boolean forRebuildDuringRecovery) {
        return mockedWriter;
    }

    @Override
    public InternalIndexState getInitialState(
            IndexDescriptor descriptor, CursorContext cursorContext, ImmutableSet<OpenOption> openOptions) {
        return initialIndexState;
    }

    public void setInitialIndexState(InternalIndexState initialIndexState) {
        this.initialIndexState = initialIndexState;
    }

    public enum PopulationLatchMethod {
        ADD_BATCH,
        CREATE
    }
}
