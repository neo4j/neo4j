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

import static org.apache.commons.lang3.ArrayUtils.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unordered;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.allEntries;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.SIMPLE_NAME_LOOKUP;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracker.NO_USAGE_TRACKER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;
import static org.neo4j.storageengine.api.IndexEntryUpdate.change;
import static org.neo4j.storageengine.api.IndexEntryUpdate.remove;
import static org.neo4j.test.Race.throwing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import org.eclipse.collections.impl.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.storageengine.api.schema.SimpleEntityClient;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;

@PageCacheExtension
@ExtendWith(RandomExtension.class)
abstract class IndexPopulationStressTest {
    private static final IndexProviderDescriptor PROVIDER = new IndexProviderDescriptor("provider", "1.0");
    private static final int THREADS = 50;
    private static final int MAX_BATCH_SIZE = 100;
    private static final int BATCHES_PER_THREAD = 100;

    @Inject
    private RandomSupport random;

    @Inject
    PageCache pageCache;

    CursorContextFactory contextFactory;

    @Inject
    FileSystemAbstraction fs;

    @Inject
    private TestDirectory testDirectory;

    private final Scheduler scheduler = new Scheduler();
    private final boolean hasValues;
    private final Function<RandomValues, Value> valueGenerator;
    private final Function<IndexPopulationStressTest, IndexProvider> providerCreator;
    private IndexDescriptor descriptor;
    private IndexDescriptor descriptor2;
    private final IndexSamplingConfig samplingConfig = new IndexSamplingConfig(1000, 0.2, true);
    private IndexPopulator populator;
    private IndexProvider indexProvider;
    private TokenNameLookup tokenNameLookup;
    private boolean prevAccessCheck;
    DefaultPageCacheTracer pageCacheTracer;

    abstract IndexType indexType();

    IndexPopulationStressTest(
            boolean hasValues,
            Function<RandomValues, Value> valueGenerator,
            Function<IndexPopulationStressTest, IndexProvider> providerCreator) {
        this.hasValues = hasValues;
        this.valueGenerator = valueGenerator;
        this.providerCreator = providerCreator;
    }

    IndexDirectoryStructure.Factory directory() {
        return directoriesByProvider(testDirectory.homePath());
    }

    @BeforeEach
    void setup() throws IOException, EntityNotFoundException {
        pageCacheTracer = new DefaultPageCacheTracer();
        contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        indexProvider = providerCreator.apply(this);
        tokenNameLookup = SIMPLE_NAME_LOOKUP;
        descriptor = indexProvider.completeConfiguration(
                forSchema(forLabel(0, 0), PROVIDER)
                        .withIndexType(indexType())
                        .withName("index_0")
                        .materialise(0),
                StorageEngineIndexingBehaviour.EMPTY);
        descriptor2 = indexProvider.completeConfiguration(
                forSchema(forLabel(1, 0), PROVIDER)
                        .withIndexType(indexType())
                        .withName("index_1")
                        .materialise(1),
                StorageEngineIndexingBehaviour.EMPTY);
        fs.mkdirs(indexProvider.directoryStructure().rootDirectory());
        populator = indexProvider.getPopulator(
                descriptor,
                samplingConfig,
                heapBufferFactory((int) kibiBytes(40)),
                INSTANCE,
                tokenNameLookup,
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY);
        prevAccessCheck = UnsafeUtil.exchangeNativeAccessCheckEnabled(false);
    }

    @AfterEach
    void tearDown() throws Exception {
        UnsafeUtil.exchangeNativeAccessCheckEnabled(prevAccessCheck);
        if (populator != null) {
            populator.close(true, CursorContext.NULL_CONTEXT);
        }

        scheduler.shutdown();
    }

    @Test
    void stressIt() throws Throwable {
        Race race = new Race();
        AtomicReferenceArray<List<ValueIndexEntryUpdate<?>>> lastBatches = new AtomicReferenceArray<>(THREADS);
        Generator[] generators = new Generator[THREADS];

        populator.create();
        CountDownLatch insertersDone = new CountDownLatch(THREADS);
        ReadWriteLock updateLock = new ReentrantReadWriteLock(true);
        for (int i = 0; i < THREADS; i++) {
            race.addContestant(inserter(lastBatches, generators, insertersDone, updateLock, i), 1);
        }
        Collection<ValueIndexEntryUpdate<?>> updates = new ArrayList<>();
        race.addContestant(updater(lastBatches, insertersDone, updateLock, updates));

        race.go();
        populator.scanCompleted(nullInstance, scheduler, CursorContext.NULL_CONTEXT);
        populator.close(true, CursorContext.NULL_CONTEXT);
        populator = null; // to let the after-method know that we've closed it ourselves

        // then assert that a tree built by a single thread ends up exactly the same
        buildReferencePopulatorSingleThreaded(generators, updates);
        try (IndexAccessor accessor = indexProvider.getOnlineAccessor(
                        descriptor,
                        samplingConfig,
                        tokenNameLookup,
                        Sets.immutable.empty(),
                        StorageEngineIndexingBehaviour.EMPTY);
                IndexAccessor referenceAccessor = indexProvider.getOnlineAccessor(
                        descriptor2,
                        samplingConfig,
                        tokenNameLookup,
                        Sets.immutable.empty(),
                        StorageEngineIndexingBehaviour.EMPTY);
                var reader = accessor.newValueReader(NO_USAGE_TRACKER);
                var referenceReader = referenceAccessor.newValueReader(NO_USAGE_TRACKER)) {
            RecordingClient entries = new RecordingClient();
            RecordingClient referenceEntries = new RecordingClient();
            reader.query(entries, QueryContext.NULL_CONTEXT, unordered(hasValues), allEntries());
            referenceReader.query(referenceEntries, QueryContext.NULL_CONTEXT, unordered(hasValues), allEntries());

            exhaustAndSort(referenceEntries);
            exhaustAndSort(entries);

            // a sanity check that we actually collected something
            assertFalse(entries.records.isEmpty());

            assertThat(entries.records).isEqualTo(referenceEntries.records);
        }
    }

    private void exhaustAndSort(RecordingClient client) {
        while (client.next()) {}

        client.records.sort(Comparator.comparingLong(o -> o.entityId));
    }

    private Runnable updater(
            AtomicReferenceArray<List<ValueIndexEntryUpdate<?>>> lastBatches,
            CountDownLatch insertersDone,
            ReadWriteLock updateLock,
            Collection<ValueIndexEntryUpdate<?>> updates) {
        return throwing(() -> {
            // Entity ids that have been removed, so that additions can reuse them
            List<Long> removed = new ArrayList<>();
            RandomValues randomValues = RandomValues.create(new Random(random.seed() + THREADS));
            while (insertersDone.getCount() > 0) {
                // Do updates now and then
                Thread.sleep(10);
                updateLock.writeLock().lock();
                try (IndexUpdater updater = populator.newPopulatingUpdater(CursorContext.NULL_CONTEXT)) {
                    for (int i = 0; i < THREADS; i++) {
                        List<ValueIndexEntryUpdate<?>> batch = lastBatches.get(i);
                        if (batch != null) {
                            ValueIndexEntryUpdate<?> update = null;
                            switch (randomValues.nextInt(3)) {
                                case 0: // add
                                    if (!removed.isEmpty()) {
                                        Long id = removed.remove(randomValues.nextInt(removed.size()));
                                        update = add(id, descriptor, valueGenerator.apply(randomValues));
                                    }
                                    break;
                                case 1: // remove
                                    ValueIndexEntryUpdate<?> removal = batch.get(randomValues.nextInt(batch.size()));
                                    update = remove(removal.getEntityId(), descriptor, removal.values());
                                    removed.add(removal.getEntityId());
                                    break;
                                case 2: // change
                                    removal = batch.get(randomValues.nextInt(batch.size()));
                                    change(
                                            removal.getEntityId(),
                                            descriptor,
                                            removal.values(),
                                            toArray(valueGenerator.apply(randomValues)));
                                    break;
                                default:
                                    throw new IllegalArgumentException();
                            }
                            if (update != null) {
                                updater.process(update);
                                updates.add(update);
                            }
                        }
                    }
                } finally {
                    updateLock.writeLock().unlock();
                }
            }
        });
    }

    private Runnable inserter(
            AtomicReferenceArray<List<ValueIndexEntryUpdate<?>>> lastBatches,
            Generator[] generators,
            CountDownLatch insertersDone,
            ReadWriteLock updateLock,
            int slot) {
        int worstCaseEntriesPerThread = BATCHES_PER_THREAD * MAX_BATCH_SIZE;
        return throwing(() -> {
            try {
                Generator generator = generators[slot] =
                        new Generator(MAX_BATCH_SIZE, random.seed() + slot, slot * worstCaseEntriesPerThread);
                for (int j = 0; j < BATCHES_PER_THREAD; j++) {
                    List<ValueIndexEntryUpdate<?>> batch = generator.batch(descriptor);
                    updateLock.readLock().lock();
                    try {
                        populator.add(batch, CursorContext.NULL_CONTEXT);
                    } finally {
                        updateLock.readLock().unlock();
                    }
                    lastBatches.set(slot, batch);
                }
            } finally {
                // This helps the updater know when to stop updating
                insertersDone.countDown();
            }
        });
    }

    private void buildReferencePopulatorSingleThreaded(
            Generator[] generators, Collection<ValueIndexEntryUpdate<?>> updates)
            throws IndexEntryConflictException, IOException {
        IndexPopulator referencePopulator = indexProvider.getPopulator(
                descriptor2,
                samplingConfig,
                heapBufferFactory((int) kibiBytes(40)),
                INSTANCE,
                tokenNameLookup,
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY);
        referencePopulator.create();
        boolean referenceSuccess = false;
        try {
            for (Generator generator : generators) {
                generator.reset();
                for (int i = 0; i < BATCHES_PER_THREAD; i++) {
                    referencePopulator.add(generator.batch(descriptor2), CursorContext.NULL_CONTEXT);
                }
            }
            try (IndexUpdater updater = referencePopulator.newPopulatingUpdater(CursorContext.NULL_CONTEXT)) {
                for (ValueIndexEntryUpdate<?> update : updates) {
                    updater.process(update);
                }
            }
            referenceSuccess = true;
            referencePopulator.scanCompleted(nullInstance, scheduler, CursorContext.NULL_CONTEXT);
        } finally {
            referencePopulator.close(referenceSuccess, CursorContext.NULL_CONTEXT);
        }
    }

    private class Generator {
        private final int maxBatchSize;
        private final long seed;
        private final long startEntityId;

        private RandomValues randomValues;
        private long nextEntityId;

        Generator(int maxBatchSize, long seed, long startEntityId) {
            this.startEntityId = startEntityId;
            this.nextEntityId = startEntityId;
            this.maxBatchSize = maxBatchSize;
            this.seed = seed;
            reset();
        }

        private void reset() {
            randomValues = RandomValues.create(new Random(seed));
            nextEntityId = startEntityId;
        }

        List<ValueIndexEntryUpdate<?>> batch(IndexDescriptor descriptor) {
            int n = randomValues.nextInt(maxBatchSize) + 1;
            List<ValueIndexEntryUpdate<?>> updates = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                updates.add(add(nextEntityId++, descriptor, valueGenerator.apply(randomValues)));
            }
            return updates;
        }
    }

    private static class Scheduler implements IndexPopulator.PopulationWorkScheduler {
        private final JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();

        @Override
        public <T> JobHandle<T> schedule(IndexPopulator.JobDescriptionSupplier descriptionSupplier, Callable<T> job) {
            return jobScheduler.schedule(Group.INDEX_POPULATION_WORK, new JobMonitoringParams(null, null, null), job);
        }

        void shutdown() throws Exception {
            jobScheduler.shutdown();
        }
    }

    private static class RecordingClient extends SimpleEntityClient implements IndexProgressor.EntityValueClient {
        final List<IndexRecord> records = new ArrayList<>();

        @Override
        public void initializeQuery(
                IndexDescriptor descriptor,
                IndexProgressor progressor,
                boolean indexIncludesTransactionState,
                boolean needStoreFilter,
                IndexQueryConstraints constraints,
                PropertyIndexQuery... query) {
            initialize(progressor);
        }

        @Override
        public boolean acceptEntity(long reference, float score, Value... values) {
            acceptEntity(reference);
            records.add(new IndexRecord(reference, values));
            return true;
        }

        @Override
        public boolean needsValues() {
            return true;
        }
    }

    private static class IndexRecord {
        private final long entityId;
        private final Value[] values;

        IndexRecord(long entityId, Value[] values) {
            this.entityId = entityId;
            this.values = values;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IndexRecord that = (IndexRecord) o;
            return entityId == that.entityId && Arrays.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(entityId);
            result = 31 * result + Arrays.hashCode(values);
            return result;
        }
    }
}
