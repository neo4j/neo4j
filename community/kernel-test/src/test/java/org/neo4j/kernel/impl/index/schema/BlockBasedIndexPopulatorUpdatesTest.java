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

import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unorderedValues;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.SIMPLE_NAME_LOOKUP;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;

import java.io.IOException;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.storageengine.api.schema.SimpleEntityValueClient;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.Value;

@PageCacheExtension
abstract class BlockBasedIndexPopulatorUpdatesTest<KEY extends NativeIndexKey<KEY>> {
    final IndexDescriptor INDEX_DESCRIPTOR = forSchema(forLabel(1, 1))
            .withName("index")
            .withIndexType(indexType())
            .materialise(1);
    private final IndexDescriptor UNIQUE_INDEX_DESCRIPTOR = uniqueForSchema(forLabel(1, 1))
            .withName("constraint")
            .withIndexType(indexType())
            .materialise(1);
    final TokenNameLookup tokenNameLookup = SIMPLE_NAME_LOOKUP;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @Inject
    private PageCache pageCache;

    IndexFiles indexFiles;
    DatabaseIndexContext databaseIndexContext;
    private JobScheduler jobScheduler;
    IndexPopulator.PopulationWorkScheduler populationWorkScheduler;

    abstract IndexType indexType();

    abstract BlockBasedIndexPopulator<KEY> instantiatePopulator(IndexDescriptor indexDescriptor) throws IOException;

    abstract Value supportedValue(int identifier);

    @BeforeEach
    void setup() {
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor("test", "v1");
        IndexDirectoryStructure directoryStructure =
                directoriesByProvider(directory.homePath()).forProvider(providerDescriptor);
        indexFiles = new IndexFiles(fs, directoryStructure, INDEX_DESCRIPTOR.getId());
        var pageCacheTracer = PageCacheTracer.NULL;
        databaseIndexContext = DatabaseIndexContext.builder(
                        pageCache,
                        fs,
                        new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                        pageCacheTracer,
                        DEFAULT_DATABASE_NAME)
                .build();
        jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        populationWorkScheduler = new IndexPopulator.PopulationWorkScheduler() {

            @Override
            public <T> JobHandle<T> schedule(
                    IndexPopulator.JobDescriptionSupplier descriptionSupplier, Callable<T> job) {
                return jobScheduler.schedule(
                        Group.INDEX_POPULATION_WORK, new JobMonitoringParams(null, null, null), job);
            }
        };
    }

    @AfterEach
    void tearDown() throws Exception {
        jobScheduler.shutdown();
    }

    @Test
    void shouldSeeExternalUpdateBothBeforeAndAfterScanCompleted() throws IndexEntryConflictException, IOException {
        // given
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(INDEX_DESCRIPTOR);
        try {
            // when
            Value first = supportedValue(1);
            Value second = supportedValue(2);
            int firstId = 1;
            int secondId = 2;
            externalUpdate(populator, first, firstId);
            populator.scanCompleted(nullInstance, populationWorkScheduler, CursorContext.NULL_CONTEXT);
            externalUpdate(populator, second, secondId);

            // then
            assertMatch(populator, first, firstId);
            assertMatch(populator, second, secondId);
        } finally {
            populator.close(true, CursorContext.NULL_CONTEXT);
        }
    }

    @Test
    void shouldThrowOnDuplicatedValuesFromScan() throws IOException {
        // given
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(UNIQUE_INDEX_DESCRIPTOR);
        try {
            // when
            Value duplicate = supportedValue(1);
            ValueIndexEntryUpdate<?> firstScanUpdate = ValueIndexEntryUpdate.add(1, INDEX_DESCRIPTOR, duplicate);
            ValueIndexEntryUpdate<?> secondScanUpdate = ValueIndexEntryUpdate.add(2, INDEX_DESCRIPTOR, duplicate);
            assertThrows(IndexEntryConflictException.class, () -> {
                populator.add(singleton(firstScanUpdate), CursorContext.NULL_CONTEXT);
                populator.add(singleton(secondScanUpdate), CursorContext.NULL_CONTEXT);
                populator.scanCompleted(nullInstance, populationWorkScheduler, CursorContext.NULL_CONTEXT);
            });
        } finally {
            populator.close(true, CursorContext.NULL_CONTEXT);
        }
    }

    @Test
    void shouldThrowOnDuplicatedValuesFromExternalUpdates() throws IOException {
        // given
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(UNIQUE_INDEX_DESCRIPTOR);
        try {
            // when
            Value duplicate = supportedValue(1);
            ValueIndexEntryUpdate<?> firstExternalUpdate = ValueIndexEntryUpdate.add(1, INDEX_DESCRIPTOR, duplicate);
            ValueIndexEntryUpdate<?> secondExternalUpdate = ValueIndexEntryUpdate.add(2, INDEX_DESCRIPTOR, duplicate);
            assertThrows(IndexEntryConflictException.class, () -> {
                try (IndexUpdater updater = populator.newPopulatingUpdater(CursorContext.NULL_CONTEXT)) {
                    updater.process(firstExternalUpdate);
                    updater.process(secondExternalUpdate);
                }
                populator.scanCompleted(nullInstance, populationWorkScheduler, CursorContext.NULL_CONTEXT);
            });
        } finally {
            populator.close(true, CursorContext.NULL_CONTEXT);
        }
    }

    @Test
    void shouldThrowOnDuplicatedValuesFromScanAndExternalUpdates() throws IOException {
        // given
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(UNIQUE_INDEX_DESCRIPTOR);
        try {
            // when
            Value duplicate = supportedValue(1);
            ValueIndexEntryUpdate<?> externalUpdate = ValueIndexEntryUpdate.add(1, INDEX_DESCRIPTOR, duplicate);
            ValueIndexEntryUpdate<?> scanUpdate = ValueIndexEntryUpdate.add(2, INDEX_DESCRIPTOR, duplicate);
            assertThrows(IndexEntryConflictException.class, () -> {
                try (IndexUpdater updater = populator.newPopulatingUpdater(CursorContext.NULL_CONTEXT)) {
                    updater.process(externalUpdate);
                }
                populator.add(singleton(scanUpdate), CursorContext.NULL_CONTEXT);
                populator.scanCompleted(nullInstance, populationWorkScheduler, CursorContext.NULL_CONTEXT);
            });
        } finally {
            populator.close(true, CursorContext.NULL_CONTEXT);
        }
    }

    @Test
    void shouldNotThrowOnDuplicationsLaterFixedByExternalUpdates() throws IndexEntryConflictException, IOException {
        // given
        BlockBasedIndexPopulator<KEY> populator = instantiatePopulator(UNIQUE_INDEX_DESCRIPTOR);
        try {
            // when
            Value duplicate = supportedValue(1);
            Value unique = supportedValue(2);
            ValueIndexEntryUpdate<?> firstScanUpdate = ValueIndexEntryUpdate.add(1, INDEX_DESCRIPTOR, duplicate);
            ValueIndexEntryUpdate<?> secondScanUpdate = ValueIndexEntryUpdate.add(2, INDEX_DESCRIPTOR, duplicate);
            ValueIndexEntryUpdate<?> externalUpdate =
                    ValueIndexEntryUpdate.change(1, INDEX_DESCRIPTOR, duplicate, unique);
            populator.add(singleton(firstScanUpdate), CursorContext.NULL_CONTEXT);
            try (IndexUpdater updater = populator.newPopulatingUpdater(CursorContext.NULL_CONTEXT)) {
                updater.process(externalUpdate);
            }
            populator.add(singleton(secondScanUpdate), CursorContext.NULL_CONTEXT);
            populator.scanCompleted(nullInstance, populationWorkScheduler, CursorContext.NULL_CONTEXT);

            // then
            assertHasEntry(populator, unique, 1);
            assertHasEntry(populator, duplicate, 2);
        } finally {
            populator.close(true, CursorContext.NULL_CONTEXT);
        }
    }

    void assertHasEntry(BlockBasedIndexPopulator<KEY> populator, Value entry, int expectedId) {
        try (NativeIndexReader<KEY> reader = populator.newReader()) {
            SimpleEntityValueClient valueClient = new SimpleEntityValueClient();
            PropertyIndexQuery.ExactPredicate exact =
                    PropertyIndexQuery.exact(INDEX_DESCRIPTOR.schema().getPropertyId(), entry);
            reader.query(valueClient, QueryContext.NULL_CONTEXT, AccessMode.Static.READ, unconstrained(), exact);
            assertTrue(valueClient.next());
            long id = valueClient.reference;
            assertEquals(expectedId, id);
        }
    }

    void externalUpdate(BlockBasedIndexPopulator<KEY> populator, Value value, int entityId) {
        try (IndexUpdater indexUpdater = populator.newPopulatingUpdater(CursorContext.NULL_CONTEXT)) {
            // After scanCompleted
            indexUpdater.process(add(entityId, INDEX_DESCRIPTOR, value));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void assertMatch(BlockBasedIndexPopulator<KEY> populator, Value value, long id) {
        try (NativeIndexReader<KEY> reader = populator.newReader()) {
            SimpleEntityValueClient cursor = new SimpleEntityValueClient();
            reader.query(
                    cursor,
                    QueryContext.NULL_CONTEXT,
                    AccessMode.Static.READ,
                    unorderedValues(),
                    PropertyIndexQuery.exact(INDEX_DESCRIPTOR.schema().getPropertyId(), value));
            assertTrue(cursor.next());
            assertEquals(id, cursor.reference);
            assertEquals(value, cursor.values[0]);
            assertFalse(cursor.next());
        }
    }
}
