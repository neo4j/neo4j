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
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.eclipse.collections.impl.factory.Sets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaUserDescription;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

@RandomSupportExtension
public class PointBlockBasedIndexPopulatorUpdatesTest extends BlockBasedIndexPopulatorUpdatesTest<PointKey> {
    private static final StandardConfiguration CONFIGURATION = new StandardConfiguration();
    private static final Config CONFIG = Config.defaults();
    private static final IndexSpecificSpaceFillingCurveSettings SPATIAL_SETTINGS =
            IndexSpecificSpaceFillingCurveSettings.fromConfig(CONFIG);
    private static final PointLayout LAYOUT = new PointLayout(SPATIAL_SETTINGS);
    private static final Set<ValueType> UNSUPPORTED_TYPES =
            Collections.unmodifiableSet(Arrays.stream(ValueType.ALL_TYPES)
                    .filter(type -> type.valueGroup.category() != ValueCategory.GEOMETRY)
                    .collect(Collectors.toCollection(() -> EnumSet.noneOf(ValueType.class))));

    @Inject
    private RandomSupport random;

    @Override
    IndexType indexType() {
        return IndexType.POINT;
    }

    @Override
    BlockBasedIndexPopulator<PointKey> instantiatePopulator(IndexDescriptor indexDescriptor) throws IOException {
        PointBlockBasedIndexPopulator populator = new PointBlockBasedIndexPopulator(
                databaseIndexContext,
                indexFiles,
                LAYOUT,
                indexDescriptor,
                SPATIAL_SETTINGS,
                CONFIGURATION,
                false,
                heapBufferFactory((int) kibiBytes(40)),
                CONFIG,
                EmptyMemoryTracker.INSTANCE,
                BlockBasedIndexPopulator.NO_MONITOR,
                Sets.immutable.empty(),
                NullLogProvider.getInstance(),
                SchemaUserDescription.TOKEN_ID_NAME_LOOKUP,
                IndexPopulator.DEFAULT_CONFIGURATION);
        populator.create();
        return populator;
    }

    @Override
    Value supportedValue(int identifier) {
        return Values.pointValue(CoordinateReferenceSystem.CARTESIAN, identifier, identifier);
    }

    @Test
    void shouldIgnoreUnsupportedValueTypesInScan() throws Exception {
        // given
        BlockBasedIndexPopulator<PointKey> populator = instantiatePopulator(INDEX_DESCRIPTOR);
        try {
            // when
            UNSUPPORTED_TYPES.forEach(unsupportedType -> {
                IndexEntryUpdate update =
                        EagerValueIndexEntryUpdate.add(1, INDEX_DESCRIPTOR, random.nextValue(unsupportedType));
                populator.add(singleton(update), NULL_CONTEXT);
            });
            populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);
        } finally {
            populator.close(true, NULL_CONTEXT);
        }

        // then
        try (PointIndexAccessor accessor = pointAccessor();
                BoundedIterable<Long> reader = accessor.newAllEntriesValueReader(NULL_CONTEXT)) {
            assertThat(reader.iterator()).isExhausted();
        }
    }

    @ParameterizedTest
    @EnumSource(ScanUpdateOrder.class)
    final void shouldIgnoreAddedUnsupportedValueTypes(ScanUpdateOrder scanUpdateOrder) throws Exception {
        // given  the population of an empty index
        Collection<IndexEntryUpdate> updates =
                generateUpdatesToIgnore((id, value) -> EagerValueIndexEntryUpdate.add(id, INDEX_DESCRIPTOR, value));
        // when   processing the addition of unsupported value types
        // then   updates should not have been indexed
        test(scanUpdateOrder, updates, 0L);
    }

    @ParameterizedTest
    @EnumSource(ScanUpdateOrder.class)
    final void shouldIgnoreRemovedUnsupportedValueTypes(ScanUpdateOrder scanUpdateOrder) throws Exception {
        // given  the population of an empty index
        Collection<IndexEntryUpdate> updates =
                generateUpdatesToIgnore((id, value) -> EagerValueIndexEntryUpdate.remove(id, INDEX_DESCRIPTOR, value));
        // when   processing the removal of unsupported value types
        // then   updates should not have been indexed
        test(scanUpdateOrder, updates, 0L);
    }

    @ParameterizedTest
    @EnumSource(ScanUpdateOrder.class)
    final void shouldIgnoreChangesBetweenUnsupportedValueTypes(ScanUpdateOrder scanUpdateOrder) throws Exception {
        // given  the population of an empty index
        Value otherValue = random.randomValues().nextValueOfTypes(UNSUPPORTED_TYPES.toArray(ValueType[]::new));
        Collection<IndexEntryUpdate> updates = generateUpdatesToIgnore(
                (id, value) -> EagerValueIndexEntryUpdate.change(id, INDEX_DESCRIPTOR, value, otherValue));
        // when   processing the change between unsupported value types
        // then   updates should not have been indexed
        test(scanUpdateOrder, updates, 0L);
    }

    @ParameterizedTest
    @EnumSource(ScanUpdateOrder.class)
    final void shouldNotIgnoreChangesUnsupportedValueTypesToSupportedValueTypes(ScanUpdateOrder scanUpdateOrder)
            throws Exception {
        // given  the population of an empty index
        Value supportedValue = supportedValue(random.nextInt());
        Collection<IndexEntryUpdate> updates = generateUpdatesToIgnore(
                (id, value) -> EagerValueIndexEntryUpdate.change(id, INDEX_DESCRIPTOR, value, supportedValue));
        // when   processing the change from an unsupported to a supported value type
        // then   updates should have been indexed as additions
        test(scanUpdateOrder, updates, updates.size());
    }

    @ParameterizedTest
    @EnumSource(ScanUpdateOrder.class)
    final void shouldNotIgnoreChangesSupportedValueTypesToUnsupportedValueTypes(ScanUpdateOrder scanUpdateOrder)
            throws Exception {
        // given  the population of an empty index
        Value supportedValue = supportedValue(random.nextInt());
        Collection<IndexEntryUpdate> updates = generateUpdatesToIgnore(
                (id, value) -> EagerValueIndexEntryUpdate.change(id, INDEX_DESCRIPTOR, supportedValue, value));
        // when   processing the change from a supported to an unsupported value type
        // then   updates should have been indexed as removals
        test(scanUpdateOrder, updates, updates.size());
    }

    private void test(ScanUpdateOrder scanUpdateOrder, Collection<IndexEntryUpdate> updates, long expectedUpdateCount)
            throws Exception {
        try (PointIndexAccessor accessor = pointAccessor();
                BoundedIterable<Long> reader = accessor.newAllEntriesValueReader(NULL_CONTEXT)) {
            assertThat(reader.iterator()).isExhausted();
        }

        BlockBasedIndexPopulator<PointKey> populator = instantiatePopulator(INDEX_DESCRIPTOR);
        scanUpdateOrder.beforeUpdates(populator, populationWorkScheduler);
        try (IndexUpdater updater = populator.newPopulatingUpdater(CursorContext.NULL_CONTEXT)) {
            for (IndexEntryUpdate update : updates) {
                updater.process(update);
            }
            scanUpdateOrder.afterUpdates(populator, populationWorkScheduler);
        } finally {
            populator.close(true, CursorContext.NULL_CONTEXT);
        }

        IndexSample sample = populator.sample(CursorContext.NULL_CONTEXT);
        assertThat(sample.indexSize()).isEqualTo(0L);
        assertThat(sample.updates()).isEqualTo(expectedUpdateCount);
    }

    private Collection<IndexEntryUpdate> generateUpdatesToIgnore(
            BiFunction<Long, Value, IndexEntryUpdate> updateFunction) {
        LongSupplier idGen = idGenerator();
        RandomValues randomValues = random.randomValues();
        return UNSUPPORTED_TYPES.stream()
                .map(randomValues::nextValueOfType)
                .map(value -> updateFunction.apply(idGen.getAsLong(), value))
                .toList();
    }

    private static LongSupplier idGenerator() {
        return new AtomicLong(0)::incrementAndGet;
    }

    private PointIndexAccessor pointAccessor() {
        RecoveryCleanupWorkCollector cleanup = RecoveryCleanupWorkCollector.immediate();
        return new PointIndexAccessor(
                databaseIndexContext,
                indexFiles,
                LAYOUT,
                cleanup,
                INDEX_DESCRIPTOR,
                SPATIAL_SETTINGS,
                CONFIGURATION,
                Sets.immutable.empty(),
                false,
                NullLogProvider.getInstance(),
                SchemaUserDescription.TOKEN_ID_NAME_LOOKUP);
    }

    private enum ScanUpdateOrder {
        UPDATES_BEFORE_SCAN_COMPLETE {
            @Override
            void beforeUpdates(
                    IndexPopulator populator, IndexPopulator.PopulationWorkScheduler populationWorkScheduler) {}

            @Override
            void afterUpdates(IndexPopulator populator, IndexPopulator.PopulationWorkScheduler populationWorkScheduler)
                    throws IndexEntryConflictException {
                populator.scanCompleted(PhaseTracker.nullInstance, populationWorkScheduler, CursorContext.NULL_CONTEXT);
            }
        },
        UPDATES_AFTER_SCAN_COMPLETE {
            @Override
            void beforeUpdates(IndexPopulator populator, IndexPopulator.PopulationWorkScheduler populationWorkScheduler)
                    throws IndexEntryConflictException {
                populator.scanCompleted(PhaseTracker.nullInstance, populationWorkScheduler, CursorContext.NULL_CONTEXT);
            }

            @Override
            void afterUpdates(
                    IndexPopulator populator, IndexPopulator.PopulationWorkScheduler populationWorkScheduler) {}
        };

        abstract void beforeUpdates(
                IndexPopulator populator, IndexPopulator.PopulationWorkScheduler populationWorkScheduler)
                throws IndexEntryConflictException;

        abstract void afterUpdates(
                IndexPopulator populator, IndexPopulator.PopulationWorkScheduler populationWorkScheduler)
                throws IndexEntryConflictException;
    }
}
