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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

@ExtendWith(RandomExtension.class)
public class PointBlockBasedIndexPopulatorUpdatesTest extends BlockBasedIndexPopulatorUpdatesTest<PointKey> {
    private static final StandardConfiguration CONFIGURATION = new StandardConfiguration();
    private static final Config CONFIG = Config.defaults();
    private static final IndexSpecificSpaceFillingCurveSettings SPATIAL_SETTINGS =
            IndexSpecificSpaceFillingCurveSettings.fromConfig(CONFIG);
    private static final PointLayout LAYOUT = new PointLayout(SPATIAL_SETTINGS);
    private static final Set<ValueType> UNSUPPORTED_TYPES =
            Collections.unmodifiableSet(Arrays.stream(ValueType.values())
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
        final var populator = new PointBlockBasedIndexPopulator(
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
                Sets.immutable.empty());
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
                IndexEntryUpdate<IndexDescriptor> update =
                        IndexEntryUpdate.add(1, INDEX_DESCRIPTOR, random.nextValue(unsupportedType));
                populator.add(singleton(update), NULL_CONTEXT);
            });
            populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);
        } finally {
            populator.close(true, NULL_CONTEXT);
        }

        // then
        try (var accessor = pointAccessor();
                var reader = accessor.newAllEntriesValueReader(NULL_CONTEXT)) {
            assertThat(reader.iterator()).isExhausted();
        }
    }

    @ParameterizedTest
    @EnumSource(ScanUpdateOrder.class)
    final void shouldIgnoreAddedUnsupportedValueTypes(ScanUpdateOrder scanUpdateOrder) throws Exception {
        // given  the population of an empty index
        final var updates = generateUpdatesToIgnore((id, value) -> IndexEntryUpdate.add(id, INDEX_DESCRIPTOR, value));
        // when   processing the addition of unsupported value types
        // then   updates should not have been indexed
        test(scanUpdateOrder, updates, 0L);
    }

    @ParameterizedTest
    @EnumSource(ScanUpdateOrder.class)
    final void shouldIgnoreRemovedUnsupportedValueTypes(ScanUpdateOrder scanUpdateOrder) throws Exception {
        // given  the population of an empty index
        final var updates =
                generateUpdatesToIgnore((id, value) -> IndexEntryUpdate.remove(id, INDEX_DESCRIPTOR, value));
        // when   processing the removal of unsupported value types
        // then   updates should not have been indexed
        test(scanUpdateOrder, updates, 0L);
    }

    @ParameterizedTest
    @EnumSource(ScanUpdateOrder.class)
    final void shouldIgnoreChangesBetweenUnsupportedValueTypes(ScanUpdateOrder scanUpdateOrder) throws Exception {
        // given  the population of an empty index
        final var otherValue = random.randomValues().nextValueOfTypes(UNSUPPORTED_TYPES.toArray(ValueType[]::new));
        final var updates = generateUpdatesToIgnore(
                (id, value) -> IndexEntryUpdate.change(id, INDEX_DESCRIPTOR, value, otherValue));
        // when   processing the change between unsupported value types
        // then   updates should not have been indexed
        test(scanUpdateOrder, updates, 0L);
    }

    @ParameterizedTest
    @EnumSource(ScanUpdateOrder.class)
    final void shouldNotIgnoreChangesUnsupportedValueTypesToSupportedValueTypes(ScanUpdateOrder scanUpdateOrder)
            throws Exception {
        // given  the population of an empty index
        final var supportedValue = supportedValue(random.nextInt());
        final var updates = generateUpdatesToIgnore(
                (id, value) -> IndexEntryUpdate.change(id, INDEX_DESCRIPTOR, value, supportedValue));
        // when   processing the change from an unsupported to a supported value type
        // then   updates should have been indexed as additions
        test(scanUpdateOrder, updates, updates.size());
    }

    @ParameterizedTest
    @EnumSource(ScanUpdateOrder.class)
    final void shouldNotIgnoreChangesSupportedValueTypesToUnsupportedValueTypes(ScanUpdateOrder scanUpdateOrder)
            throws Exception {
        // given  the population of an empty index
        final var supportedValue = supportedValue(random.nextInt());
        final var updates = generateUpdatesToIgnore(
                (id, value) -> IndexEntryUpdate.change(id, INDEX_DESCRIPTOR, supportedValue, value));
        // when   processing the change from a supported to an unsupported value type
        // then   updates should have been indexed as removals
        test(scanUpdateOrder, updates, updates.size());
    }

    private void test(
            ScanUpdateOrder scanUpdateOrder, Collection<IndexEntryUpdate<?>> updates, long expectedUpdateCount)
            throws Exception {
        try (var accessor = pointAccessor();
                var reader = accessor.newAllEntriesValueReader(NULL_CONTEXT)) {
            assertThat(reader.iterator()).isExhausted();
        }

        final var populator = instantiatePopulator(INDEX_DESCRIPTOR);
        scanUpdateOrder.beforeUpdates(populator, populationWorkScheduler);
        try (var updater = populator.newPopulatingUpdater(CursorContext.NULL_CONTEXT)) {
            for (final var update : updates) {
                updater.process(update);
            }
            scanUpdateOrder.afterUpdates(populator, populationWorkScheduler);
        } finally {
            populator.close(true, CursorContext.NULL_CONTEXT);
        }

        final var sample = populator.sample(CursorContext.NULL_CONTEXT);
        assertThat(sample.indexSize()).isEqualTo(0L);
        assertThat(sample.updates()).isEqualTo(expectedUpdateCount);
    }

    private Collection<IndexEntryUpdate<?>> generateUpdatesToIgnore(
            BiFunction<Long, Value, IndexEntryUpdate<?>> updateFunction) {
        final var idGen = idGenerator();
        final var randomValues = random.randomValues();
        return UNSUPPORTED_TYPES.stream()
                .map(randomValues::nextValueOfType)
                .map(value -> updateFunction.apply(idGen.getAsLong(), value))
                .collect(Collectors.toUnmodifiableList());
    }

    private static LongSupplier idGenerator() {
        return new AtomicLong(0)::incrementAndGet;
    }

    private PointIndexAccessor pointAccessor() {
        final var cleanup = RecoveryCleanupWorkCollector.immediate();
        return new PointIndexAccessor(
                databaseIndexContext,
                indexFiles,
                LAYOUT,
                cleanup,
                INDEX_DESCRIPTOR,
                SPATIAL_SETTINGS,
                CONFIGURATION,
                Sets.immutable.empty(),
                false);
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
