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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.index.schema.BlockBasedIndexPopulator.NO_MONITOR;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.eclipse.collections.impl.factory.Sets;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaUserDescription;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
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
public class PointBlockBasedIndexPopulatorTest extends BlockBasedIndexPopulatorTest<PointKey> {
    private static final StandardConfiguration CONFIGURATION = new StandardConfiguration();
    private static final Config CONFIG = Config.defaults(GraphDatabaseInternalSettings.index_populator_merge_factor, 2);
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
    PointBlockBasedIndexPopulator instantiatePopulator(
            BlockBasedIndexPopulator.Monitor monitor,
            ByteBufferFactory bufferFactory,
            MemoryTracker memoryTracker,
            IndexPopulator.Configuration configuration)
            throws IOException {
        PointBlockBasedIndexPopulator populator = new PointBlockBasedIndexPopulator(
                databaseIndexContext,
                indexFiles,
                LAYOUT,
                INDEX_DESCRIPTOR,
                SPATIAL_SETTINGS,
                CONFIGURATION,
                false,
                bufferFactory,
                CONFIG,
                memoryTracker,
                monitor,
                Sets.immutable.empty(),
                NullLogProvider.getInstance(),
                SchemaUserDescription.TOKEN_ID_NAME_LOOKUP,
                IndexPopulator.DEFAULT_CONFIGURATION);
        populator.create();
        return populator;
    }

    @Override
    PointLayout layout() {
        return LAYOUT;
    }

    @Override
    protected Value supportedValue(int i) {
        return Values.pointValue(CoordinateReferenceSystem.CARTESIAN, i, i);
    }

    @Test
    final void shouldIgnoreAddedUnsupportedValueTypes() throws Exception {
        // given  the population of an empty index
        try (PointIndexAccessor accessor = pointAccessor();
                BoundedIterable<Long> reader = accessor.newAllEntriesValueReader(NULL_CONTEXT)) {
            assertThat(reader.iterator()).isExhausted();
        }
        BlockBasedIndexPopulator<PointKey> populator = instantiatePopulator(NO_MONITOR);
        assertThat(populator.indexConfig()).isEqualTo(spatialSettingsAsMap(SPATIAL_SETTINGS));

        try {
            LongSupplier idGen = idGenerator();
            RandomValues randomValues = random.randomValues();

            // when   processing unsupported value types
            List<EagerValueIndexEntryUpdate> updates = UNSUPPORTED_TYPES.stream()
                    .map(randomValues::nextValueOfType)
                    .map(value -> EagerValueIndexEntryUpdate.add(idGen.getAsLong(), INDEX_DESCRIPTOR, value))
                    .toList();

            populator.add(updates, CursorContext.NULL_CONTEXT);
            populator.scanCompleted(PhaseTracker.nullInstance, populationWorkScheduler, CursorContext.NULL_CONTEXT);
        } finally {
            populator.close(true, CursorContext.NULL_CONTEXT);
        }

        try (PointIndexAccessor accessor = pointAccessor();
                BoundedIterable<Long> reader = accessor.newAllEntriesValueReader(NULL_CONTEXT)) {
            // then   updates should not have been indexed
            assertThat(reader.iterator()).isExhausted();
        }
    }

    private static Map<String, Value> spatialSettingsAsMap(IndexSpecificSpaceFillingCurveSettings spatialSettings) {
        Map<String, Value> values = new HashMap<>();
        spatialSettings.visitIndexSpecificSettings(new SpatialConfigVisitor(values));
        return values;
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
}
