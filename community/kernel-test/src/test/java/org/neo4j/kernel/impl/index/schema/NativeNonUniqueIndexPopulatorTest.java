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

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.kernel.impl.index.schema.ValueCreatorUtil.countUniqueValues;

import java.io.IOException;
import java.util.Iterator;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueType;

abstract class NativeNonUniqueIndexPopulatorTest<KEY extends NativeIndexKey<KEY>>
        extends NativeIndexPopulatorTests<KEY> {
    private final NativeIndexPopulatorTestCases.PopulatorFactory<KEY> populatorFactory;
    private final ValueType[] typesOfGroup;

    NativeNonUniqueIndexPopulatorTest(
            NativeIndexPopulatorTestCases.PopulatorFactory<KEY> populatorFactory,
            ValueType[] typesOfGroup,
            IndexLayout<KEY> layout) {
        this.populatorFactory = populatorFactory;
        this.typesOfGroup = typesOfGroup;
        this.layout = layout;
    }

    private final IndexDescriptor nonUniqueDescriptor = TestIndexDescriptorFactory.forLabel(indexType(), 42, 666);

    abstract IndexType indexType();

    @Override
    NativeIndexPopulator<KEY> createPopulator(PageCache pageCache) throws IOException {
        var cacheTracer = PageCacheTracer.NULL;
        DatabaseIndexContext context = DatabaseIndexContext.builder(
                        pageCache,
                        fs,
                        new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER),
                        cacheTracer,
                        DEFAULT_DATABASE_NAME)
                .build();
        return populatorFactory.create(context, indexFiles, layout, indexDescriptor, tokenNameLookup);
    }

    @Override
    ValueCreatorUtil<KEY> createValueCreatorUtil() {
        return new ValueCreatorUtil<>(
                nonUniqueDescriptor, typesOfGroup, ValueCreatorUtil.FRACTION_DUPLICATE_NON_UNIQUE);
    }

    @Override
    IndexLayout<KEY> layout() {
        return layout;
    }

    @Override
    IndexDescriptor indexDescriptor() {
        return nonUniqueDescriptor;
    }

    @Test
    void addShouldApplyDuplicateValues() throws Exception {
        // given
        populator.create();
        ValueIndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.someUpdatesWithDuplicateValues(random);

        // when
        populator.add(asList(updates), NULL_CONTEXT);

        // then
        populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);
        populator.close(true, NULL_CONTEXT);
        valueUtil.verifyUpdates(updates, this::getTree);
    }

    @Test
    void updaterShouldApplyDuplicateValues() throws Exception {
        // given
        populator.create();
        ValueIndexEntryUpdate<IndexDescriptor>[] updates = valueCreatorUtil.someUpdatesWithDuplicateValues(random);
        try (IndexUpdater updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
            // when
            for (ValueIndexEntryUpdate<IndexDescriptor> update : updates) {
                updater.process(update);
            }
        }

        // then
        populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);
        populator.close(true, NULL_CONTEXT);
        valueUtil.verifyUpdates(updates, this::getTree);
    }

    @Test
    void shouldSampleUpdatesIfConfiguredForOnlineSampling() throws Exception {
        // GIVEN
        try {
            populator.create();
            ValueIndexEntryUpdate<IndexDescriptor>[] scanUpdates = valueCreatorUtil.someUpdates(random);
            populator.add(asList(scanUpdates), NULL_CONTEXT);
            Iterator<ValueIndexEntryUpdate<IndexDescriptor>> generator = valueCreatorUtil.randomUpdateGenerator(random);
            Value[] updates = new Value[5];
            updates[0] = generator.next().values()[0];
            updates[1] = generator.next().values()[0];
            updates[2] = updates[1];
            updates[3] = generator.next().values()[0];
            updates[4] = updates[3];
            try (IndexUpdater updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
                long nodeId = 1000;
                for (Value value : updates) {
                    ValueIndexEntryUpdate<IndexDescriptor> update = valueCreatorUtil.add(nodeId++, value);
                    updater.process(update);
                }
            }

            // WHEN
            populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);
            IndexSample sample = populator.sample(NULL_CONTEXT);

            // THEN
            assertEquals(scanUpdates.length, sample.sampleSize());
            assertEquals(countUniqueValues(scanUpdates), sample.uniqueValues());
            assertEquals(scanUpdates.length, sample.indexSize());
            assertEquals(updates.length, sample.updates());
        } finally {
            populator.close(true, NULL_CONTEXT);
        }
    }
}
