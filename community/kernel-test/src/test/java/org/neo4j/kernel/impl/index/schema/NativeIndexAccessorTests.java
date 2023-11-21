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

import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.function.Predicates.in;
import static org.neo4j.internal.helpers.collection.Iterables.asUniqueSet;
import static org.neo4j.internal.helpers.collection.Iterators.filter;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.ONLINE;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracker.NO_USAGE_TRACKER;
import static org.neo4j.kernel.impl.index.schema.ValueCreatorUtil.countUniqueValues;
import static org.neo4j.storageengine.api.IndexEntryUpdate.change;
import static org.neo4j.storageengine.api.IndexEntryUpdate.remove;
import static org.neo4j.values.storable.Values.of;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.eclipse.collections.api.iterator.LongIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueType;

abstract class NativeIndexAccessorTests<KEY extends NativeIndexKey<KEY>>
        extends IndexAccessorTests<KEY, NullValue, IndexLayout<KEY>> {
    NativeValueIndexUtility<KEY> valueUtil;
    ValueCreatorUtil<KEY> valueCreatorUtil;

    @BeforeEach
    void setupValueUtil() {
        valueCreatorUtil = createValueCreatorUtil();
        valueUtil = new NativeValueIndexUtility<>(valueCreatorUtil, layout);
    }

    abstract ValueCreatorUtil<KEY> createValueCreatorUtil();

    // UPDATER

    @Test
    void processMustThrowAfterClose() throws Exception {
        // given
        IndexUpdater updater = accessor.newUpdater(ONLINE, NULL_CONTEXT, false);
        updater.close();

        assertThrows(IllegalStateException.class, () -> updater.process(simpleUpdate()));
    }

    @Test
    void shouldIndexAdd() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        try (IndexUpdater updater = accessor.newUpdater(ONLINE, NULL_CONTEXT, false)) {
            // when
            processAll(updater, updates);
        }

        // then
        forceAndCloseAccessor();
        valueUtil.verifyUpdates(updates, this::getTree);
    }

    @Test
    void shouldIndexChange() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        processAll(updates);
        Iterator<ValueIndexEntryUpdate<IndexDescriptor>> generator =
                filter(skipExisting(updates), valueCreatorUtil.randomUpdateGenerator(random));

        for (int i = 0; i < updates.length; i++) {
            ValueIndexEntryUpdate<IndexDescriptor> update = updates[i];
            Value newValue = generator.next().values()[0];
            updates[i] = change(update.getEntityId(), indexDescriptor, update.values()[0], newValue);
        }

        // when
        processAll(updates);

        // then
        forceAndCloseAccessor();
        valueUtil.verifyUpdates(updates, this::getTree);
    }

    @Test
    void shouldIndexRemove() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        processAll(updates);

        for (int i = 0; i < updates.length; i++) {
            // when
            ValueIndexEntryUpdate<IndexDescriptor> update = updates[i];
            ValueIndexEntryUpdate<IndexDescriptor> remove =
                    remove(update.getEntityId(), indexDescriptor, update.values());
            processAll(remove);
            forceAndCloseAccessor();

            // then
            valueUtil.verifyUpdates(Arrays.copyOfRange(updates, i + 1, updates.length), this::getTree);
            setupAccessor();
        }
    }

    @Test
    void shouldHandleRandomUpdates() throws Exception {
        // given
        Set<ValueIndexEntryUpdate<IndexDescriptor>> expectedData = new HashSet<>();
        Iterator<ValueIndexEntryUpdate<IndexDescriptor>> newDataGenerator =
                valueCreatorUtil.randomUpdateGenerator(random);

        // when
        int rounds = 50;
        for (int round = 0; round < rounds; round++) {
            // generate a batch of updates (add, change, remove)
            ValueIndexEntryUpdate<IndexDescriptor>[] batch = generateRandomUpdates(
                    expectedData, newDataGenerator, random.nextInt(5, 20), (float) round / rounds * 2);
            // apply to tree
            processAll(batch);
            // apply to expectedData
            applyUpdatesToExpectedData(expectedData, batch);
            // verifyUpdates
            forceAndCloseAccessor();
            //noinspection unchecked
            valueUtil.verifyUpdates(expectedData.toArray(new ValueIndexEntryUpdate[0]), this::getTree);
            setupAccessor();
        }
    }

    // === READER ===

    @Test
    void tokenReaderShouldThrow() {
        assertThatThrownBy(() -> accessor.newTokenReader(NO_USAGE_TRACKER))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void readingAfterDropShouldThrow() {
        // given
        accessor.drop();

        assertThatThrownBy(() -> accessor.newValueReader(NO_USAGE_TRACKER)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void readingAfterCloseShouldThrow() {
        // given
        accessor.close();

        assertThatThrownBy(() -> accessor.newValueReader(NO_USAGE_TRACKER)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldReturnZeroCountForEmptyIndex() {
        // given
        try (var reader = accessor.newValueReader(NO_USAGE_TRACKER)) {
            // when
            ValueIndexEntryUpdate<IndexDescriptor> update =
                    valueCreatorUtil.randomUpdateGenerator(random).next();
            long count = reader.countIndexedEntities(
                    123,
                    NULL_CONTEXT,
                    valueCreatorUtil.indexDescriptor().schema().getPropertyIds(),
                    update.values()[0]);

            // then
            assertEquals(0, count);
        }
    }

    @Test
    void shouldReturnCountOneForExistingData() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        processAll(updates);

        // when
        try (var reader = accessor.newValueReader(NO_USAGE_TRACKER)) {
            for (ValueIndexEntryUpdate<IndexDescriptor> update : updates) {
                long count = reader.countIndexedEntities(
                        update.getEntityId(),
                        NULL_CONTEXT,
                        valueCreatorUtil.indexDescriptor().schema().getPropertyIds(),
                        update.values());

                // then
                assertEquals(1, count);
            }

            // and when
            Iterator<ValueIndexEntryUpdate<IndexDescriptor>> generator =
                    filter(skipExisting(updates), valueCreatorUtil.randomUpdateGenerator(random));
            long count = reader.countIndexedEntities(
                    123,
                    NULL_CONTEXT,
                    valueCreatorUtil.indexDescriptor().schema().getPropertyIds(),
                    generator.next().values()[0]);

            // then
            assertEquals(0, count);
        }
    }

    @Test
    void shouldReturnCountZeroForMismatchingData() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleTypeNoDuplicates();
        processAll(updates);

        // when
        var reader = accessor.newValueReader(NO_USAGE_TRACKER);

        for (ValueIndexEntryUpdate<IndexDescriptor> update : updates) {
            int[] propKeys = valueCreatorUtil.indexDescriptor().schema().getPropertyIds();
            long countWithMismatchingData =
                    reader.countIndexedEntities(update.getEntityId() + 1, NULL_CONTEXT, propKeys, update.values());
            long countWithNonExistentEntityId =
                    reader.countIndexedEntities(NON_EXISTENT_ENTITY_ID, NULL_CONTEXT, propKeys, update.values());
            long countWithNonExistentValue = reader.countIndexedEntities(
                    update.getEntityId(), NULL_CONTEXT, propKeys, generateUniqueValue(updates));

            // then
            assertEquals(0, countWithMismatchingData);
            assertEquals(0, countWithNonExistentEntityId);
            assertEquals(0, countWithNonExistentValue);
        }
    }

    @Test
    void shouldReturnAllEntriesForAllEntriesPredicate() throws Exception {
        // given
        final var updates = someUpdatesSingleType();
        processAll(updates);

        final var expectedIds =
                Stream.of(updates).mapToLong(ValueIndexEntryUpdate::getEntityId).toArray();
        // when
        try (var reader = accessor.newValueReader(NO_USAGE_TRACKER);
                var result = query(reader, PropertyIndexQuery.allEntries())) {
            // then
            assertEntityIdHits(expectedIds, result);
        }
    }

    @Test
    void shouldReturnMatchingEntriesForExactPredicate() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        processAll(updates);

        // when
        var reader = accessor.newValueReader(NO_USAGE_TRACKER);
        for (ValueIndexEntryUpdate<IndexDescriptor> update : updates) {
            Value value = update.values()[0];
            try (NodeValueIterator result = query(reader, PropertyIndexQuery.exact(0, value))) {
                assertEntityIdHits(extractEntityIds(updates, in(value)), result);
            }
        }
    }

    @Test
    void shouldReturnNoEntriesForMismatchingExactPredicate() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        processAll(updates);

        // when
        var reader = accessor.newValueReader(NO_USAGE_TRACKER);
        Object value = generateUniqueValue(updates);
        try (NodeValueIterator result = query(reader, PropertyIndexQuery.exact(0, value))) {
            assertEntityIdHits(EMPTY_LONG_ARRAY, result);
        }
    }

    @Test
    void shouldHandleMultipleConsecutiveUpdaters() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();

        // when
        for (ValueIndexEntryUpdate<IndexDescriptor> update : updates) {
            try (IndexUpdater updater = accessor.newUpdater(ONLINE, NULL_CONTEXT, false)) {
                updater.process(update);
            }
        }

        // then
        forceAndCloseAccessor();
        valueUtil.verifyUpdates(updates, this::getTree);
    }

    @Test
    void forceShouldCheckpointTree() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] data = someUpdatesSingleType();
        processAll(data);

        // when
        accessor.force(FileFlushEvent.NULL, NULL_CONTEXT);
        accessor.close();

        // then
        valueUtil.verifyUpdates(data, this::getTree);
    }

    @SuppressWarnings("unchecked")
    @Test
    void closeShouldCloseTreeWithoutCheckpoint() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] data = someUpdatesSingleType();
        processAll(data);

        // when
        accessor.close();

        // then
        valueUtil.verifyUpdates(new ValueIndexEntryUpdate[0], this::getTree);
    }

    @Test
    void shouldSampleIndex() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        processAll(updates);
        try (var reader = accessor.newValueReader(NO_USAGE_TRACKER);
                IndexSampler sampler = reader.createSampler()) {
            // when
            IndexSample sample = sampler.sampleIndex(NULL_CONTEXT, new AtomicBoolean());

            // then
            assertEquals(updates.length, sample.indexSize());
            assertEquals(updates.length, sample.sampleSize());
            assertEquals(countUniqueValues(updates), sample.uniqueValues());
        }
    }

    @Test
    void shouldSeeAllEntriesInAllEntriesReader() throws Exception {
        // given
        ValueIndexEntryUpdate<IndexDescriptor>[] updates = someUpdatesSingleType();
        processAll(updates);

        // when
        try (var cursorContext = contextFactory.create("test")) {
            Set<Long> ids;
            try (var reader = accessor.newAllEntriesValueReader(cursorContext)) {
                ids = asUniqueSet(reader);
            }

            // then
            Set<Long> expectedIds = Stream.of(updates)
                    .map(ValueIndexEntryUpdate::getEntityId)
                    .collect(Collectors.toCollection(HashSet::new));
            assertEquals(expectedIds, ids);
            assertThat(cursorContext.getCursorTracer().pins()).isEqualTo(1);
            assertThat(cursorContext.getCursorTracer().unpins()).isEqualTo(1);
            assertThat(cursorContext.getCursorTracer().faults()).isEqualTo(0);
        }
    }

    @Test
    void shouldSeeNoEntriesInAllEntriesReaderOnEmptyIndex() {
        // when
        Set<Long> ids = asUniqueSet(accessor.newAllEntriesValueReader(NULL_CONTEXT));

        // then
        Set<Long> expectedIds = Collections.emptySet();
        assertEquals(expectedIds, ids);
    }

    @Test
    void dropShouldDeleteEntireIndexFolder() {
        // given
        assertFilePresent();

        // when
        accessor.drop();

        // then
        assertFalse(fs.fileExists(indexFiles.getBase()));
    }

    private Value generateUniqueValue(ValueIndexEntryUpdate<IndexDescriptor>[] updates) {
        return filter(skipExisting(updates), valueCreatorUtil.randomUpdateGenerator(random))
                .next()
                .values()[0];
    }

    private static Predicate<ValueIndexEntryUpdate<IndexDescriptor>> skipExisting(
            ValueIndexEntryUpdate<IndexDescriptor>[] existing) {
        return update -> {
            for (ValueIndexEntryUpdate<IndexDescriptor> e : existing) {
                if (Arrays.equals(e.values(), update.values())) {
                    return false;
                }
            }
            return true;
        };
    }

    static NodeValueIterator query(ValueIndexReader reader, PropertyIndexQuery query)
            throws IndexNotApplicableKernelException {
        NodeValueIterator client = new NodeValueIterator();
        reader.query(client, QueryContext.NULL_CONTEXT, unconstrained(), query);
        return client;
    }

    static void assertEntityIdHits(long[] expected, LongIterator result) {
        long[] actual = PrimitiveLongCollections.asArray(result);
        assertSameContent(expected, actual);
    }

    static void assertSameContent(long[] expected, long[] actual) {
        Arrays.sort(actual);
        Arrays.sort(expected);
        assertArrayEquals(
                expected,
                actual,
                format(
                        "Expected arrays to be equal but wasn't.%nexpected:%s%n  actual:%s%n",
                        Arrays.toString(expected), Arrays.toString(actual)));
    }

    static long[] extractEntityIds(ValueIndexEntryUpdate<?>[] updates, Predicate<Value> valueFilter) {
        long[] entityIds = new long[updates.length];
        int cursor = 0;
        for (ValueIndexEntryUpdate<?> update : updates) {
            if (valueFilter.test(update.values()[0])) {
                entityIds[cursor++] = update.getEntityId();
            }
        }
        return Arrays.copyOf(entityIds, cursor);
    }

    private void applyUpdatesToExpectedData(
            Set<ValueIndexEntryUpdate<IndexDescriptor>> expectedData, ValueIndexEntryUpdate<IndexDescriptor>[] batch) {
        for (ValueIndexEntryUpdate<IndexDescriptor> update : batch) {
            ValueIndexEntryUpdate<IndexDescriptor> addition = null;
            ValueIndexEntryUpdate<IndexDescriptor> removal = null;
            switch (update.updateMode()) {
                case ADDED -> addition = valueCreatorUtil.add(update.getEntityId(), update.values()[0]);
                case CHANGED -> {
                    addition = valueCreatorUtil.add(update.getEntityId(), update.values()[0]);
                    removal = valueCreatorUtil.add(update.getEntityId(), update.beforeValues()[0]);
                }
                case REMOVED -> removal = valueCreatorUtil.add(update.getEntityId(), update.values()[0]);
                default -> throw new IllegalArgumentException(
                        update.updateMode().name());
            }

            if (removal != null) {
                expectedData.remove(removal);
            }
            if (addition != null) {
                expectedData.add(addition);
            }
        }
    }

    private ValueIndexEntryUpdate<IndexDescriptor>[] generateRandomUpdates(
            Set<ValueIndexEntryUpdate<IndexDescriptor>> expectedData,
            Iterator<ValueIndexEntryUpdate<IndexDescriptor>> newDataGenerator,
            int count,
            float removeFactor) {
        @SuppressWarnings("unchecked")
        ValueIndexEntryUpdate<IndexDescriptor>[] updates = new ValueIndexEntryUpdate[count];
        float addChangeRatio = 0.5f;
        for (int i = 0; i < count; i++) {
            float factor = random.nextFloat();
            if (!expectedData.isEmpty() && factor < removeFactor) {
                // remove something
                ValueIndexEntryUpdate<IndexDescriptor> toRemove = selectRandomItem(expectedData);
                updates[i] = remove(toRemove.getEntityId(), indexDescriptor, toRemove.values());
            } else if (!expectedData.isEmpty() && factor < (1 - removeFactor) * addChangeRatio) {
                // change
                ValueIndexEntryUpdate<IndexDescriptor> toChange = selectRandomItem(expectedData);
                // use the data generator to generate values, even if the whole update as such won't be used
                ValueIndexEntryUpdate<IndexDescriptor> updateContainingValue = newDataGenerator.next();
                updates[i] = change(
                        toChange.getEntityId(), indexDescriptor, toChange.values(), updateContainingValue.values());
            } else {
                // add
                updates[i] = newDataGenerator.next();
            }
        }
        return updates;
    }

    @SuppressWarnings("unchecked")
    private ValueIndexEntryUpdate<IndexDescriptor> selectRandomItem(
            Set<ValueIndexEntryUpdate<IndexDescriptor>> expectedData) {
        return expectedData.toArray(new ValueIndexEntryUpdate[0])[random.nextInt(expectedData.size())];
    }

    @SafeVarargs
    final void processAll(ValueIndexEntryUpdate<IndexDescriptor>... updates) throws IndexEntryConflictException {
        try (IndexUpdater updater = accessor.newUpdater(ONLINE, NULL_CONTEXT, false)) {
            for (ValueIndexEntryUpdate<IndexDescriptor> update : updates) {
                updater.process(update);
            }
        }
    }

    private void forceAndCloseAccessor() {
        accessor.force(FileFlushEvent.NULL, NULL_CONTEXT);
        closeAccessor();
    }

    private static void processAll(IndexUpdater updater, ValueIndexEntryUpdate<IndexDescriptor>[] updates)
            throws IndexEntryConflictException {
        for (ValueIndexEntryUpdate<IndexDescriptor> update : updates) {
            updater.process(update);
        }
    }

    private ValueIndexEntryUpdate<IndexDescriptor> simpleUpdate() {
        return ValueIndexEntryUpdate.add(0, indexDescriptor, of(0));
    }

    ValueIndexEntryUpdate<IndexDescriptor>[] someUpdatesSingleType() {
        ValueType type = random.randomValues().among(valueCreatorUtil.supportedTypes());
        return valueCreatorUtil.someUpdates(random, new ValueType[] {type}, true);
    }

    private ValueIndexEntryUpdate<IndexDescriptor>[] someUpdatesSingleTypeNoDuplicates() {
        return someUpdatesSingleTypeNoDuplicates(valueCreatorUtil.supportedTypes());
    }

    ValueIndexEntryUpdate<IndexDescriptor>[] someUpdatesSingleTypeNoDuplicates(ValueType... types) {
        ValueType type;
        do {
            // Can not generate enough unique values of boolean
            type = random.randomValues().among(types);
        } while (type == ValueType.BOOLEAN);
        return valueCreatorUtil.someUpdates(random, new ValueType[] {type}, false);
    }
}
