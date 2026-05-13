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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.constrained;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unorderedValues;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracking.NO_USAGE_TRACKING;
import static org.neo4j.kernel.impl.index.schema.ValueCreatorUtil.FRACTION_DUPLICATE_NON_UNIQUE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.factory.primitive.LongSets;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexQuery;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.storageengine.api.schema.SimpleEntityValueClient;
import org.neo4j.test.RandomSupport;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

class RangeIndexAccessorTest extends GenericNativeIndexAccessorTests<RangeKey> {
    private static final IndexDescriptor INDEX_DESCRIPTOR = forSchema(forLabel(42, 0))
            .withIndexType(IndexType.RANGE)
            .withIndexProvider(AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
            .withName("index")
            .materialise(0);
    private static final ValueType[] SUPPORTED_TYPES = ValueType.ALL_TYPES;
    private static final RangeLayout LAYOUT = new RangeLayout(1);

    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @AfterEach
    void tearDown() {
        logProvider.clear();
    }

    @Override
    NativeIndexAccessor<RangeKey> createAccessor(PageCache pageCache, IndexDescriptor indexDescriptor) {
        RecoveryCleanupWorkCollector cleanup = RecoveryCleanupWorkCollector.immediate();
        DatabaseIndexContext context = DatabaseIndexContext.builder(
                        pageCache, fs, contextFactory, pageCacheTracer, DEFAULT_DATABASE_NAME)
                .withReadOnlyChecker(writable())
                .build();
        return new RangeIndexAccessor(
                context,
                createIndexFiles(fs, directory, indexDescriptor),
                layout,
                cleanup,
                indexDescriptor,
                tokenNameLookup,
                ElementIdMapper.PLACEHOLDER,
                Sets.immutable.empty(),
                false,
                logProvider);
    }

    @Override
    ValueCreatorUtil<RangeKey> createValueCreatorUtil() {
        return new ValueCreatorUtil<>(INDEX_DESCRIPTOR, SUPPORTED_TYPES, FRACTION_DUPLICATE_NON_UNIQUE);
    }

    @Override
    IndexDescriptor indexDescriptor() {
        return INDEX_DESCRIPTOR;
    }

    @Override
    RangeLayout layout() {
        return LAYOUT;
    }

    @ParameterizedTest
    @MethodSource("unsupportedPredicates")
    void readerShouldThrowOnUnsupportedQuery(PropertyIndexQuery predicate) {
        try (ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING)) {
            IndexNotApplicableKernelException e = assertThrows(IndexNotApplicableKernelException.class, () -> {
                try (SimpleEntityValueClient client = new SimpleEntityValueClient()) {
                    reader.query(client, NULL_CONTEXT, CursorContext.NULL_CONTEXT, unorderedValues(), predicate);
                }
            });
            assertThat(e)
                    .hasMessageContaining(
                            "Tried to query index with illegal query. A %s predicate is not allowed", predicate.type());
            assertThat(e.gqlStatus()).isEqualTo("50N15");
            assertThat(e.statusDescription())
                    .isEqualTo(String.format(
                            "error: general processing exception - unsupported index operation. The system attempted to execute an unsupported operation on index `%s`. See debug.log for more information.",
                            INDEX_DESCRIPTOR.getName()));
            LogAssertions.assertThat(logProvider)
                    .containsMessageWithAll("Tried to query index with illegal query. A %s predicate is not allowed"
                            .formatted(predicate.type()))
                    .containsException(e);
        }
    }

    @Test
    void readerShouldThrowOnUnsupportedCompositePredicates() {
        try (ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING)) {
            IndexNotApplicableKernelException e = assertThrows(IndexNotApplicableKernelException.class, () -> {
                try (SimpleEntityValueClient client = new SimpleEntityValueClient()) {
                    reader.query(
                            client,
                            NULL_CONTEXT,
                            CursorContext.NULL_CONTEXT,
                            unorderedValues(),
                            PropertyIndexQuery.exact(0, Values.stringValue("myValue")),
                            PropertyIndexQuery.allEntries());
                }
            });

            assertThat(e)
                    .hasMessageContaining(
                            "Tried to query index with illegal composite query. %s queries are not allowed in composite query. Query was:",
                            IndexQuery.IndexQueryType.ALL_ENTRIES);
            assertThat(e.gqlStatus()).isEqualTo("50N15");
            assertThat(e.statusDescription())
                    .isEqualTo(String.format(
                            "error: general processing exception - unsupported index operation. The system attempted to execute an unsupported operation on index `%s`. See debug.log for more information.",
                            INDEX_DESCRIPTOR.getName()));
            LogAssertions.assertThat(logProvider)
                    .containsMessageWithAll(
                            "Tried to query index with illegal composite query. %s queries are not allowed in composite query. Query was:"
                                    .formatted(IndexQuery.IndexQueryType.ALL_ENTRIES))
                    .containsException(e);
        }
    }

    @Test
    void readerShouldThrowOnUnsupportedQueryPrecisionInCompositePredicates() {
        try (ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING)) {
            IndexNotApplicableKernelException e = assertThrows(IndexNotApplicableKernelException.class, () -> {
                try (SimpleEntityValueClient client = new SimpleEntityValueClient()) {
                    reader.query(
                            client,
                            NULL_CONTEXT,
                            CursorContext.NULL_CONTEXT,
                            unorderedValues(),
                            PropertyIndexQuery.exists(0),
                            PropertyIndexQuery.exact(0, Values.stringValue("myValue")));
                }
            });
            assertThat(e)
                    .hasMessageContaining(
                            "Tried to query index with illegal composite query. Composite query must have decreasing precision. Query was:");
            assertThat(e.gqlStatus()).isEqualTo("50N15");
            assertThat(e.statusDescription())
                    .isEqualTo(String.format(
                            "error: general processing exception - unsupported index operation. The system attempted to execute an unsupported operation on index `%s`. See debug.log for more information.",
                            INDEX_DESCRIPTOR.getName()));
            LogAssertions.assertThat(logProvider)
                    .containsMessageWithAll(
                            "Tried to query index with illegal composite query. Composite query must have decreasing precision. Query was:")
                    .containsException(e);
        }
    }

    @Test
    void shouldRespectIndexOrderForGeometryTypes() throws Exception {
        // given
        int nUpdates = 10000;
        ValueType[] types = supportedTypesForGeometry();
        Iterator<EagerValueIndexEntryUpdate> randomUpdateGenerator =
                valueCreatorUtil.randomUpdateGenerator(random, types);
        //noinspection unchecked
        EagerValueIndexEntryUpdate[] someUpdates = new EagerValueIndexEntryUpdate[nUpdates];
        for (int i = 0; i < nUpdates; i++) {
            someUpdates[i] = randomUpdateGenerator.next();
        }
        processAll(someUpdates);
        Value[] allValues = ValueCreatorUtil.extractValuesFromUpdates(someUpdates);

        // when
        try (ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING)) {
            PropertyIndexQuery.ExistsPredicate exists = PropertyIndexQuery.exists(0);

            expectIndexOrder(allValues, reader, IndexOrder.ASCENDING, exists);
            expectIndexOrder(allValues, reader, IndexOrder.DESCENDING, exists);
        }
    }

    // This is a particular instance of `shouldSeeAllEntriesBetweenSpecificValues` that previously failed.
    // We keep it here with a fixed seed and slimmer value type candidates to ensure we don't regress.
    @RandomSupport.Seed(1758870311927L)
    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void regressionTestWithSeed(boolean fromBeginning, boolean toEnd) throws Exception {
        ValueType[] valueTypeCandidates = new ValueType[] {ValueType.STRING_ARRAY};
        shouldSeeAllEntriesBetweenSpecificValues(fromBeginning, toEnd, valueTypeCandidates);
    }

    @Test
    void shouldValidateUniquenessAmongShards() throws IOException, IndexEntryConflictException {
        // given
        int totalNumShards = 4;
        int numOtherShards = totalNumShards - 1;
        List<IndexAccessor> otherShards = new ArrayList<>(numOtherShards);
        List<IndexDescriptor> indexDescriptors = new ArrayList<>();
        indexDescriptors.add(indexDescriptor());
        for (int i = 0; i < numOtherShards; i++) {
            IndexDescriptor shardIndexDescriptor = IndexPrototype.forSchema(INDEX_DESCRIPTOR.schema())
                    .withIndexProvider(INDEX_DESCRIPTOR.getIndexProvider())
                    .withName("shard-" + i)
                    .materialise(INDEX_DESCRIPTOR.getId() + 1 + i);
            otherShards.addLast(createAccessor(pageCache, shardIndexDescriptor));
            indexDescriptors.add(shardIndexDescriptor);
        }

        // Inserting a bunch of (across all shards) unique values into the shards
        List<IndexUpdater> updaters = new ArrayList<>();
        updaters.add(accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false));
        for (IndexAccessor shard : otherShards) {
            updaters.add(shard.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false));
        }
        int initialDataSize = 100_000;
        int numConflicts = random.nextInt(100, 1_000);
        Map<ValueTuple, MutableLongSet> data = new ConcurrentHashMap<>();
        try {
            for (int i = 0; i < initialDataSize; i++) {
                int shard = i % totalNumShards;
                IndexUpdater updater = updaters.get(shard);
                IndexDescriptor descriptor = indexDescriptors.get(shard);
                StringValue value = Values.stringValue("Value" + i);
                updater.process(EagerValueIndexEntryUpdate.add(i, descriptor, value));
                data.put(ValueTuple.of(value), LongSets.mutable.of(i));
            }

            MutableIntSet conflictsAdded = IntSets.mutable.empty();
            for (int i = 0; i < numConflicts; i++) {
                // The assumption here is that all these shards are internally unique
                int conflictingValueId;
                do {
                    conflictingValueId = random.nextInt(initialDataSize);
                } while (!conflictsAdded.add(conflictingValueId));

                // Don't make a shard have conflicts, there should only be conflicts across shards
                int shard;
                do {
                    shard = random.nextInt(totalNumShards);
                } while (shard == conflictingValueId % totalNumShards);

                IndexUpdater updater = updaters.get(shard);
                IndexDescriptor descriptor = indexDescriptors.get(shard);
                StringValue value = Values.stringValue("Value" + conflictingValueId);
                long entityId = initialDataSize + i;
                updater.process(EagerValueIndexEntryUpdate.add(entityId, descriptor, value));
                data.get(ValueTuple.of(value)).add(entityId);
            }
        } finally {
            IOUtils.closeAll(updaters);
        }

        try {
            // when
            AtomicInteger foundConflicts = new AtomicInteger();
            accessor.validateShards(
                    otherShards,
                    true,
                    (firstEntityId, firstShardId, otherEntityId, otherShardId, values) -> {
                        foundConflicts.incrementAndGet();
                        MutableLongSet expectedConflict = data.remove(ValueTuple.of(values));
                        assertThat(expectedConflict).isEqualTo(LongSets.mutable.of(firstEntityId, otherEntityId));
                    },
                    4,
                    jobScheduler);

            // then
            assertThat(foundConflicts.get()).isEqualTo(numConflicts);
        } finally {
            IOUtils.closeAll(otherShards);
        }
    }

    private static void expectIndexOrder(
            Value[] allValues,
            ValueIndexReader reader,
            IndexOrder supportedOrder,
            PropertyIndexQuery.ExistsPredicate supportedQuery)
            throws IndexNotApplicableKernelException {
        if (supportedOrder == IndexOrder.ASCENDING) {
            Arrays.sort(allValues, Values.COMPARATOR);
        } else if (supportedOrder == IndexOrder.DESCENDING) {
            Arrays.sort(allValues, Values.COMPARATOR.reversed());
        }
        try (SimpleEntityValueClient client = new SimpleEntityValueClient()) {
            reader.query(
                    client,
                    NULL_CONTEXT,
                    CursorContext.NULL_CONTEXT,
                    constrained(supportedOrder, true),
                    supportedQuery);
            int i = 0;
            while (client.next()) {
                assertEquals(allValues[i++], client.values[0], "values in order");
            }
            assertEquals(i, allValues.length, "found all values");
        }
    }

    private ValueType[] supportedTypesForGeometry() {
        return RandomValues.excluding(valueCreatorUtil.supportedTypes(), type -> switch (type.valueGroup.category()) {
            case GEOMETRY, GEOMETRY_ARRAY -> false;
            default -> true;
        });
    }

    private static Stream<PropertyIndexQuery> unsupportedPredicates() {
        return Stream.of(
                PropertyIndexQuery.boundingBox(
                        0,
                        Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1, 1),
                        Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 2, 2)),
                PropertyIndexQuery.stringSuffix(0, Values.stringValue("myValue")),
                PropertyIndexQuery.stringContains(0, Values.stringValue("myValue")),
                PropertyIndexQuery.fulltextSearch("myValue"),
                PropertyIndexQuery.nearestNeighbors(10, new float[] {1, 2}));
    }
}
