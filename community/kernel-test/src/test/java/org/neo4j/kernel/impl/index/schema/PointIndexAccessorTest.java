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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import org.eclipse.collections.impl.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaUserDescription;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.storageengine.api.schema.SimpleEntityValueClient;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

class PointIndexAccessorTest extends NativeIndexAccessorTests<PointKey> {
    private static final IndexSpecificSpaceFillingCurveSettings SPACE_FILLING_CURVE_SETTINGS =
            IndexSpecificSpaceFillingCurveSettings.fromConfig(Config.defaults());
    private static final StandardConfiguration CONFIGURATION = new StandardConfiguration();
    private static final IndexDescriptor INDEX_DESCRIPTOR = forSchema(forLabel(42, 666))
            .withIndexType(IndexType.POINT)
            .withIndexProvider(AllIndexProviderDescriptors.POINT_DESCRIPTOR)
            .withName("index")
            .materialise(0);

    private static final PointLayout LAYOUT = new PointLayout(SPACE_FILLING_CURVE_SETTINGS);

    private static final ValueType[] SUPPORTED_TYPES = Stream.of(ValueType.ALL_TYPES)
            .filter(type -> type.valueGroup.category() == ValueCategory.GEOMETRY)
            .toArray(ValueType[]::new);

    private static final ValueType[] UNSUPPORTED_TYPES = Stream.of(ValueType.ALL_TYPES)
            .filter(type -> type.valueGroup.category() != ValueCategory.GEOMETRY)
            .toArray(ValueType[]::new);

    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @AfterEach
    void tearDown() {
        logProvider.clear();
    }

    @Override
    ValueCreatorUtil<PointKey> createValueCreatorUtil() {
        return new ValueCreatorUtil<>(INDEX_DESCRIPTOR, SUPPORTED_TYPES, FRACTION_DUPLICATE_NON_UNIQUE);
    }

    @Override
    IndexAccessor createAccessor(PageCache pageCache, IndexDescriptor indexDescriptor) {
        RecoveryCleanupWorkCollector cleanup = RecoveryCleanupWorkCollector.immediate();
        DatabaseIndexContext context = DatabaseIndexContext.builder(
                        pageCache, fs, contextFactory, pageCacheTracer, DEFAULT_DATABASE_NAME)
                .withReadOnlyChecker(writable())
                .build();
        return new PointIndexAccessor(
                context,
                createIndexFiles(fs, directory, indexDescriptor),
                layout,
                cleanup,
                indexDescriptor,
                SPACE_FILLING_CURVE_SETTINGS,
                CONFIGURATION,
                Sets.immutable.empty(),
                false,
                logProvider,
                SchemaUserDescription.TOKEN_ID_NAME_LOOKUP);
    }

    @Override
    IndexDescriptor indexDescriptor() {
        return INDEX_DESCRIPTOR;
    }

    @Override
    PointLayout layout() {
        return LAYOUT;
    }

    @ParameterizedTest
    @MethodSource("unsupportedPredicates")
    void readerShouldThrowOnUnsupportedPredicates(PropertyIndexQuery predicate) {
        try (ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING)) {
            IndexNotApplicableKernelException e = assertThrows(
                    IndexNotApplicableKernelException.class,
                    () -> {
                        try (SimpleEntityValueClient client = new SimpleEntityValueClient()) {
                            reader.query(
                                    client, NULL_CONTEXT, CursorContext.NULL_CONTEXT, unorderedValues(), predicate);
                        }
                    },
                    "%s is an unsupported query".formatted(predicate));
            assertThat(e)
                    .hasMessageContaining(
                            "Tried to query index with illegal query. Only %s, %s, and %s queries are supported by a point index",
                            IndexQueryType.ALL_ENTRIES, IndexQueryType.EXACT, IndexQueryType.BOUNDING_BOX);
            assertThat(e.gqlStatus()).isEqualTo("50N15");
            assertThat(e.statusDescription())
                    .isEqualTo(String.format(
                            "error: general processing exception - unsupported index operation. The system attempted to execute an unsupported operation on index `%s`. See debug.log for more information.",
                            INDEX_DESCRIPTOR.getName()));
            LogAssertions.assertThat(logProvider)
                    .containsMessageWithAll(
                            "Tried to query index with illegal query. Only %s, %s, and %s queries are supported by a point index"
                                    .formatted(
                                            IndexQueryType.ALL_ENTRIES,
                                            IndexQueryType.EXACT,
                                            IndexQueryType.BOUNDING_BOX))
                    .containsException(e);
        }
    }

    @Test
    void readerShouldThrowOnUnsupportedCompositePredicates() {
        try (ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING)) {
            IndexNotApplicableKernelException e = assertThrows(
                    IndexNotApplicableKernelException.class,
                    () -> reader.query(
                            new SimpleEntityValueClient(),
                            NULL_CONTEXT,
                            CursorContext.NULL_CONTEXT,
                            unorderedValues(),
                            PropertyIndexQuery.exact(0, Values.stringValue("myValue")),
                            PropertyIndexQuery.exact(1, Values.stringValue("myValue"))));
            assertThat(e)
                    .hasMessageContaining(
                            "Tried to query a point index with a composite query. Composite queries are not supported by a point index. Query was");
            assertThat(e.gqlStatus()).isEqualTo("50N15");
            assertThat(e.statusDescription())
                    .isEqualTo(String.format(
                            "error: general processing exception - unsupported index operation. The system attempted to execute an unsupported operation on index `%s`. See debug.log for more information.",
                            INDEX_DESCRIPTOR.getName()));
            LogAssertions.assertThat(logProvider)
                    .containsMessageWithAll(
                            "Tried to query a point index with a composite query. Composite queries are not supported by a point index. Query was")
                    .containsException(e);
        }
    }

    @ParameterizedTest
    @MethodSource("unsupportedOrders")
    void readerShouldThrowOnUnsupportedOrder(IndexOrder indexOrder) {
        try (ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING)) {
            PropertyIndexQuery.ExactPredicate query = PropertyIndexQuery.exact(0, PointValue.MAX_VALUE);
            IndexNotApplicableKernelException e = assertThrows(
                    IndexNotApplicableKernelException.class,
                    () -> {
                        try (SimpleEntityValueClient client = new SimpleEntityValueClient()) {
                            reader.query(
                                    client,
                                    NULL_CONTEXT,
                                    CursorContext.NULL_CONTEXT,
                                    constrained(indexOrder, false),
                                    query);
                        }
                    },
                    "order is not supported with point index");
            assertThat(e)
                    .hasMessageContainingAll(
                            "Tried to query a point index with order. Order is not supported by a point index.");
            assertThat(e.gqlStatus()).isEqualTo("50N15");
            assertThat(e.statusDescription())
                    .isEqualTo(String.format(
                            "error: general processing exception - unsupported index operation. The system attempted to execute an unsupported operation on index `%s`. See debug.log for more information.",
                            INDEX_DESCRIPTOR.getName()));
            LogAssertions.assertThat(logProvider)
                    .containsMessageWithAll(
                            "Tried to query a point index with order. Order is not supported by a point index.")
                    .containsException(e);
        }
    }

    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    void updaterShouldIgnoreUnsupportedTypes(ValueType unsupportedType) throws Exception {
        // given  an empty index
        // when   an unsupported value type is added
        try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
            Value unsupportedValue = random.randomValues().nextValueOfType(unsupportedType);
            updater.process(
                    EagerValueIndexEntryUpdate.add(idGenerator().getAsLong(), INDEX_DESCRIPTOR, unsupportedValue));
        }

        // then   it should not be indexed, and thus not visible
        try (BoundedIterable<Long> reader = accessor.newAllEntriesValueReader(CursorContext.NULL_CONTEXT)) {
            assertThat(reader).isEmpty();
        }
    }

    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    void updaterShouldChangeUnsupportedToSupportedByAdd(ValueType unsupportedType) throws Exception {
        // given  an empty index
        // when   an unsupported value type is added
        long entityId = idGenerator().getAsLong();
        Value unsupportedValue = random.randomValues().nextValueOfType(unsupportedType);
        try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
            updater.process(EagerValueIndexEntryUpdate.add(entityId, INDEX_DESCRIPTOR, unsupportedValue));
        }

        // then   it should not be indexed, and thus not visible
        try (BoundedIterable<Long> reader = accessor.newAllEntriesValueReader(CursorContext.NULL_CONTEXT)) {
            assertThat(reader).isEmpty();
        }

        // when   the unsupported value type is changed to a supported value type
        try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
            Value supportedValue = random.randomValues().nextValueOfTypes(SUPPORTED_TYPES);
            updater.process(
                    EagerValueIndexEntryUpdate.change(entityId, INDEX_DESCRIPTOR, unsupportedValue, supportedValue));
        }

        // then   it should be added to the index, and thus now visible
        try (BoundedIterable<Long> reader = accessor.newAllEntriesValueReader(CursorContext.NULL_CONTEXT)) {
            assertThat(reader).containsExactlyInAnyOrder(entityId);
        }
    }

    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    void updaterShouldChangeSupportedToUnsupportedByRemove(ValueType unsupportedType) throws Exception {
        // given  an empty index
        // when   a supported value type is added
        long entityId = idGenerator().getAsLong();
        Value supportedValue = random.randomValues().nextValueOfTypes(SUPPORTED_TYPES);
        try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
            updater.process(EagerValueIndexEntryUpdate.add(entityId, INDEX_DESCRIPTOR, supportedValue));
        }

        // then   it should be added to the index, and thus visible
        try (BoundedIterable<Long> reader = accessor.newAllEntriesValueReader(CursorContext.NULL_CONTEXT)) {
            assertThat(reader).containsExactlyInAnyOrder(entityId);
        }

        // when   the supported value type is changed to an unsupported value type
        try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
            Value unsupportedValue = random.randomValues().nextValueOfType(unsupportedType);
            updater.process(
                    EagerValueIndexEntryUpdate.change(entityId, INDEX_DESCRIPTOR, supportedValue, unsupportedValue));
        }

        // then   it should be removed from the index, and thus no longer visible
        try (BoundedIterable<Long> reader = accessor.newAllEntriesValueReader(CursorContext.NULL_CONTEXT)) {
            assertThat(reader).isEmpty();
        }
    }

    @Disabled("Point indexes compare points differently than just mere values, don't they")
    @Override
    void shouldSeeAllEntriesBetweenSpecificValues(boolean fromBeginning, boolean toEnd) {}

    private static LongSupplier idGenerator() {
        return new AtomicLong(0)::incrementAndGet;
    }

    private static Stream<PropertyIndexQuery> unsupportedPredicates() {
        return Stream.of(
                PropertyIndexQuery.exists(0),
                PropertyIndexQuery.stringPrefix(0, Values.stringValue("myValue")),
                PropertyIndexQuery.stringSuffix(0, Values.stringValue("myValue")),
                PropertyIndexQuery.stringContains(0, Values.stringValue("myValue")),
                PropertyIndexQuery.fulltextSearch("myValue"));
    }

    private static Stream<IndexOrder> unsupportedOrders() {
        return Stream.of(IndexOrder.DESCENDING, IndexOrder.ASCENDING);
    }

    private static Stream<ValueType> unsupportedTypes() {
        return Arrays.stream(UNSUPPORTED_TYPES);
    }
}
