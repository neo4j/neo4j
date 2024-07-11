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
package org.neo4j.kernel.api.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.constrained;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracking.NO_USAGE_TRACKING;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.annotations.documented.ReporterFactories;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.storageengine.api.schema.SimpleEntityValueClient;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

abstract class IndexAccessorCompatibility extends PropertyIndexProviderCompatibilityTestSuite.Compatibility {
    IndexAccessor accessor;
    // This map is for spatial values, so that the #query method can lookup the values for the results and filter
    // properly
    private final Map<Long, Value[]> committedValues = new HashMap<>();

    IndexAccessorCompatibility(PropertyIndexProviderCompatibilityTestSuite testSuite, IndexPrototype prototype) {
        super(testSuite, prototype);
    }

    @BeforeEach
    void before() throws Exception {
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig(config);
        IndexPopulator populator = indexProvider.getPopulator(
                descriptor,
                indexSamplingConfig,
                heapBufferFactory(1024),
                INSTANCE,
                tokenNameLookup,
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY);
        populator.create();
        populator.close(true, CursorContext.NULL_CONTEXT);
        accessor = indexProvider.getOnlineAccessor(
                descriptor,
                indexSamplingConfig,
                tokenNameLookup,
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY);
    }

    @AfterEach
    void after() {
        try {
            boolean consistent = accessor.consistencyCheck(
                    ReporterFactories.throwingReporterFactory(),
                    NULL_CONTEXT_FACTORY,
                    Runtime.getRuntime().availableProcessors());
            assertThat(consistent).isTrue();
        } finally {
            accessor.drop();
            accessor.close();
        }
    }

    ValueType[] randomSetOfSupportedTypes() {
        ValueType[] supportedTypes = testSuite.supportedValueTypes();
        return random.randomValues().selection(supportedTypes, 2, supportedTypes.length, false);
    }

    ValueType[] randomSetOfSupportedAndSortableTypes() {
        ValueType[] types = RandomValues.excluding(testSuite.supportedValueTypes(), type -> switch (type) {
            case STRING, STRING_ARRAY -> true; // exclude strings outside the Basic Multilingual Plane
            default -> switch (type.valueGroup) {
                case GEOMETRY, GEOMETRY_ARRAY, DURATION, DURATION_ARRAY -> true; // exclude spacial types
                default -> false;
            };
        });

        return random.randomValues().selection(types, 2, types.length, false);
    }

    protected List<Long> query(PropertyIndexQuery... predicates) throws Exception {
        var list = queryNoSort(predicates);
        Collections.sort(list);
        return list;
    }

    protected List<Long> queryNoSort(PropertyIndexQuery... predicates) throws Exception {
        try (ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING)) {
            SimpleEntityValueClient nodeValueClient = new SimpleEntityValueClient();
            reader.query(nodeValueClient, QueryContext.NULL_CONTEXT, unconstrained(), predicates);
            List<Long> list = new LinkedList<>();
            while (nodeValueClient.next()) {
                long entityId = nodeValueClient.reference;
                if (passesFilter(entityId, predicates)) {
                    list.add(entityId);
                }
            }
            return list;
        }
    }

    protected AutoCloseable query(SimpleEntityValueClient client, IndexOrder order, PropertyIndexQuery... predicates)
            throws Exception {
        ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING);
        reader.query(client, QueryContext.NULL_CONTEXT, constrained(order, false), predicates);
        return reader;
    }

    List<Long> assertInOrder(IndexOrder order, PropertyIndexQuery... predicates) throws Exception {
        List<Long> actualIds;
        if (order == IndexOrder.NONE) {
            actualIds = query(predicates);
        } else {
            SimpleEntityValueClient client = new SimpleEntityValueClient();
            try (AutoCloseable ignore = query(client, order, predicates)) {
                actualIds = assertClientReturnValuesInOrder(client, order);
            }
        }
        return actualIds;
    }

    static List<Long> assertClientReturnValuesInOrder(SimpleEntityValueClient client, IndexOrder order) {
        List<Long> seenIds = new ArrayList<>();
        Value[] prevValues = null;
        Value[] values;
        int count = 0;
        while (client.next()) {
            count++;
            seenIds.add(client.reference);
            values = client.values;
            if (order == IndexOrder.ASCENDING) {
                assertLessThanOrEqualTo(prevValues, values);
            } else if (order == IndexOrder.DESCENDING) {
                assertLessThanOrEqualTo(values, prevValues);
            } else {
                fail("Unexpected order " + order + " (count = " + count + ")");
            }
            prevValues = values;
        }
        return seenIds;
    }

    private static void assertLessThanOrEqualTo(Value[] o1, Value[] o2) {
        if (o1 == null || o2 == null) {
            return;
        }
        int length = Math.min(o1.length, o2.length);
        for (int i = 0; i < length; i++) {
            int compare = Values.COMPARATOR.compare(o1[i], o2[i]);
            assertThat(compare)
                    .as("expected less than or equal to but was " + Arrays.toString(o1) + " and " + Arrays.toString(o2))
                    .isLessThanOrEqualTo(0);
            if (compare != 0) {
                return;
            }
        }
    }

    /**
     * Run the Value[] from a particular entityId through the list of IndexQuery[] predicates to see if they all accept the value.
     */
    private boolean passesFilter(long entityId, PropertyIndexQuery[] predicates) {
        if (predicates.length == 1
                && EnumSet.of(IndexQueryType.ALL_ENTRIES, IndexQueryType.EXISTS).contains(predicates[0].type())) {
            return true;
        }

        Value[] values = committedValues.get(entityId);
        for (int i = 0; i < values.length; i++) {
            PropertyIndexQuery predicate = predicates[i];
            if (EnumSet.of(ValueGroup.GEOMETRY, ValueGroup.GEOMETRY_ARRAY).contains(predicate.valueGroup())
                    || (predicate.valueGroup() == ValueGroup.NUMBER
                            && !testSuite.supportFullValuePrecisionForNumbers())) {
                if (!predicates[i].acceptsValue(values[i])) {
                    return false;
                }
            }
            // else there's no functional need to let values, other than those of GEOMETRY type, to pass through the
            // IndexQuery filtering
            // avoiding this filtering will have testing be more strict in what index readers returns.
        }
        return true;
    }

    /**
     * Commit these updates to the index. Also store the values, which currently are stored for all types except geometry,
     * so therefore it's done explicitly here so that we can filter on them later.
     */
    void updateAndCommit(Collection<ValueIndexEntryUpdate<?>> updates) throws IndexEntryConflictException {
        try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
            for (ValueIndexEntryUpdate<?> update : updates) {
                updater.process(update);
                switch (update.updateMode()) {
                    case ADDED:
                    case CHANGED:
                        committedValues.put(update.getEntityId(), update.values());
                        break;
                    case REMOVED:
                        committedValues.remove(update.getEntityId());
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown update mode " + update.updateMode());
                }
            }
        }
    }
}
