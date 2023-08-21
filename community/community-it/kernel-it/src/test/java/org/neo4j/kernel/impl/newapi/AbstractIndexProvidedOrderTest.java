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
package org.neo4j.kernel.impl.newapi;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.constrained;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.values.storable.ValueTuple.COMPARATOR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.ValueType;

@SuppressWarnings("FieldCanBeLocal")
@ExtendWith(RandomExtension.class)
public abstract class AbstractIndexProvidedOrderTest extends KernelAPIReadTestBase<ReadTestSupport> {
    private static final int N_ENTITIES = 10000;
    private static final int N_ITERATIONS = 100;
    private static final String TOKEN = "Node";
    private static final String PROPERTY_KEY = "prop";
    private static final String INDEX_NAME = "propIndex";
    private static final ValueType[] ALL_ORDERABLE = RandomValues.excluding(
            ValueType.STRING,
            ValueType.STRING_ARRAY,
            ValueType.GEOGRAPHIC_POINT,
            ValueType.GEOGRAPHIC_POINT_ARRAY,
            ValueType.GEOGRAPHIC_POINT_3D,
            ValueType.GEOGRAPHIC_POINT_3D_ARRAY,
            ValueType.CARTESIAN_POINT,
            ValueType.CARTESIAN_POINT_ARRAY,
            ValueType.CARTESIAN_POINT_3D,
            ValueType.CARTESIAN_POINT_3D_ARRAY,
            ValueType.DURATION,
            ValueType.DURATION_ARRAY,
            ValueType.PERIOD,
            ValueType.PERIOD_ARRAY);

    @Inject
    RandomSupport randomRule;

    private TreeSet<EntityValueTuple> singlePropValues = new TreeSet<>(COMPARATOR);
    private ValueType[] targetedTypes;

    @Override
    public ReadTestSupport newTestSupport() {
        return new ReadTestSupport();
    }

    abstract EntityControl getEntityControl();

    @Override
    public void createTestGraph(GraphDatabaseService graphDb) {
        try (Transaction tx = graphDb.beginTx()) {
            getEntityControl().createIndex(tx, TOKEN, PROPERTY_KEY, INDEX_NAME);
            tx.commit();
        }
        try (Transaction tx = graphDb.beginTx()) {
            tx.schema().awaitIndexesOnline(5, MINUTES);
            tx.commit();
        }

        RandomValues randomValues = randomRule.randomValues();

        targetedTypes = randomValues.selection(ALL_ORDERABLE, 1, ALL_ORDERABLE.length, false);
        targetedTypes = ensureHighEnoughCardinality(targetedTypes);
        try (Transaction tx = graphDb.beginTx()) {
            for (int i = 0; i < N_ENTITIES; i++) {
                var node = getEntityControl().createEntity(tx, TOKEN);
                Value propValue;
                EntityValueTuple singleValue;
                do {
                    propValue = randomValues.nextValueOfTypes(targetedTypes);
                    singleValue = new EntityValueTuple(node.getId(), propValue);
                } while (singlePropValues.contains(singleValue));
                singlePropValues.add(singleValue);

                node.setProperty(PROPERTY_KEY, propValue.asObject());
            }
            tx.commit();
        }
    }

    @Test
    void shouldProvideResultInOrderIfCapable() throws KernelException {
        int prop = token.propertyKey(PROPERTY_KEY);

        RandomValues randomValues = randomRule.randomValues();
        IndexReadSession index = read.indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));
        for (int i = 0; i < N_ITERATIONS; i++) {
            ValueType type = randomValues.among(targetedTypes);

            expectResultInOrder(randomValues, type, prop, index, IndexOrder.ASCENDING);

            expectResultInOrder(randomValues, type, prop, index, IndexOrder.DESCENDING);
        }
    }

    private void expectResultInOrder(
            RandomValues randomValues, ValueType type, int prop, IndexReadSession index, IndexOrder indexOrder)
            throws KernelException {
        EntityValueTuple from = new EntityValueTuple(Long.MIN_VALUE, randomValues.nextValueOfType(type));
        EntityValueTuple to = new EntityValueTuple(Long.MAX_VALUE, randomValues.nextValueOfType(type));
        if (COMPARATOR.compare(from, to) > 0) {
            EntityValueTuple tmp = from;
            from = to;
            to = tmp;
        }
        boolean fromInclusive = randomValues.nextBoolean();
        boolean toInclusive = randomValues.nextBoolean();
        PropertyIndexQuery.RangePredicate<?> range =
                PropertyIndexQuery.range(prop, from.getOnlyValue(), fromInclusive, to.getOnlyValue(), toInclusive);
        List<Long> expectedIdsInOrder = expectedIdsInOrder(from, fromInclusive, to, toInclusive, indexOrder);

        var actualIdsInOrder = getEntityControl().findEntities(tx, cursors, index, indexOrder, range);
        assertThat(actualIdsInOrder)
                .as("actual node ids not in same order as expected for value type " + type)
                .containsExactlyElementsOf(expectedIdsInOrder);
    }

    private List<Long> expectedIdsInOrder(
            EntityValueTuple from,
            boolean fromInclusive,
            EntityValueTuple to,
            boolean toInclusive,
            IndexOrder indexOrder) {
        List<Long> expectedIdsInOrder = singlePropValues.subSet(from, fromInclusive, to, toInclusive).stream()
                .map(EntityValueTuple::nodeId)
                .collect(Collectors.toList());
        if (indexOrder == IndexOrder.DESCENDING) {
            Collections.reverse(expectedIdsInOrder);
        }
        return expectedIdsInOrder;
    }

    /**
     * If targetedTypes only contain types that has very low cardinality, then add one random high cardinality value type to the array.
     * This is to prevent createTestGraph from looping forever when trying to generate unique values.
     */
    private ValueType[] ensureHighEnoughCardinality(ValueType[] targetedTypes) {
        ValueType[] lowCardinalityArray = new ValueType[] {ValueType.BOOLEAN, ValueType.BYTE, ValueType.BOOLEAN_ARRAY};
        List<ValueType> typesOfLowCardinality = new ArrayList<>(Arrays.asList(lowCardinalityArray));
        for (ValueType targetedType : targetedTypes) {
            if (!typesOfLowCardinality.contains(targetedType)) {
                return targetedTypes;
            }
        }
        List<ValueType> result = new ArrayList<>(Arrays.asList(targetedTypes));
        ValueType highCardinalityType =
                randomRule.randomValues().among(RandomValues.excluding(ALL_ORDERABLE, lowCardinalityArray));
        result.add(highCardinalityType);
        return result.toArray(new ValueType[0]);
    }

    private static class EntityValueTuple extends ValueTuple {
        private final long nodeId;

        private EntityValueTuple(long nodeId, Value... values) {
            super(values);
            this.nodeId = nodeId;
        }

        long nodeId() {
            return nodeId;
        }
    }

    enum EntityControl {
        NODE {
            @Override
            public void createIndex(Transaction tx, String token, String propertyKey, String indexName) {
                tx.schema()
                        .indexFor(label(token))
                        .on(propertyKey)
                        .withName(indexName)
                        .create();
            }

            @Override
            public List<Long> findEntities(
                    KernelTransaction tx,
                    CursorFactory cursors,
                    IndexReadSession index,
                    IndexOrder indexOrder,
                    PropertyIndexQuery.RangePredicate<?> range)
                    throws KernelException {
                try (var cursor = cursors.allocateNodeValueIndexCursor(NULL_CONTEXT, tx.memoryTracker())) {
                    tx.dataRead()
                            .nodeIndexSeek(tx.queryContext(), index, cursor, constrained(indexOrder, false), range);

                    List<Long> actualIdsInOrder = new ArrayList<>();
                    while (cursor.next()) {
                        actualIdsInOrder.add(cursor.nodeReference());
                    }

                    return actualIdsInOrder;
                }
            }

            @Override
            public Entity createEntity(Transaction tx, String token) {
                return tx.createNode(label(token));
            }
        },

        RELATIONSHIP {
            @Override
            public void createIndex(Transaction tx, String token, String propertyKey, String indexName) {
                tx.schema()
                        .indexFor(RelationshipType.withName(token))
                        .on(propertyKey)
                        .withName(indexName)
                        .create();
            }

            @Override
            public List<Long> findEntities(
                    KernelTransaction tx,
                    CursorFactory cursors,
                    IndexReadSession index,
                    IndexOrder indexOrder,
                    PropertyIndexQuery.RangePredicate<?> range)
                    throws KernelException {
                try (var cursor = cursors.allocateRelationshipValueIndexCursor(NULL_CONTEXT, tx.memoryTracker())) {
                    tx.dataRead()
                            .relationshipIndexSeek(
                                    tx.queryContext(), index, cursor, constrained(indexOrder, false), range);

                    List<Long> actualIdsInOrder = new ArrayList<>();
                    while (cursor.next()) {
                        actualIdsInOrder.add(cursor.relationshipReference());
                    }

                    return actualIdsInOrder;
                }
            }

            @Override
            public Entity createEntity(Transaction tx, String token) {
                return tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName(token));
            }
        };

        abstract void createIndex(Transaction tx, String token, String propertyKey, String indexName);

        abstract List<Long> findEntities(
                KernelTransaction tx,
                CursorFactory cursors,
                IndexReadSession index,
                IndexOrder indexOrder,
                PropertyIndexQuery.RangePredicate<?> range)
                throws KernelException;

        abstract Entity createEntity(Transaction tx, String token);
    }
}
