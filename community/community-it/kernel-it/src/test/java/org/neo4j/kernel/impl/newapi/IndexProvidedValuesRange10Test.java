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
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unorderedValues;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

@ExtendWith(RandomExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class IndexProvidedValuesRange10Test extends KernelAPIReadTestBase<ReadTestSupport> {
    private static final int N_ENTITIES = 10000;
    public static final String TOKEN = "Token";
    public static final String PROP = "prop";
    public static final String PRIP = "prip";
    public static final String PROP_INDEX = "propIndex";
    public static final String PROP_PRIP_INDEX = "propPripIndex";

    @Inject
    private RandomSupport randomRule;

    private List<Value> singlePropValues = new ArrayList<>();
    private List<ValueTuple> doublePropValues = new ArrayList<>();

    @Override
    public ReadTestSupport newTestSupport() {
        return new ReadTestSupport();
    }

    abstract EntityControl getEntityControl();

    @Override
    public void createTestGraph(GraphDatabaseService graphDb) {
        try (Transaction tx = graphDb.beginTx()) {
            getEntityControl().createIndex(tx, TOKEN, PROP, PROP_INDEX);
            getEntityControl().createIndex(tx, TOKEN, PROP, PRIP, PROP_PRIP_INDEX);
            tx.commit();
        }
        try (Transaction tx = graphDb.beginTx()) {
            tx.schema().awaitIndexesOnline(5, MINUTES);
            tx.commit();
        }

        try (Transaction tx = graphDb.beginTx()) {
            RandomValues randomValues = randomRule.randomValues();

            ValueType[] allExceptNonSortable = RandomValues.excluding(ValueType.STRING, ValueType.STRING_ARRAY);

            for (int i = 0; i < N_ENTITIES; i++) {
                var node = getEntityControl().createEntity(tx, TOKEN);
                Value propValue = randomValues.nextValueOfTypes(allExceptNonSortable);
                node.setProperty(PROP, propValue.asObject());
                Value pripValue = randomValues.nextValueOfTypes(allExceptNonSortable);
                node.setProperty(PRIP, pripValue.asObject());

                singlePropValues.add(propValue);
                doublePropValues.add(ValueTuple.of(propValue, pripValue));
            }
            tx.commit();
        }

        singlePropValues.sort(Values.COMPARATOR);
        doublePropValues.sort(ValueTuple.COMPARATOR);
    }

    @Test
    void shouldGetAllSinglePropertyValues() throws Exception {
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX));
        var values = getEntityControl().findValues(tx, cursors, index);

        assertThat(values)
                .as("index should return all single property values")
                .containsExactlyInAnyOrderElementsOf(singlePropValues);
    }

    @Test
    void shouldGetAllDoublePropertyValues() throws Exception {
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_PRIP_INDEX));
        var values = getEntityControl().findValuePairs(tx, cursors, index);
        assertThat(values)
                .as("index should return all double property values")
                .containsExactlyInAnyOrderElementsOf(doublePropValues);
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
            void createIndex(Transaction tx, String token, String propertyKey1, String propertyKey2, String indexName) {
                tx.schema()
                        .indexFor(label(token))
                        .on(propertyKey1)
                        .on(propertyKey2)
                        .withName(indexName)
                        .create();
            }

            @Override
            public List<Value> findValues(KernelTransaction tx, CursorFactory cursors, IndexReadSession index)
                    throws KernelException {
                try (NodeValueIndexCursor cursor =
                        cursors.allocateNodeValueIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
                    tx.dataRead().nodeIndexScan(index, cursor, unorderedValues());

                    var values = new ArrayList<Value>();
                    while (cursor.next()) {
                        values.add(cursor.propertyValue(0));
                    }
                    return values;
                }
            }

            @Override
            public List<ValueTuple> findValuePairs(KernelTransaction tx, CursorFactory cursors, IndexReadSession index)
                    throws KernelException {
                try (NodeValueIndexCursor cursor =
                        cursors.allocateNodeValueIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
                    tx.dataRead().nodeIndexScan(index, cursor, unorderedValues());

                    var values = new ArrayList<ValueTuple>();
                    while (cursor.next()) {
                        values.add(ValueTuple.of(cursor.propertyValue(0), cursor.propertyValue(1)));
                    }
                    return values;
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
            void createIndex(Transaction tx, String token, String propertyKey1, String propertyKey2, String indexName) {
                tx.schema()
                        .indexFor(RelationshipType.withName(token))
                        .on(propertyKey1)
                        .on(propertyKey2)
                        .withName(indexName)
                        .create();
            }

            @Override
            public List<Value> findValues(KernelTransaction tx, CursorFactory cursors, IndexReadSession index)
                    throws KernelException {
                try (var cursor = cursors.allocateRelationshipValueIndexCursor(NULL_CONTEXT, tx.memoryTracker())) {
                    tx.dataRead().relationshipIndexScan(index, cursor, unorderedValues());

                    var values = new ArrayList<Value>();
                    while (cursor.next()) {
                        values.add(cursor.propertyValue(0));
                    }

                    return values;
                }
            }

            @Override
            public List<ValueTuple> findValuePairs(KernelTransaction tx, CursorFactory cursors, IndexReadSession index)
                    throws KernelException {
                try (var cursor = cursors.allocateRelationshipValueIndexCursor(NULL_CONTEXT, tx.memoryTracker())) {
                    tx.dataRead().relationshipIndexScan(index, cursor, unorderedValues());

                    var values = new ArrayList<ValueTuple>();
                    while (cursor.next()) {
                        values.add(ValueTuple.of(cursor.propertyValue(0), cursor.propertyValue(1)));
                    }

                    return values;
                }
            }

            @Override
            public Entity createEntity(Transaction tx, String token) {
                return tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName(token));
            }
        };

        abstract void createIndex(Transaction tx, String token, String propertyKey, String indexName);

        abstract void createIndex(
                Transaction tx, String token, String propertyKey1, String propertyKey2, String indexName);

        abstract List<Value> findValues(KernelTransaction tx, CursorFactory cursors, IndexReadSession index)
                throws KernelException;

        public abstract List<ValueTuple> findValuePairs(
                KernelTransaction tx, CursorFactory cursors, IndexReadSession index) throws KernelException;

        abstract Entity createEntity(Transaction tx, String token);
    }
}
