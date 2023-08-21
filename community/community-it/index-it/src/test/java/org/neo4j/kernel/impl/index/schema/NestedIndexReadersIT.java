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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThread;
import org.neo4j.test.extension.OtherThreadExtension;

@ImpermanentDbmsExtension
@ExtendWith(OtherThreadExtension.class)
public class NestedIndexReadersIT {
    private static final int ENTITIES_PER_ID = 3;
    private static final int IDS = 5;
    private static final String TOKEN = "Token";
    private static final String KEY = "key";

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private OtherThread t2;

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldReadCorrectResultsFromMultipleNestedReaders(EntityControl<?> entityControl) {
        // given
        createIndex(entityControl);
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < ENTITIES_PER_ID; i++) {
                createRoundOfEntities(tx, entityControl);
            }
            tx.commit();
        }

        // when
        try (Transaction tx = db.beginTx()) {
            // opening all the index readers
            List<ResourceIterator<?>> iterators = new ArrayList<>();
            for (int id = 0; id < IDS; id++) {
                iterators.add(entityControl.findEntities(tx, TOKEN, KEY, id));
            }

            // then iterating over them interleaved should yield all the expected results each
            for (int i = 0; i < ENTITIES_PER_ID; i++) {
                assertRoundOfEntities(iterators, entityControl);
            }

            for (ResourceIterator<?> reader : iterators) {
                assertFalse(reader.hasNext());
                reader.close();
            }

            tx.commit();
        }
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldReadCorrectResultsFromMultipleNestedReadersWhenConcurrentWriteHappens(EntityControl<?> entityControl)
            throws Exception {
        // given
        createIndex(entityControl);
        try (Transaction tx = db.beginTx()) {
            for (int id = 0; id < IDS; id++) {
                for (int i = 0; i < ENTITIES_PER_ID; i++) {
                    entityControl.createEntity(tx, TOKEN, KEY, id);
                }
            }
            tx.commit();
        }

        // when
        try (Transaction tx = db.beginTx()) {
            // opening all the index readers
            List<ResourceIterator<?>> iterators = new ArrayList<>();
            for (int id = 0; id < IDS; id++) {
                iterators.add(entityControl.findEntities(tx, TOKEN, KEY, id));
            }

            // then iterating over them interleaved should yield all the expected results each
            for (int i = 0; i < ENTITIES_PER_ID; i++) {
                assertRoundOfEntities(iterators, entityControl);

                if (i % 2 == 1) {
                    // will be triggered on i == 1
                    t2.execute(entityCreator(entityControl)).get();
                }
            }

            assertRoundOfEntities(iterators, entityControl);

            for (ResourceIterator<?> reader : iterators) {
                assertFalse(reader.hasNext());
                reader.close();
            }

            tx.commit();
        }
    }

    private static void createRoundOfEntities(Transaction tx, EntityControl<?> entityControl) {
        for (int id = 0; id < IDS; id++) {
            entityControl.createEntity(tx, TOKEN, KEY, id);
        }
    }

    private static void assertRoundOfEntities(List<ResourceIterator<?>> iterators, EntityControl<?> entityControl) {
        for (int id = 0; id < IDS; id++) {
            entityControl.assertEntity(iterators.get(id), TOKEN, KEY, id);
        }
    }

    private Callable<Void> entityCreator(EntityControl<?> entityControl) {
        return () -> {
            try (Transaction tx = db.beginTx()) {
                createRoundOfEntities(tx, entityControl);
                tx.commit();
            }
            return null;
        };
    }

    private void createIndex(EntityControl<?> entityControl) {
        try (Transaction tx = db.beginTx()) {
            entityControl.createIndex(tx, TOKEN, KEY);
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(30, SECONDS);
            tx.commit();
        }
    }

    interface EntityControl<T extends Entity> {
        ResourceIterator<T> findEntities(Transaction tx, String token, String key, Object value);

        void assertEntity(ResourceIterator<?> resourceIterator, String token, String key, Object value);

        void createEntity(Transaction tx, String token, String key, Object value);

        void createIndex(Transaction tx, String token, String key);
    }

    static Stream<Arguments> parameters() {
        return Stream.of(
                arguments(new EntityControl<Node>() {
                    @Override
                    public ResourceIterator<Node> findEntities(Transaction tx, String token, String key, Object value) {
                        return tx.findNodes(Label.label(token), KEY, value);
                    }

                    @Override
                    public void assertEntity(ResourceIterator<?> reader, String token, String key, Object value) {
                        assertTrue(reader.hasNext());
                        Node node = (Node) reader.next();
                        assertTrue(node.hasLabel(Label.label(token)));
                        assertEquals(
                                value,
                                node.getProperty(key),
                                "Expected node " + node + " (returned by index reader) to have 'id' property w/ value "
                                        + value);
                    }

                    @Override
                    public void createEntity(Transaction tx, String token, String key, Object value) {
                        tx.createNode(Label.label(token)).setProperty(key, value);
                    }

                    @Override
                    public void createIndex(Transaction tx, String token, String key) {
                        tx.schema().indexFor(Label.label(token)).on(key).create();
                    }

                    @Override
                    public String toString() {
                        return "NODE";
                    }
                }),
                arguments(new EntityControl<Relationship>() {
                    @Override
                    public ResourceIterator<Relationship> findEntities(
                            Transaction tx, String token, String key, Object value) {
                        return tx.findRelationships(RelationshipType.withName(token), KEY, value);
                    }

                    @Override
                    public void assertEntity(ResourceIterator<?> reader, String token, String key, Object value) {
                        assertTrue(reader.hasNext());
                        Relationship rel = (Relationship) reader.next();
                        Assertions.assertThat(rel.getType()).isEqualTo(RelationshipType.withName(token));
                        assertEquals(
                                value,
                                rel.getProperty(key),
                                "Expected rel " + rel + " (returned by index reader) to have 'id' property w/ value "
                                        + value);
                    }

                    @Override
                    public void createEntity(Transaction tx, String token, String key, Object value) {
                        var from = tx.createNode();
                        var to = tx.createNode();
                        from.createRelationshipTo(to, RelationshipType.withName(token))
                                .setProperty(key, value);
                    }

                    @Override
                    public void createIndex(Transaction tx, String token, String key) {
                        tx.schema()
                                .indexFor(RelationshipType.withName(token))
                                .on(key)
                                .create();
                    }

                    @Override
                    public String toString() {
                        return "RELATIONSHIP";
                    }
                }));
    }
}
