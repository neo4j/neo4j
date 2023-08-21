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
package org.neo4j.kernel.api.impl.schema;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterators.loop;
import static org.neo4j.kernel.api.impl.schema.DatabaseFunctions.awaitIndexesOnline;
import static org.neo4j.kernel.api.impl.schema.DatabaseFunctions.index;
import static org.neo4j.kernel.api.impl.schema.DatabaseFunctions.uniquenessConstraint;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
public class UniqueIndexApplicationIT {
    @Inject
    private GraphDatabaseAPI db;

    private static Stream<Function<Transaction, Void>> indexTypes() {
        return Stream.of(index(label("Label1"), "key1"), uniquenessConstraint(label("Label1"), "key1"));
    }

    @AfterEach
    void then() {
        try (var transaction = db.beginTx()) {
            assertThat(listNodeIdsFromIndexLookup(transaction, label("Label1"), "key1", "value1")
                            .apply(db))
                    .as("Matching nodes from index lookup")
                    .hasSize(1);
        }
    }

    private void createIndex(Function<Transaction, Void> createIndexFunc) {
        try (Transaction tx = db.beginTx()) {
            createIndexFunc.apply(tx);
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            awaitIndexesOnline(5, SECONDS).apply(tx);
            tx.commit();
        }
    }

    @ParameterizedTest
    @MethodSource("indexTypes")
    void tx_createNode_addLabel_setProperty(Function<Transaction, Void> createIndexFunc) {
        createIndex(createIndexFunc);

        try (var transaction = db.beginTx()) {
            var node = transaction.createNode();
            node.addLabel(label("Label1"));
            node.setProperty("key1", "value1");
            transaction.commit();
        }
    }

    @ParameterizedTest
    @MethodSource("indexTypes")
    void tx_createNode_tx_addLabel_setProperty(Function<Transaction, Void> createIndexFunc) {
        createIndex(createIndexFunc);

        try (var transaction = db.beginTx()) {
            var node = transaction.createNode();
            node.addLabel(label("Label1"));
            node.setProperty("key1", "value1");
            transaction.commit();
        }
    }

    @ParameterizedTest
    @MethodSource("indexTypes")
    void tx_createNode_addLabel_tx_setProperty(Function<Transaction, Void> createIndexFunc) {
        createIndex(createIndexFunc);

        Node node;
        try (var transaction = db.beginTx()) {
            node = transaction.createNode();
            node.addLabel(label("Label1"));
            transaction.commit();
        }
        try (var transaction = db.beginTx()) {
            transaction.getNodeById(node.getId()).setProperty("key1", "value1");
            transaction.commit();
        }
    }

    @ParameterizedTest
    @MethodSource("indexTypes")
    void tx_createNode_setProperty_tx_addLabel(Function<Transaction, Void> createIndexFunc) {
        createIndex(createIndexFunc);

        Node node;
        try (var transaction = db.beginTx()) {
            node = transaction.createNode();
            node.addLabel(label("Label1"));
            node.setProperty("key1", "value1");
            transaction.commit();
        }
        try (var transaction = db.beginTx()) {
            transaction.getNodeById(node.getId()).addLabel(label("Label1"));
            transaction.commit();
        }
    }

    @ParameterizedTest
    @MethodSource("indexTypes")
    void tx_createNode_tx_addLabel_tx_setProperty(Function<Transaction, Void> createIndexFunc) {
        createIndex(createIndexFunc);

        Node node;
        try (var transaction = db.beginTx()) {
            node = transaction.createNode();
            transaction.commit();
        }
        try (var transaction = db.beginTx()) {
            transaction.getNodeById(node.getId()).addLabel(label("Label1"));
            transaction.commit();
        }
        try (var transaction = db.beginTx()) {
            transaction.getNodeById(node.getId()).setProperty("key1", "value1");
            transaction.commit();
        }
    }

    @ParameterizedTest
    @MethodSource("indexTypes")
    void tx_createNode_tx_setProperty_tx_addLabel(Function<Transaction, Void> createIndexFunc) {
        createIndex(createIndexFunc);

        try (var transaction = db.beginTx()) {
            var node = transaction.createNode();
            node.setProperty("key1", "value1");
            node.addLabel(label("Label1"));
            transaction.commit();
        }
    }

    private static Function<GraphDatabaseService, List<Long>> listNodeIdsFromIndexLookup(
            Transaction tx, final Label label, final String propertyKey, final Object value) {
        return graphDb -> {
            List<Long> ids = new ArrayList<>();
            try (ResourceIterator<Node> nodes = tx.findNodes(label, propertyKey, value)) {
                for (Node node : loop(nodes)) {
                    ids.add(node.getId());
                }
            }
            return ids;
        };
    }
}
