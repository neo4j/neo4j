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
package org.neo4j.kernel.impl.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.collection.Iterators.count;

import java.util.Iterator;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class NodeManagerTest {
    @Inject
    GraphDatabaseService db;

    @Test
    void getAllNodesIteratorShouldPickUpHigherIdsThanHighIdWhenStarted() throws Exception {
        // GIVEN
        try (var tx = db.beginTx()) {
            tx.createNode();
            tx.createNode();
            tx.commit();
        }

        // WHEN iterator is started
        try (var tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            Iterator<Node> nodeIterator = allNodes.iterator();
            nodeIterator.next();

            // and WHEN another node is then added
            Thread thread = new Thread(() -> {
                Transaction newTx = db.beginTx();
                newTx.createNode();
                newTx.commit();
            });
            thread.start();
            thread.join();

            // THEN the new node is picked up by the iterator
            assertThat(count(nodeIterator)).isEqualTo(2);
        }
    }

    @Test
    void getAllRelationshipsIteratorShouldPickUpHigherIdsThanHighIdWhenStarted() throws Exception {
        // GIVEN
        try (var tx = db.beginTx()) {
            createRelationshipAssumingTxWith(tx, "key", 1);
            createRelationshipAssumingTxWith(tx, "key", 2);
            tx.commit();
        }

        // WHEN
        try (var tx = db.beginTx();
                ResourceIterable<Relationship> allRelationships = tx.getAllRelationships();
                ResourceIterator<Relationship> relationships = allRelationships.iterator()) {
            relationships.next();

            Thread thread = new Thread(() -> {
                Transaction newTx = db.beginTx();
                createRelationshipAssumingTxWith(newTx, "key", 3);
                newTx.commit();
            });
            thread.start();
            thread.join();

            // THEN
            assertThat(count(relationships)).isEqualTo(2);
        }
    }

    private static void createRelationshipAssumingTxWith(Transaction transaction, String key, Object value) {
        Node a = transaction.createNode();
        Node b = transaction.createNode();
        Relationship relationship = a.createRelationshipTo(b, RelationshipType.withName("FOO"));
        relationship.setProperty(key, value);
    }
}
