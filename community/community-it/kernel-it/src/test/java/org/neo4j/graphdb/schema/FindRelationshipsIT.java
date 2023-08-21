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
package org.neo4j.graphdb.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
public class FindRelationshipsIT {
    private static final RelationshipType REL_TYPE = RelationshipType.withName("REL_TYPE");
    private static final RelationshipType OTHER_REL_TYPE = RelationshipType.withName("OTHER_REL_TYPE");

    @Inject
    GraphDatabaseService db;

    private static Stream<Arguments> indexConfiguration() {
        return Stream.of(Arguments.of("with token indexes", false), Arguments.of("without token indexes", true));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexConfiguration")
    void findRelationshipsWhenTypeNotExistShouldGiveEmptyIterator(String name, boolean removeTokenIndex) {
        prepareIndexSetup(removeTokenIndex);

        try (Transaction tx = db.beginTx()) {
            tx.createNode().createRelationshipTo(tx.createNode(), OTHER_REL_TYPE);
            tx.createNode().createRelationshipTo(tx.createNode(), OTHER_REL_TYPE);
            tx.commit();
        }

        try (Transaction tx = db.beginTx();
                ResourceIterator<Relationship> relationships = tx.findRelationships(REL_TYPE)) {
            assertFalse(relationships.hasNext());
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexConfiguration")
    void findRelationshipsShouldGiveAllRelationshipsOfType(String name, boolean removeTokenIndex) {
        prepareIndexSetup(removeTokenIndex);

        Relationship rel1;
        Relationship rel2;
        try (Transaction tx = db.beginTx()) {
            tx.createNode().createRelationshipTo(tx.createNode(), OTHER_REL_TYPE);
            rel1 = tx.createNode().createRelationshipTo(tx.createNode(), REL_TYPE);
            tx.createNode().createRelationshipTo(tx.createNode(), OTHER_REL_TYPE);
            rel2 = tx.createNode().createRelationshipTo(tx.createNode(), REL_TYPE);
            tx.commit();
        }

        List<Relationship> result;
        try (Transaction tx = db.beginTx()) {
            result = Iterators.asList(tx.findRelationships(REL_TYPE));
        }
        assertThat(result).containsExactlyInAnyOrder(rel1, rel2);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexConfiguration")
    void findRelationshipsShouldIncludeChangesInTx(String name, boolean removeTokenIndex) {
        prepareIndexSetup(removeTokenIndex);

        Relationship rel1;
        Relationship rel2;
        Relationship rel3;
        Label label = Label.label("label");
        try (Transaction tx = db.beginTx()) {
            tx.createNode().createRelationshipTo(tx.createNode(), OTHER_REL_TYPE);

            // Two relationships we will delete
            Node node = tx.createNode(label);
            node.setProperty("key", "value");
            node.createRelationshipTo(tx.createNode(), REL_TYPE);
            node.createRelationshipTo(tx.createNode(), REL_TYPE);

            tx.createNode().createRelationshipTo(tx.createNode(), OTHER_REL_TYPE);
            rel1 = tx.createNode().createRelationshipTo(tx.createNode(), REL_TYPE);
            tx.commit();
        }

        List<Relationship> result;
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            Node node2 = tx.createNode();
            rel2 = node.createRelationshipTo(node2, REL_TYPE);
            tx.createNode().createRelationshipTo(tx.createNode(), OTHER_REL_TYPE);
            rel3 = node2.createRelationshipTo(node, REL_TYPE);

            Iterables.forEach(tx.findNode(label, "key", "value").getRelationships(), Relationship::delete);

            result = Iterators.asList(tx.findRelationships(REL_TYPE));
        }
        assertThat(result).containsExactlyInAnyOrder(rel1, rel2, rel3);
    }

    private void prepareIndexSetup(boolean removeTokenIndex) {
        if (removeTokenIndex) {
            try (Transaction tx = db.beginTx()) {
                // Drop the default indexes to be able to test fallback to store scan
                tx.schema().getIndexes().forEach(IndexDefinition::drop);
                tx.commit();
            }
        }
    }
}
