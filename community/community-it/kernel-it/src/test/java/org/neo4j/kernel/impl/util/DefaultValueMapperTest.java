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
package org.neo4j.kernel.impl.util;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;
import static org.neo4j.values.virtual.VirtualValues.nodeValue;
import static org.neo4j.values.virtual.VirtualValues.pathReference;
import static org.neo4j.values.virtual.VirtualValues.relationshipValue;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

@ImpermanentDbmsExtension
class DefaultValueMapperTest {
    @Inject
    private GraphDatabaseService db;

    @Test
    void shouldHandleSingleNodePath() {
        // Given
        Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode();
            tx.commit();
        }

        // Then
        try (Transaction tx = db.beginTx()) {
            var mapper = new DefaultValueMapper((InternalTransaction) tx);
            Path mapped = mapper.mapPath(pathReference(asNodeValues(node), asRelationshipsValues()));
            assertThat(mapped.length()).isEqualTo(0);
            assertThat(mapped.startNode()).isEqualTo(node);
            assertThat(mapped.endNode()).isEqualTo(node);
            assertThat(Iterables.asList(mapped.relationships())).hasSize(0);
            assertThat(Iterables.asList(mapped.reverseRelationships())).hasSize(0);
            assertThat(Iterables.asList(mapped.nodes())).isEqualTo(singletonList(node));
            assertThat(Iterables.asList(mapped.reverseNodes())).isEqualTo(singletonList(node));
            assertThat(mapped.lastRelationship()).isNull();
            assertThat(Iterators.asList(mapped.iterator())).isEqualTo(singletonList(node));
        }
    }

    @Test
    void shouldHandleSingleRelationshipPath() {
        // Given
        Node start, end;
        Relationship relationship;
        try (Transaction tx = db.beginTx()) {
            start = tx.createNode();
            end = tx.createNode();
            relationship = start.createRelationshipTo(end, RelationshipType.withName("R"));
            tx.commit();
        }

        // Then
        try (Transaction tx = db.beginTx()) {
            var mapper = new DefaultValueMapper((InternalTransaction) tx);
            Path mapped = mapper.mapPath(pathReference(asNodeValues(start, end), asRelationshipsValues(relationship)));
            assertThat(mapped.length()).isEqualTo(1);
            assertThat(mapped.startNode()).isEqualTo(start);
            assertThat(mapped.endNode()).isEqualTo(end);
            assertThat(Iterables.asList(mapped.relationships())).isEqualTo(singletonList(relationship));
            assertThat(Iterables.asList(mapped.reverseRelationships())).isEqualTo(singletonList(relationship));
            assertThat(Iterables.asList(mapped.nodes())).isEqualTo(Arrays.asList(start, end));
            assertThat(Iterables.asList(mapped.reverseNodes())).isEqualTo(Arrays.asList(end, start));
            assertThat(mapped.lastRelationship()).isEqualTo(relationship);
            assertThat(Iterators.asList(mapped.iterator())).isEqualTo(Arrays.asList(start, relationship, end));
        }
    }

    @Test
    void shouldHandleLongPath() {
        // Given
        Node a, b, c, d, e;
        Relationship r1, r2, r3, r4;
        try (Transaction tx = db.beginTx()) {
            a = tx.createNode();
            b = tx.createNode();
            c = tx.createNode();
            d = tx.createNode();
            e = tx.createNode();
            r1 = a.createRelationshipTo(b, RelationshipType.withName("R"));
            r2 = b.createRelationshipTo(c, RelationshipType.withName("R"));
            r3 = c.createRelationshipTo(d, RelationshipType.withName("R"));
            r4 = d.createRelationshipTo(e, RelationshipType.withName("R"));
            tx.commit();
        }

        // Then
        try (Transaction tx = db.beginTx()) {
            var mapper = new DefaultValueMapper((InternalTransaction) tx);
            Path mapped =
                    mapper.mapPath(pathReference(asNodeValues(a, b, c, d, e), asRelationshipsValues(r1, r2, r3, r4)));
            assertThat(mapped.length()).isEqualTo(4);
            assertThat(mapped.startNode()).isEqualTo(a);
            assertThat(mapped.endNode()).isEqualTo(e);
            assertThat(Iterables.asList(mapped.relationships())).isEqualTo(Arrays.asList(r1, r2, r3, r4));
            assertThat(Iterables.asList(mapped.reverseRelationships())).isEqualTo(Arrays.asList(r4, r3, r2, r1));
            assertThat(Iterables.asList(mapped.nodes())).isEqualTo(Arrays.asList(a, b, c, d, e));
            assertThat(Iterables.asList(mapped.reverseNodes())).isEqualTo(Arrays.asList(e, d, c, b, a));
            assertThat(mapped.lastRelationship()).isEqualTo(r4);
            assertThat(Iterators.asList(mapped.iterator())).isEqualTo(Arrays.asList(a, r1, b, r2, c, r3, d, r4, e));
        }
    }

    @Test
    void shouldMapDirectRelationship() {
        // Given
        Node start, end;
        Relationship relationship;
        try (Transaction tx = db.beginTx()) {
            start = tx.createNode();
            end = tx.createNode();
            relationship = start.createRelationshipTo(end, RelationshipType.withName("R"));
            tx.commit();
        }
        RelationshipValue relationshipValue = relationshipValue(
                relationship.getId(),
                relationship.getElementId(),
                nodeValue(start.getId(), start.getElementId(), Values.EMPTY_TEXT_ARRAY, EMPTY_MAP),
                nodeValue(start.getId(), start.getElementId(), Values.EMPTY_TEXT_ARRAY, EMPTY_MAP),
                stringValue("R"),
                EMPTY_MAP);

        // Then
        try (Transaction tx = db.beginTx()) {
            var mapper = new DefaultValueMapper((InternalTransaction) tx);
            Relationship coreAPIRelationship = mapper.mapRelationship(relationshipValue);
            assertThat(coreAPIRelationship.getId()).isEqualTo(relationship.getId());
            assertThat(coreAPIRelationship.getElementId()).isEqualTo(relationship.getElementId());
            assertThat(coreAPIRelationship.getStartNode()).isEqualTo(start);
            assertThat(coreAPIRelationship.getEndNode()).isEqualTo(end);
        }
    }

    private static List<VirtualNodeValue> asNodeValues(Node... nodes) {
        return Arrays.stream(nodes).map(ValueUtils::fromNodeEntity).toList();
    }

    private static List<VirtualRelationshipValue> asRelationshipsValues(Relationship... relationships) {
        return Arrays.stream(relationships)
                .map(ValueUtils::fromRelationshipEntity)
                .toList();
    }
}
