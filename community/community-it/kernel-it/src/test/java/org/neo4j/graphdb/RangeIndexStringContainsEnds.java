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
package org.neo4j.graphdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.StringSearchMode.CONTAINS;
import static org.neo4j.graphdb.StringSearchMode.PREFIX;
import static org.neo4j.graphdb.StringSearchMode.SUFFIX;
import static org.neo4j.graphdb.schema.IndexType.RANGE;

import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.TextIndexIT.IndexAccessMonitor;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
public class RangeIndexStringContainsEnds {

    @Inject
    protected DatabaseLayout databaseLayout;

    @Test
    void shouldFindNodesUsingRangeIndexIfNoTextIndex() {
        var person = label("PERSON");
        var monitor = new IndexAccessMonitor();
        var dbms = new TestDatabaseManagementServiceBuilder(databaseLayout)
                .setMonitors(monitor.monitors())
                .build();
        var db = dbms.database(DEFAULT_DATABASE_NAME);
        try (var tx = db.beginTx()) {
            tx.schema().indexFor(person).on("name").withIndexType(RANGE).create();
            tx.commit();
        }
        try (var tx = db.beginTx()) {
            tx.createNode(person).setProperty("name", "David Smith Adams");
            tx.createNode(person).setProperty("name", "Smith Evans");
            tx.createNode(person).setProperty("name", "Smith James");
            tx.createNode(person).setProperty("name", "Luke Smith");
            tx.commit();
        }

        // And monitor watching index access
        monitor.reset();

        // When the nodes are queried
        try (var tx = db.beginTx()) {
            assertThat(Iterators.count(tx.findNodes(person, "name", "Smith", CONTAINS)))
                    .isEqualTo(4);
            assertThat(Iterators.count(tx.findNodes(person, "name", "Unknown", CONTAINS)))
                    .isEqualTo(0);
            assertThat(Iterators.count(tx.findNodes(person, "name", "Smith", PREFIX)))
                    .isEqualTo(2);
            assertThat(Iterators.count(tx.findNodes(person, "name", "Smith", SUFFIX)))
                    .isEqualTo(1);
        }

        // Then all queries go to RANGE index even if it doesn't have native support for contains and suffix
        assertThat(monitor.accessed(org.neo4j.internal.schema.IndexType.RANGE)).isEqualTo(4);
        dbms.shutdown();
    }

    @Test
    void shouldFindRelationshipsUsingRangeIndexIfNoTextIndex() {
        var person = label("PERSON");
        var relation = RelationshipType.withName("FRIEND");
        var monitor = new IndexAccessMonitor();
        var dbms = new TestDatabaseManagementServiceBuilder(databaseLayout)
                .setMonitors(monitor.monitors())
                .build();
        var db = dbms.database(DEFAULT_DATABASE_NAME);
        try (var tx = db.beginTx()) {
            tx.schema().indexFor(relation).on("since").withIndexType(RANGE).create();
            tx.commit();
        }
        try (var tx = db.beginTx()) {
            tx.createNode(person)
                    .createRelationshipTo(tx.createNode(person), relation)
                    .setProperty("since", "two years");
            tx.createNode(person)
                    .createRelationshipTo(tx.createNode(person), relation)
                    .setProperty("since", "five years, two months");
            tx.createNode(person)
                    .createRelationshipTo(tx.createNode(person), relation)
                    .setProperty("since", "three months");
            tx.commit();
        }

        // And an index monitor
        monitor.reset();

        // When the relationships are queried
        try (var tx = db.beginTx()) {
            assertThat(Iterators.count(tx.findRelationships(relation, "since", "years", CONTAINS)))
                    .isEqualTo(2);
            assertThat(Iterators.count(tx.findRelationships(relation, "since", "unknown", CONTAINS)))
                    .isEqualTo(0);
            assertThat(Iterators.count(tx.findRelationships(relation, "since", "five", PREFIX)))
                    .isEqualTo(1);
            assertThat(Iterators.count(tx.findRelationships(relation, "since", "months", SUFFIX)))
                    .isEqualTo(2);
        }

        // Then all queries go to RANGE index even if it doesn't have native support for contains and suffix
        assertThat(monitor.accessed(org.neo4j.internal.schema.IndexType.RANGE)).isEqualTo(4);
        dbms.shutdown();
    }
}
