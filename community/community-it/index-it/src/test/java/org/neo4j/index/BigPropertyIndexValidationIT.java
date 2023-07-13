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
package org.neo4j.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class BigPropertyIndexValidationIT {
    @Inject
    private GraphDatabaseService db;

    private Label LABEL;
    private String longString;
    private String propertyKey;

    @BeforeEach
    void setup() {
        LABEL = Label.label("LABEL");
        char[] chars = new char[1 << 15];
        Arrays.fill(chars, 'c');
        longString = new String(chars);
        propertyKey = "name";
    }

    @Test
    void shouldFailTransactionThatIndexesLargePropertyDuringNodeCreation() {
        // GIVEN
        createIndex(db, LABEL, propertyKey);

        // We expect this transaction to fail due to the huge property
        assertThrows(TransientTransactionFailureException.class, () -> {
            try (Transaction tx = db.beginTx()) {
                assertThrows(
                        IllegalArgumentException.class,
                        () -> tx.execute("CREATE (n:" + LABEL + " {name: \"" + longString + "\"})"));
                tx.commit();
            }
            // Check that the database is empty.
            try (Transaction tx = db.beginTx();
                    ResourceIterable<Node> allNodes = tx.getAllNodes()) {
                assertThat(allNodes).hasSize(0);
            }
        });
    }

    @Test
    void shouldFailTransactionThatIndexesLargePropertyAfterNodeCreation() {
        // GIVEN
        createIndex(db, LABEL, propertyKey);

        // We expect this transaction to fail due to the huge property
        assertThrows(TransientTransactionFailureException.class, () -> {
            try (Transaction tx = db.beginTx()) {
                tx.execute("CREATE (n:" + LABEL + ")");
                assertThrows(
                        IllegalArgumentException.class,
                        () -> tx.execute("match (n:" + LABEL + ")set n.name= \"" + longString + "\""));
                tx.commit();
            }
            // Check that the database is empty.
            try (Transaction tx = db.beginTx();
                    ResourceIterable<Node> allNodes = tx.getAllNodes()) {
                assertThat(allNodes).hasSize(0);
            }
        });
    }

    @Test
    void shouldFailTransactionThatIndexesLargePropertyOnLabelAdd() {
        // GIVEN
        createIndex(db, LABEL, propertyKey);

        // We expect this transaction to fail due to the huge property
        assertThrows(TransientTransactionFailureException.class, () -> {
            try (Transaction tx = db.beginTx()) {
                String otherLabel = "SomethingElse";
                tx.execute("CREATE (n:" + otherLabel + " {name: \"" + longString + "\"})");
                assertThrows(
                        IllegalArgumentException.class, () -> tx.execute("match (n:" + otherLabel + ")set n:" + LABEL));
                tx.commit();
            }
            // Check that the database is empty.
            try (Transaction tx = db.beginTx();
                    ResourceIterable<Node> allNodes = tx.getAllNodes()) {
                assertThat(allNodes).hasSize(0);
            }
        });
    }

    private static void createIndex(GraphDatabaseService gds, Label label, String propKey) {
        try (Transaction tx = gds.beginTx()) {
            tx.schema().indexFor(label).on(propKey).create();
            tx.commit();
        }

        try (Transaction tx = gds.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.commit();
        }
    }
}
