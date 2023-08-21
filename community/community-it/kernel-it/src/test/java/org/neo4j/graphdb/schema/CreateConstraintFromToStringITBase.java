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
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.Inject;

@TestInstance(PER_CLASS)
public class CreateConstraintFromToStringITBase {
    protected static final Label LABEL = TestLabels.LABEL_ONE;
    protected static final RelationshipType REL_TYPE = RelationshipType.withName("REL_TYPE");
    protected static final String PROP_ONE = "propOne";
    protected static final String PROP_TWO = "propTwo";

    @Inject
    private GraphDatabaseService db;

    @BeforeEach
    void cleanupConstraint() {
        try (Transaction tx = db.beginTx()) {
            tx.schema().getConstraints().forEach(ConstraintDefinition::drop);
            tx.commit();
        }
    }

    protected void testShouldRecreateSimilarConstraintFromToStringMethod(ConstraintFunction constraintFunction) {
        // Given a constraint
        ConstraintDefinition originalConstraint;
        try (Transaction tx = db.beginTx()) {
            originalConstraint = constraintFunction.apply(tx.schema()).create();
            tx.commit();
        }
        assertConstraintCount(1);

        // When we drop it
        try (Transaction tx = db.beginTx()) {
            tx.schema().getConstraintByName(originalConstraint.getName()).drop();
            tx.commit();
        }
        assertConstraintCount(0);

        // Then we should be able to recreate it with the toString() method
        try (Transaction tx = db.beginTx()) {
            tx.execute("CREATE CONSTRAINT " + originalConstraint);
            tx.commit();
        }
        assertConstraintCount(1);

        try (Transaction tx = db.beginTx()) {
            var resultingConstraint = Iterables.single(tx.schema().getConstraints());
            assertThat(resultingConstraint.toString()).isEqualTo(originalConstraint.toString());
            tx.commit();
        }
    }

    private void assertConstraintCount(int expectedNumberOfConstraints) {
        try (Transaction tx = db.beginTx()) {
            assertThat(Iterables.count(tx.schema().getConstraints())).isEqualTo(expectedNumberOfConstraints);
            tx.commit();
        }
    }

    @FunctionalInterface
    protected interface ConstraintFunction {
        ConstraintCreator apply(Schema schema);
    }
}
