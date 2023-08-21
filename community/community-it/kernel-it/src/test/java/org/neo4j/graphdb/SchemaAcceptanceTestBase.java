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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.function.Consumer;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.Schema;

public class SchemaAcceptanceTestBase {
    protected final String propertyKey = "my_property_key";
    protected final String secondPropertyKey = "my_second_property_key";
    protected final String propertyKeyWithBackticks = "``backticked_property_key";
    protected final Label label = Labels.MY_LABEL;
    protected final Label labelWithBackticks = Label.label("``BACKTICK`LABEL`");
    protected final RelationshipType relType = RelationshipType.withName("relType");

    protected static <EXCEPTION extends Throwable, CAUSE extends Throwable> void assertExpectedException(
            EXCEPTION exception, Class<CAUSE> expectedCause, String... expectedMessageParts) {
        final Throwable cause = exception.getCause();
        assertEquals(
                expectedCause,
                cause.getClass(),
                "Expected cause to be of type " + expectedCause + " but was " + cause.getClass());
        assertThat(exception.getMessage()).contains(expectedMessageParts);
    }

    protected void dropIndexBackedConstraintAndCreateSimilarInSameTxMustThrow(
            GraphDatabaseService db, ConstraintCreateOperation initial, ConstraintCreateOperation similar) {
        // Given
        String nameA = "nameA";
        String nameB = "nameB";
        try (Transaction tx = db.beginTx()) {
            initial.createConstraint(tx.schema(), propertyKey, nameA);
            tx.commit();
        }

        // When
        // Then
        var e = assertThrows(UnsupportedOperationException.class, () -> {
            try (Transaction tx = db.beginTx()) {
                tx.schema().getConstraintByName(nameA).drop();
                similar.createConstraint(tx.schema(), propertyKey, nameB);
                tx.commit();
            }
        });
        assertThat(e)
                .hasMessageContaining(format(
                        "Trying to create constraint '%s' in same transaction as dropping '%s'. "
                                + "This is not supported because they are both backed by similar indexes. "
                                + "Please drop constraint in a separate transaction before creating the new one.",
                        nameB, nameA));
    }

    protected void dropIndexBackedConstraintAndCreateSlightlyDifferentInSameTxMustSucceed(
            GraphDatabaseService db, ConstraintCreateOperation initial, ConstraintCreateOperation similar) {
        // Given
        String nameA = "nameA";
        String nameB = "nameB";
        try (Transaction tx = db.beginTx()) {
            initial.createConstraint(tx.schema(), propertyKey, nameA);
            tx.commit();
        }

        // When
        try (Transaction tx = db.beginTx()) {
            tx.schema().getConstraintByName(nameA).drop();
            similar.createConstraint(tx.schema(), secondPropertyKey, nameB);
            tx.commit();
        }

        // Then
        try (Transaction tx = db.beginTx()) {
            // First constraint should be dropped
            assertThrows(IllegalArgumentException.class, () -> tx.schema().getConstraintByName(nameA));

            // Second constraint should exist (doesn't throw)
            tx.schema().getConstraintByName(nameB);
            tx.commit();
        }
    }

    protected enum Labels implements Label {
        MY_LABEL,
        MY_OTHER_LABEL
    }

    protected enum SchemaTxStrategy {
        SEPARATE_TX {
            @Override
            public <EXCEPTION extends Throwable> EXCEPTION execute(
                    GraphDatabaseService db,
                    Consumer<Schema> firstSchemaRule,
                    Consumer<Schema> secondSchemaRule,
                    Class<EXCEPTION> expectedException) {
                try (Transaction tx = db.beginTx()) {
                    firstSchemaRule.accept(tx.schema());
                    tx.commit();
                }
                return assertThrows(expectedException, () -> {
                    try (Transaction tx = db.beginTx()) {
                        secondSchemaRule.accept(tx.schema());
                        tx.commit();
                    }
                });
            }
        },
        SAME_TX {
            @Override
            public <EXCEPTION extends Throwable> EXCEPTION execute(
                    GraphDatabaseService db,
                    Consumer<Schema> firstSchemaRule,
                    Consumer<Schema> secondSchemaRule,
                    Class<EXCEPTION> expectedException) {
                return assertThrows(expectedException, () -> {
                    try (Transaction tx = db.beginTx()) {
                        firstSchemaRule.accept(tx.schema());
                        secondSchemaRule.accept(tx.schema());
                        tx.commit();
                    }
                });
            }
        };

        public abstract <EXCEPTION extends Throwable> EXCEPTION execute(
                GraphDatabaseService db,
                Consumer<Schema> firstSchemaRule,
                Consumer<Schema> secondSchemaRule,
                Class<EXCEPTION> expectedException);
    }

    @FunctionalInterface
    protected interface ConstraintCreateOperation {
        ConstraintDefinition createConstraint(Schema schema, String prop, String name);
    }
}
