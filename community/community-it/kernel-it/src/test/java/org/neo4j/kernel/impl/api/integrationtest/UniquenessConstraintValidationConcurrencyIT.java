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
package org.neo4j.kernel.impl.api.integrationtest;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.util.Strings;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThread;
import org.neo4j.test.extension.OtherThreadExtension;

@ImpermanentDbmsExtension
@ExtendWith(OtherThreadExtension.class)
public class UniquenessConstraintValidationConcurrencyIT {
    private static String TOKEN = "Token1";
    private static String KEY = "key1";
    private static String VALUE = "value1";
    private static String VALUE2 = "value2";

    @Inject
    private GraphDatabaseService database;

    @Inject
    private OtherThread otherThread;

    @ParameterizedTest
    @EnumSource(EntityControl.class)
    void shouldAllowConcurrentCreationOfNonConflictingData(EntityControl entityControl) throws Exception {
        createTestConstraint(entityControl);

        try (var tx = database.beginTx()) {
            entityControl.createEntityWithTokenAndProp(tx, VALUE);
            assertTrue(otherThread.execute(createEntity(entityControl, VALUE2)).get());
        }
    }

    @ParameterizedTest
    @EnumSource(EntityControl.class)
    void shouldPreventConcurrentCreationOfConflictingData(EntityControl entityControl) throws Exception {
        createTestConstraint(entityControl);

        Future<Boolean> result;
        try (var tx = database.beginTx()) {
            entityControl.createEntityWithTokenAndProp(tx, VALUE);
            try {
                result = otherThread.execute(createEntity(entityControl, VALUE));
            } finally {
                waitUntilWaiting();
            }
            tx.commit();
        }
        assertFalse(result.get(), "entity creation should fail");
    }

    @ParameterizedTest
    @EnumSource(EntityControl.class)
    void shouldAllowOtherTransactionToCompleteIfFirstTransactionRollsBack(EntityControl entityControl)
            throws Exception {
        createTestConstraint(entityControl);

        // when
        Future<Boolean> result;
        try (var tx = database.beginTx()) {
            entityControl.createEntityWithTokenAndProp(tx, VALUE);
            try {
                result = otherThread.execute(createEntity(entityControl, VALUE));
            } finally {
                waitUntilWaiting();
            }
            tx.rollback();
        }
        assertTrue(result.get(), "entity creation should succeed");
    }

    @ParameterizedTest
    @EnumSource(EntityControl.class)
    void shouldFailPolitelyOnTooLargeKeyLength(EntityControl entityControl) throws Exception {
        // given
        int numChars = 40_000;
        try (var tx = database.beginTx()) {
            entityControl.createEntityWithTokenAndProp(tx, Strings.repeat("a", numChars));
            tx.commit();
        }

        assertThatThrownBy(() -> createTestConstraint(entityControl))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Property value is too large to index");
    }

    private void createTestConstraint(EntityControl entityControl) {
        try (var transaction = database.beginTx()) {
            entityControl.constraint(transaction.schema()).create();
            transaction.commit();
        }
    }

    private void waitUntilWaiting() {
        try {
            otherThread.get().waitUntilWaiting();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public Callable<Boolean> createEntity(EntityControl entityControl, String propertyValue) {
        return () -> {
            try (Transaction tx = database.beginTx()) {
                entityControl.createEntityWithTokenAndProp(tx, propertyValue);

                tx.commit();
                return true;
            } catch (ConstraintViolationException e) {
                return false;
            }
        };
    }

    enum EntityControl {
        NODE {
            @Override
            ConstraintCreator constraint(Schema schema) {
                return schema.constraintFor(label(TOKEN)).assertPropertyIsUnique(KEY);
            }

            @Override
            void createEntityWithTokenAndProp(Transaction tx, String value) {
                tx.createNode(label(TOKEN)).setProperty(KEY, value);
            }
        },
        RELATIONSHIP {
            @Override
            ConstraintCreator constraint(Schema schema) {
                return schema.constraintFor(withName(TOKEN)).assertPropertyIsUnique(KEY);
            }

            @Override
            void createEntityWithTokenAndProp(Transaction tx, String value) {
                Node node = tx.createNode();
                node.createRelationshipTo(node, RelationshipType.withName(TOKEN))
                        .setProperty(KEY, value);
            }
        };

        abstract ConstraintCreator constraint(Schema schema);

        abstract void createEntityWithTokenAndProp(Transaction tx, String value);
    }
}
