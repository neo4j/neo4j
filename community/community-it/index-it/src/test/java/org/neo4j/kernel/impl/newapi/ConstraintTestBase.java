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
package org.neo4j.kernel.impl.newapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterators.asList;

import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;

@SuppressWarnings("Duplicates")
public abstract class ConstraintTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G> {
    abstract SchemaDescriptor schemaDescriptor(int tokenId, int... propertyIds);

    abstract ConstraintDescriptor uniqueConstraintDescriptor(int tokenId, int... propertyIds);

    abstract ConstraintDefinition createConstraint(Schema schema, String entityToken, String propertyKey);

    abstract int entityTokenId(TokenWrite tokenWrite, String entityToken) throws KernelException;

    abstract Iterator<ConstraintDescriptor> getConstraintsByEntityToken(SchemaRead schemaRead, int entityTokenId);

    @BeforeEach
    public void setup() {
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            for (ConstraintDefinition definition : tx.schema().getConstraints()) {
                definition.drop();
            }
            tx.commit();
        }
    }

    @Test
    void shouldFindConstraintsBySchema() throws Exception {
        // GIVEN
        addConstraints("FOO", "prop");

        try (KernelTransaction tx = beginTransaction()) {
            int token = entityTokenId(tx.tokenWrite(), "FOO");
            int prop = tx.tokenWrite().propertyKeyGetOrCreateForName("prop");
            SchemaDescriptor descriptor = schemaDescriptor(token, prop);

            // WHEN
            List<ConstraintDescriptor> constraints = asList(tx.schemaRead().constraintsGetForSchema(descriptor));

            // THEN
            assertThat(constraints).hasSize(1);
            assertThat(constraints.get(0).schema().getPropertyId()).isEqualTo(prop);
        }
    }

    @Test
    void shouldFindConstraintsByEntityToken() throws Exception {
        // GIVEN
        addConstraints("FOO", "prop1", "FOO", "prop2");

        try (KernelTransaction tx = beginTransaction()) {
            int entityToken = entityTokenId(tx.tokenWrite(), "FOO");

            // WHEN
            List<ConstraintDescriptor> constraints = asList(getConstraintsByEntityToken(tx.schemaRead(), entityToken));

            // THEN
            assertThat(constraints).hasSize(2);
        }
    }

    @Test
    void shouldBeAbleCheckExistenceOfConstraints() throws Exception {
        // GIVEN
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {

            createConstraint(tx.schema(), "FOO", "prop1");
            ConstraintDefinition dropped = createConstraint(tx.schema(), "FOO", "prop2");
            dropped.drop();
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            int entityToken = entityTokenId(tx.tokenWrite(), "FOO");
            int prop1 = tx.tokenWrite().propertyKeyGetOrCreateForName("prop1");
            int prop2 = tx.tokenWrite().propertyKeyGetOrCreateForName("prop2");

            // THEN
            assertTrue(tx.schemaRead().constraintExists(uniqueConstraintDescriptor(entityToken, prop1)));
            assertFalse(tx.schemaRead().constraintExists(uniqueConstraintDescriptor(entityToken, prop2)));
        }
    }

    @Test
    void shouldFindAllConstraints() throws Exception {
        // GIVEN
        addConstraints("FOO", "prop1", "BAR", "prop2", "BAZ", "prop3");

        try (KernelTransaction tx = beginTransaction()) {
            // WHEN
            List<ConstraintDescriptor> constraints = asList(tx.schemaRead().constraintsGetAll());

            // THEN
            assertThat(constraints).hasSize(3);
        }
    }

    void addConstraints(String... entityTokenAndProps) {
        assert entityTokenAndProps.length % 2 == 0;

        try (Transaction tx = graphDb.beginTx()) {
            for (int i = 0; i < entityTokenAndProps.length; i += 2) {
                createConstraint(tx.schema(), entityTokenAndProps[i], entityTokenAndProps[i + 1]);
            }
            tx.commit();
        }
    }
}
