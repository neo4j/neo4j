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
package org.neo4j.kernel.impl.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterators.single;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.IndexingTestUtil;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.ConstraintType;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorPredicates;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.token.TokenHolders;

@ImpermanentDbmsExtension
class SchemaStorageIT {
    private static final String LABEL1 = "Label1";
    private static final String LABEL2 = "Label2";
    private static final String TYPE1 = "Type1";
    private static final String PROP1 = "prop1";
    private static final String PROP2 = "prop2";

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private StorageEngine storageEngine;

    @Inject
    private TokenHolders tokenHolders;

    @BeforeEach
    void initStorage() throws Exception {
        try (Transaction transaction = db.beginTx()) {
            TokenWrite tokenWrite =
                    ((InternalTransaction) transaction).kernelTransaction().tokenWrite();
            tokenWrite.propertyKeyGetOrCreateForName(PROP1);
            tokenWrite.propertyKeyGetOrCreateForName(PROP2);
            tokenWrite.labelGetOrCreateForName(LABEL1);
            tokenWrite.labelGetOrCreateForName(LABEL2);
            tokenWrite.relationshipTypeGetOrCreateForName(TYPE1);
            transaction.commit();
        }
    }

    @Test
    void shouldReturnIndexRuleForLabelAndProperty() {
        // Given
        createSchema(index(LABEL1, PROP1), index(LABEL1, PROP2), index(LABEL2, PROP1));

        // When
        IndexDescriptor rule = indexGetForSchema(LABEL1, PROP2);

        // Then
        assertNotNull(rule);
        assertRule(rule, LABEL1, PROP2, false);
    }

    @Test
    void shouldReturnIndexRuleForLabelAndPropertyComposite() {
        // Given
        String a = "a";
        String b = "b";
        String c = "c";
        String d = "d";
        String e = "e";
        String f = "f";
        createSchema(tx -> tx.schema()
                .indexFor(Label.label(LABEL1))
                .on(a)
                .on(b)
                .on(c)
                .on(d)
                .on(e)
                .on(f)
                .create());

        // When

        IndexDescriptor rule = indexGetForSchema(LABEL1, a, b, c, d, e, f);

        assertNotNull(rule);
        assertTrue(SchemaDescriptorPredicates.hasLabel(rule, labelId(LABEL1)));
        assertTrue(SchemaDescriptorPredicates.hasProperty(rule, propId(a)));
        assertTrue(SchemaDescriptorPredicates.hasProperty(rule, propId(b)));
        assertTrue(SchemaDescriptorPredicates.hasProperty(rule, propId(c)));
        assertTrue(SchemaDescriptorPredicates.hasProperty(rule, propId(d)));
        assertTrue(SchemaDescriptorPredicates.hasProperty(rule, propId(e)));
        assertTrue(SchemaDescriptorPredicates.hasProperty(rule, propId(f)));
        assertFalse(rule.isUnique());
    }

    @Test
    void shouldReturnIndexRuleForLabelAndVeryManyPropertiesComposite() {
        // Given
        String[] props = "abcdefghijklmnopqrstuvwxyzABCDEFGHJILKMNOPQRSTUVWXYZ".split("\\B");
        createSchema(tx -> {
            IndexCreator indexCreator = tx.schema().indexFor(Label.label(LABEL1));
            for (String prop : props) {
                indexCreator = indexCreator.on(prop);
            }
            indexCreator.create();
        });

        // When
        IndexDescriptor rule = indexGetForSchema(LABEL1, props);

        // Then
        assertNotNull(rule);
        assertTrue(SchemaDescriptorPredicates.hasLabel(rule, labelId(LABEL1)));
        for (String prop : props) {
            assertTrue(SchemaDescriptorPredicates.hasProperty(rule, propId(prop)));
        }
        assertFalse(rule.isUnique());
    }

    @Test
    void shouldReturnEmptyArrayIfIndexRuleForLabelAndPropertyDoesNotExist() {
        // Given
        createSchema(index(LABEL1, PROP1));

        // When
        IndexDescriptor rule = indexGetForSchema(LABEL1, PROP2);

        // Then
        assertThat(rule).isNull();
    }

    @Test
    void shouldListIndexRulesForLabelPropertyAndKind() {
        // Given
        createSchema(uniquenessConstraint(LABEL1, PROP1), index(LABEL1, PROP2));

        // When
        IndexDescriptor rule = indexGetForSchema(LABEL1, PROP1);

        // Then
        assertNotNull(rule);
        assertRule(rule, LABEL1, PROP1, true);
    }

    @Test
    void shouldListAllIndexRules() {
        // Setup
        IndexingTestUtil.dropAllIndexes(db);

        // Given
        createSchema(index(LABEL1, PROP1), index(LABEL1, PROP2), uniquenessConstraint(LABEL2, PROP1));

        // When
        Set<SchemaDescriptor> listedSchema = new HashSet<>();
        try (var reader = storageEngine.newReader()) {
            reader.indexesGetAll().forEachRemaining(rule -> listedSchema.add(rule.schema()));
        }

        // Then
        Set<SchemaDescriptor> expectedSchema = new HashSet<>();
        expectedSchema.add(SchemaDescriptors.forLabel(labelId(LABEL1), propId(PROP1)));
        expectedSchema.add(SchemaDescriptors.forLabel(labelId(LABEL1), propId(PROP2)));
        expectedSchema.add(SchemaDescriptors.forLabel(labelId(LABEL2), propId(PROP1)));

        assertEquals(expectedSchema, listedSchema);
    }

    @Test
    void shouldReturnCorrectUniquenessRuleForLabelAndProperty() {
        // Given
        createSchema(uniquenessConstraint(LABEL1, PROP1), uniquenessConstraint(LABEL2, PROP1));

        // When
        try (var reader = storageEngine.newReader()) {
            ConstraintDescriptor rule =
                    single(reader.constraintsGetForSchema(SchemaDescriptors.forLabel(labelId(LABEL1), propId(PROP1))));
            // Then
            assertNotNull(rule);
            assertRule(rule, LABEL1, PROP1, ConstraintType.UNIQUE);
        }
    }

    private IndexDescriptor indexGetForSchema(String label, String... propertyKeys) {
        return indexGetForSchema(SchemaDescriptors.forLabel(
                labelId(label),
                Arrays.stream(propertyKeys).mapToInt(this::propId).toArray()));
    }

    private IndexDescriptor indexGetForSchema(SchemaDescriptor descriptor) {
        try (var reader = storageEngine.newReader()) {
            return Iterators.singleOrNull(reader.indexGetForSchema(descriptor));
        }
    }

    private void assertRule(IndexDescriptor rule, String label, String propertyKey, boolean isUnique) {
        assertTrue(SchemaDescriptorPredicates.hasLabel(rule, labelId(label)));
        assertTrue(SchemaDescriptorPredicates.hasProperty(rule, propId(propertyKey)));
        assertEquals(isUnique, rule.isUnique());
    }

    private void assertRule(ConstraintDescriptor constraint, String label, String propertyKey, ConstraintType type) {
        assertTrue(SchemaDescriptorPredicates.hasLabel(constraint, labelId(label)));
        assertTrue(SchemaDescriptorPredicates.hasProperty(constraint, propId(propertyKey)));
        assertEquals(type, constraint.type());
    }

    private int labelId(String labelName) {
        try (Transaction tx = db.beginTx()) {
            return ((InternalTransaction) tx).kernelTransaction().tokenRead().nodeLabel(labelName);
        }
    }

    private int propId(String propName) {
        try (Transaction tx = db.beginTx()) {
            return ((InternalTransaction) tx).kernelTransaction().tokenRead().propertyKey(propName);
        }
    }

    private static Consumer<Transaction> index(String label, String prop) {
        return tx -> tx.schema().indexFor(Label.label(label)).on(prop).create();
    }

    private static Consumer<Transaction> uniquenessConstraint(String label, String prop) {
        return tx -> tx.schema()
                .constraintFor(Label.label(label))
                .assertPropertyIsUnique(prop)
                .create();
    }

    @SafeVarargs
    private void createSchema(Consumer<Transaction>... creators) {
        try (Transaction tx = db.beginTx()) {
            for (Consumer<Transaction> rule : creators) {
                rule.accept(tx);
            }
            tx.commit();
        }
        awaitIndexes();
    }

    private void awaitIndexes() {
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.MINUTES);
            tx.commit();
        }
    }
}
