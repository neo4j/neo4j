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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exact;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptors.forRelType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class UniquenessConstraintValidationIT extends KernelIntegrationTest {
    private static String TOKEN = "Token1";
    private static String KEY = "key1";
    private static String VALUE = "value1";

    private static long createLabeledNode(KernelTransaction transaction, String label) throws KernelException {
        long node = transaction.dataWrite().nodeCreate();
        int labelId = transaction.tokenWrite().labelGetOrCreateForName(label);
        transaction.dataWrite().nodeAddLabel(node, labelId);
        return node;
    }

    private static long createNode(KernelTransaction transaction, String key, Object value) throws KernelException {
        long node = transaction.dataWrite().nodeCreate();
        int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName(key);
        transaction.dataWrite().nodeSetProperty(node, propertyKeyId, Values.of(value));
        return node;
    }

    private static long createLabeledNode(KernelTransaction transaction, String label, String key, Object value)
            throws KernelException {
        long node = createLabeledNode(transaction, label);
        int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName(key);
        transaction.dataWrite().nodeSetProperty(node, propertyKeyId, Values.of(value));
        return node;
    }

    private static long createRelationship(KernelTransaction transaction, String type, String key, Object value)
            throws KernelException {
        Write write = transaction.dataWrite();
        TokenWrite tokenWrite = transaction.tokenWrite();
        int typeId = tokenWrite.relationshipTypeGetOrCreateForName(type);
        long rel = write.relationshipCreate(write.nodeCreate(), typeId, write.nodeCreate());
        int propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName(key);
        write.relationshipSetProperty(rel, propertyKeyId, Values.of(value));
        return rel;
    }

    @ParameterizedTest
    @EnumSource(EntityControl.class)
    void shouldEnforceOnSetProperty(EntityControl entityControl) throws Exception {
        // given
        constrainedEntity(entityControl, TOKEN, KEY, VALUE);

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());

        // when/then
        long entity = entityControl.createEntityWithToken(transaction, TOKEN);
        UniquePropertyValueValidationException e = assertThrows(UniquePropertyValueValidationException.class, () -> {
            int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName(KEY);
            entityControl.setProperty(transaction.dataWrite(), entity, propertyKeyId, Values.of(VALUE));
        });
        assertThat(e.getUserMessage(transaction.tokenRead())).contains("`key1` = 'value1'");
        commit();
    }

    @Test
    void roundingErrorsFromLongToDoubleShouldNotPreventTxFromCommitting() throws Exception {
        // Given
        // a node with a constrained label and a long value
        long propertyValue = 285414114323346805L;
        long firstNode = constrainedNode(TOKEN, KEY, propertyValue);

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());

        long node = createLabeledNode(transaction, TOKEN);

        assertNotEquals(firstNode, node);

        // When
        // a new node with the same constraint is added, with a value not equal but which would be mapped to the same
        // double
        propertyValue++;
        // note how propertyValue is definitely not equal to propertyValue++ but they do equal if they are cast to
        // double
        int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName(KEY);
        transaction.dataWrite().nodeSetProperty(node, propertyKeyId, Values.of(propertyValue));

        // Then
        // the commit should still succeed
        commit();
    }

    @Test
    void shouldEnforceUniquenessConstraintOnAddLabelForNumberPropertyOnNodeNotFromTransaction() throws Exception {
        // given
        constrainedNode(TOKEN, KEY, 1);

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());
        long node = createNode(transaction, KEY, 1);
        commit();

        // when/then
        KernelTransaction transaction2 = newTransaction(AnonymousContext.writeToken());
        UniquePropertyValueValidationException e = assertThrows(UniquePropertyValueValidationException.class, () -> {
            int label = transaction2.tokenWrite().labelGetOrCreateForName(TOKEN);
            transaction2.dataWrite().nodeAddLabel(node, label);
        });
        assertThat(e.getUserMessage(transaction2.tokenRead())).contains("`key1` = 1");

        commit();
    }

    @Test
    void shouldEnforceUniquenessConstraintOnAddLabelForStringProperty() throws Exception {
        // given
        constrainedNode(TOKEN, KEY, VALUE);

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());

        // when/then
        long node = createNode(transaction, KEY, VALUE);
        UniquePropertyValueValidationException e = assertThrows(UniquePropertyValueValidationException.class, () -> {
            int label = transaction.tokenWrite().labelGetOrCreateForName(TOKEN);
            transaction.dataWrite().nodeAddLabel(node, label);
        });
        assertThat(e.getUserMessage(transaction.tokenRead())).contains("`key1` = 'value1'");

        commit();
    }

    @ParameterizedTest
    @EnumSource(EntityControl.class)
    void shouldAllowRemoveAndAddConflictingDataInOneTransaction_DeleteEntity(EntityControl entityControl)
            throws Exception {
        // given
        long entity = constrainedEntity(entityControl, TOKEN, KEY, VALUE);

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());

        // when
        entityControl.deleteEntity(transaction.dataWrite(), entity);
        entityControl.createEntityWithTokenAndProp(transaction, TOKEN, KEY, Values.of(VALUE));
        commit();
    }

    @Test
    void shouldAllowRemoveAndAddConflictingDataInOneTransaction_RemoveLabel() throws Exception {
        // given
        long node = constrainedNode(TOKEN, KEY, VALUE);

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());

        // when
        int label = transaction.tokenWrite().labelGetOrCreateForName(TOKEN);
        transaction.dataWrite().nodeRemoveLabel(node, label);
        createLabeledNode(transaction, TOKEN, KEY, VALUE);
        commit();
    }

    @ParameterizedTest
    @EnumSource(EntityControl.class)
    void shouldAllowRemoveAndAddConflictingDataInOneTransaction_RemoveProperty(EntityControl entityControl)
            throws Exception {
        // given
        long entity = constrainedEntity(entityControl, TOKEN, KEY, VALUE);

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());

        // when
        int key = transaction.tokenRead().propertyKey(KEY);
        entityControl.removeProperty(transaction.dataWrite(), entity, key);
        entityControl.createEntityWithTokenAndProp(transaction, TOKEN, KEY, Values.of(VALUE));
        commit();
    }

    @ParameterizedTest
    @EnumSource(EntityControl.class)
    void shouldAllowRemoveAndAddConflictingDataInOneTransaction_ChangeProperty(EntityControl entityControl)
            throws Exception {
        // given
        long entity = constrainedEntity(entityControl, TOKEN, KEY, VALUE);

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());

        // when
        int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName(KEY);
        entityControl.setProperty(transaction.dataWrite(), entity, propertyKeyId, Values.of("value2"));
        entityControl.createEntityWithTokenAndProp(transaction, TOKEN, KEY, Values.of(VALUE));
        commit();
    }

    @ParameterizedTest
    @EnumSource(EntityControl.class)
    void shouldPreventConflictingDataInSameTransaction(EntityControl entityControl) throws Exception {
        // given
        constrainedEntity(entityControl, TOKEN, KEY, VALUE);

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());

        // when/then
        entityControl.createEntityWithTokenAndProp(transaction, TOKEN, KEY, Values.of("value2"));
        UniquePropertyValueValidationException e = assertThrows(
                UniquePropertyValueValidationException.class,
                () -> entityControl.createEntityWithTokenAndProp(transaction, TOKEN, KEY, Values.of("value2")));
        assertThat(e.getUserMessage(transaction.tokenRead())).contains("`key1` = 'value2'");

        commit();
    }

    @ParameterizedTest
    @EnumSource(EntityControl.class)
    void shouldAllowNoopPropertyUpdate(EntityControl entityControl) throws KernelException {
        // given
        long entity = constrainedEntity(entityControl, TOKEN, KEY, VALUE);

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());

        // when
        int key = transaction.tokenWrite().propertyKeyGetOrCreateForName(KEY);
        entityControl.setProperty(transaction.dataWrite(), entity, key, Values.of(VALUE));

        // then should not throw exception
        commit();
    }

    @Test
    void shouldAllowNoopLabelUpdate() throws KernelException {
        // given
        long node = constrainedNode(TOKEN, KEY, VALUE);

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());

        // when
        int label = transaction.tokenWrite().labelGetOrCreateForName(TOKEN);
        transaction.dataWrite().nodeAddLabel(node, label);

        // then should not throw exception
        commit();
    }

    @ParameterizedTest
    @EnumSource(EntityControl.class)
    void shouldAllowCreationOfNonConflictingData(EntityControl entityControl) throws Exception {
        // given
        constrainedEntity(entityControl, TOKEN, KEY, VALUE);

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());

        // when
        createNode(transaction, KEY, VALUE);
        entityControl.createEntityWithTokenAndProp(transaction, "Token2", KEY, Values.of(VALUE));
        entityControl.createEntityWithTokenAndProp(transaction, TOKEN, KEY, Values.of("value2"));
        entityControl.createEntityWithTokenAndProp(transaction, TOKEN, "key2", Values.of(VALUE));

        commit();

        // then
        transaction = newTransaction(AnonymousContext.writeToken());
        assertEquals(
                entityControl == EntityControl.NODE ? 5 : 4,
                entityControl.countEntities(transaction),
                "number of entities");
        rollback();
    }

    @Test
    void unrelatedNodesWithSamePropertyShouldNotInterfereWithUniquenessCheck() throws Exception {
        // given
        ConstraintDescriptor constraint = createNodeConstraint("Person", "id");

        long ourNode;
        {
            KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());
            ourNode = createLabeledNode(transaction, "Person", "id", 1);
            createLabeledNode(transaction, "Item", "id", 2);
            commit();
        }

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());
        TokenRead tokenRead = transaction.tokenRead();
        int propId = tokenRead.propertyKey("id");
        IndexDescriptor idx = transaction.schemaRead().indexGetForName(constraint.getName());

        // when
        createLabeledNode(transaction, "Item", "id", 2);

        // then I should find the original node
        try (NodeValueIndexCursor cursor = transaction
                .cursors()
                .allocateNodeValueIndexCursor(transaction.cursorContext(), transaction.memoryTracker())) {
            assertThat(transaction.dataRead().lockingNodeUniqueIndexSeek(idx, cursor, exact(propId, Values.of(1))))
                    .isEqualTo(ourNode);
        }
        commit();
    }

    @Test
    void unrelatedRelationshipWithSamePropertyShouldNotInterfereWithUniquenessCheck() throws Exception {
        // given
        ConstraintDescriptor constraint = createRelationshipConstraint("R", "id");

        long ourRelationship;
        {
            KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());
            ourRelationship = createRelationship(transaction, "R", "id", 1);
            createRelationship(transaction, "OTHER", "id", 2);
            commit();
        }

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());
        TokenRead tokenRead = transaction.tokenRead();
        int propId = tokenRead.propertyKey("id");
        IndexDescriptor idx = transaction.schemaRead().indexGetForName(constraint.getName());

        // when
        createRelationship(transaction, "OTHER", "id", 2);

        // then I should find the original relationship
        try (RelationshipValueIndexCursor cursor = transaction
                .cursors()
                .allocateRelationshipValueIndexCursor(transaction.cursorContext(), transaction.memoryTracker())) {
            assertThat(transaction
                            .dataRead()
                            .lockingRelationshipUniqueIndexSeek(idx, cursor, exact(propId, Values.of(1))))
                    .isEqualTo(ourRelationship);
        }
        commit();
    }

    @Test
    void addingUniqueNodeWithUnrelatedValueShouldNotAffectLookup() throws Exception {
        // given
        ConstraintDescriptor constraint = createNodeConstraint("Person", "id");

        long ourNode;
        {
            KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());
            ourNode = createLabeledNode(transaction, "Person", "id", 1);
            commit();
        }

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());
        TokenRead tokenRead = transaction.tokenRead();
        int propId = tokenRead.propertyKey("id");
        IndexDescriptor idx = transaction.schemaRead().indexGetForName(constraint.getName());

        // when
        createLabeledNode(transaction, "Person", "id", 2);

        // then I should find the original node
        try (NodeValueIndexCursor cursor = transaction
                .cursors()
                .allocateNodeValueIndexCursor(transaction.cursorContext(), transaction.memoryTracker())) {
            assertThat(transaction.dataRead().lockingNodeUniqueIndexSeek(idx, cursor, exact(propId, Values.of(1))))
                    .isEqualTo(ourNode);
        }
        commit();
    }

    @Test
    void addingUniqueRelationshipWithUnrelatedValueShouldNotAffectLookup() throws Exception {
        // given
        ConstraintDescriptor constraint = createRelationshipConstraint("R", "id");

        long ourRelationship;
        {
            KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());
            ourRelationship = createRelationship(transaction, "R", "id", 1);
            commit();
        }

        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());
        TokenRead tokenRead = transaction.tokenRead();
        int propId = tokenRead.propertyKey("id");
        IndexDescriptor idx = transaction.schemaRead().indexGetForName(constraint.getName());

        // when
        createRelationship(transaction, "R", "id", 2);

        // then I should find the original relationship
        try (RelationshipValueIndexCursor cursor = transaction
                .cursors()
                .allocateRelationshipValueIndexCursor(transaction.cursorContext(), transaction.memoryTracker())) {
            assertThat(transaction
                            .dataRead()
                            .lockingRelationshipUniqueIndexSeek(idx, cursor, exact(propId, Values.of(1))))
                    .isEqualTo(ourRelationship);
        }
        commit();
    }

    private long constrainedEntity(
            EntityControl entityControl, String tokenName, String propertyKey, Object propertyValue)
            throws KernelException {
        long node;
        int token;
        int key;
        {
            KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());
            token = entityControl.getOrCreateToken(transaction.tokenWrite(), tokenName);
            key = transaction.tokenWrite().propertyKeyGetOrCreateForName(propertyKey);
            node = entityControl.createEntityWithTokenAndProp(
                    transaction.dataWrite(), token, key, Values.of(propertyValue));
            commit();
        }
        createConstraint(entityControl.schema(token, key));
        return node;
    }

    private long constrainedNode(String labelName, String propertyKey, Object propertyValue) throws KernelException {
        long node;
        int label;
        int key;
        {
            KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());
            label = transaction.tokenWrite().labelGetOrCreateForName(labelName);
            node = transaction.dataWrite().nodeCreate();
            transaction.dataWrite().nodeAddLabel(node, label);
            key = transaction.tokenWrite().propertyKeyGetOrCreateForName(propertyKey);
            transaction.dataWrite().nodeSetProperty(node, key, Values.of(propertyValue));
            commit();
        }
        createConstraint(forLabel(label, key));
        return node;
    }

    private ConstraintDescriptor createConstraint(SchemaDescriptor schema) throws KernelException {
        SchemaWrite schemaWrite = schemaWriteInNewTransaction();
        ConstraintDescriptor constraint = schemaWrite.uniquePropertyConstraintCreate(uniqueForSchema(schema));
        commit();

        return constraint;
    }

    private ConstraintDescriptor createNodeConstraint(String label, String propertyKey) throws KernelException {
        int labelId;
        int propertyKeyId;
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        labelId = tokenWrite.labelGetOrCreateForName(label);
        propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName(propertyKey);
        commit();

        SchemaWrite schemaWrite = schemaWriteInNewTransaction();
        ConstraintDescriptor constraint =
                schemaWrite.uniquePropertyConstraintCreate(uniqueForSchema(forLabel(labelId, propertyKeyId)));
        commit();

        return constraint;
    }

    private ConstraintDescriptor createRelationshipConstraint(String type, String propertyKey) throws KernelException {
        int labelId;
        int propertyKeyId;
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        labelId = tokenWrite.relationshipTypeGetOrCreateForName(type);
        propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName(propertyKey);
        commit();

        SchemaWrite schemaWrite = schemaWriteInNewTransaction();
        ConstraintDescriptor constraint =
                schemaWrite.uniquePropertyConstraintCreate(uniqueForSchema(forRelType(labelId, propertyKeyId)));
        commit();

        return constraint;
    }

    enum EntityControl {
        NODE {
            @Override
            int getOrCreateToken(TokenWrite tokenWrite, String tokenName) throws KernelException {
                return tokenWrite.labelGetOrCreateForName(tokenName);
            }

            @Override
            long createEntityWithToken(KernelTransaction tx, String tokenName) throws KernelException {
                int token = getOrCreateToken(tx.tokenWrite(), tokenName);
                Write write = tx.dataWrite();
                long node = write.nodeCreate();
                write.nodeAddLabel(node, token);
                return node;
            }

            @Override
            long createEntityWithTokenAndProp(Write write, int token, int propKey, Value value) throws KernelException {
                long node = write.nodeCreate();
                write.nodeAddLabel(node, token);
                write.nodeSetProperty(node, propKey, value);
                return node;
            }

            @Override
            long createEntityWithTokenAndProp(KernelTransaction tx, String token, String propKey, Value value)
                    throws KernelException {
                int tok = tx.tokenWrite().labelGetOrCreateForName(token);
                int prop = tx.tokenWrite().propertyKeyGetOrCreateForName(propKey);
                return createEntityWithTokenAndProp(tx.dataWrite(), tok, prop, value);
            }

            @Override
            void setProperty(Write write, long entity, int propKey, Value value) throws KernelException {
                write.nodeSetProperty(entity, propKey, value);
            }

            @Override
            void removeProperty(Write write, long entity, int propKey) throws KernelException {
                write.nodeRemoveProperty(entity, propKey);
            }

            @Override
            void deleteEntity(Write write, long entity) {
                write.nodeDelete(entity);
            }

            @Override
            int countEntities(KernelTransaction tx) {
                return KernelIntegrationTest.countNodes(tx);
            }

            @Override
            SchemaDescriptor schema(int token, int propKey) {
                return forLabel(token, propKey);
            }
        },
        RELATIONSHIP {
            @Override
            int getOrCreateToken(TokenWrite tokenWrite, String tokenName) throws KernelException {
                return tokenWrite.relationshipTypeGetOrCreateForName(tokenName);
            }

            @Override
            long createEntityWithToken(KernelTransaction tx, String tokenName) throws KernelException {
                int token = getOrCreateToken(tx.tokenWrite(), tokenName);
                Write write = tx.dataWrite();
                long node = write.nodeCreate();
                return write.relationshipCreate(node, token, node);
            }

            @Override
            long createEntityWithTokenAndProp(Write write, int token, int propKey, Value value) throws KernelException {
                long node = write.nodeCreate();
                long rel = write.relationshipCreate(node, token, node);
                write.relationshipSetProperty(rel, propKey, value);
                return rel;
            }

            @Override
            long createEntityWithTokenAndProp(KernelTransaction tx, String token, String propKey, Value value)
                    throws KernelException {
                int tok = tx.tokenWrite().relationshipTypeGetOrCreateForName(token);
                int prop = tx.tokenWrite().propertyKeyGetOrCreateForName(propKey);
                return createEntityWithTokenAndProp(tx.dataWrite(), tok, prop, value);
            }

            @Override
            void setProperty(Write write, long entity, int propKey, Value value) throws KernelException {
                write.relationshipSetProperty(entity, propKey, value);
            }

            @Override
            void removeProperty(Write write, long entity, int propKey) throws KernelException {
                write.relationshipRemoveProperty(entity, propKey);
            }

            @Override
            void deleteEntity(Write write, long entity) {
                write.relationshipDelete(entity);
            }

            @Override
            int countEntities(KernelTransaction tx) {
                return KernelIntegrationTest.countRelationships(tx);
            }

            @Override
            SchemaDescriptor schema(int token, int propKey) {
                return forRelType(token, propKey);
            }
        };

        abstract int getOrCreateToken(TokenWrite tokenWrite, String tokenName) throws KernelException;

        abstract long createEntityWithToken(KernelTransaction tx, String tokenName) throws KernelException;

        abstract long createEntityWithTokenAndProp(Write write, int token, int propKey, Value value)
                throws KernelException;

        abstract long createEntityWithTokenAndProp(KernelTransaction tx, String token, String propKey, Value value)
                throws KernelException;

        abstract void setProperty(Write write, long entity, int propKey, Value value) throws KernelException;

        abstract void removeProperty(Write write, long entity, int propKey) throws KernelException;

        abstract void deleteEntity(Write write, long entity);

        abstract int countEntities(KernelTransaction tx);

        abstract SchemaDescriptor schema(int token, int propKey);
    }
}
