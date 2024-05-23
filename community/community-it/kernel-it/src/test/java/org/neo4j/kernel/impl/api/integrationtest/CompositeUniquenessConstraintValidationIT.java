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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptors.forRelType;

import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.Values;

@ImpermanentDbmsExtension
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CompositeUniquenessConstraintValidationIT {
    @Inject
    private GraphDatabaseAPI db;

    private ConstraintDescriptor constraintDescriptor;
    private int label;
    private int type;
    private int[] propIds;
    private KernelTransaction transaction;
    protected Kernel kernel;

    public static Stream<Arguments> parameterValues() {
        return Stream.of(
                Arguments.of(values(10), values(10d)),
                Arguments.of(values(10, 20), values(10, 20)),
                Arguments.of(values(10L, 20L), values(10, 20)),
                Arguments.of(values(10, 20), values(10L, 20L)),
                Arguments.of(values(10, 20), values(10.0, 20.0)),
                Arguments.of(values(10, 20), values(10.0, 20.0)),
                Arguments.of(values(new int[] {1, 2}, "v2"), values(new int[] {1, 2}, "v2")),
                Arguments.of(values("a", "b", "c"), values("a", "b", "c")),
                Arguments.of(values(285414114323346805L), values(285414114323346805L)),
                Arguments.of(values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), values(1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d)));
    }

    private static Object[] values(Object... values) {
        return values;
    }

    @BeforeEach
    public void setup() throws Exception {
        kernel = db.getDependencyResolver().resolveDependency(Kernel.class);

        newTransaction();
        // This transaction allocates all the tokens we'll need in this test.
        TokenWrite tokenWrite = transaction.tokenWrite();
        tokenWrite.labelGetOrCreateForName("Label0");
        label = tokenWrite.labelGetOrCreateForName("Label1");
        type = tokenWrite.relationshipTypeGetOrCreateForName("Type1");
        propIds = new int[10];
        for (int i = 0; i < propIds.length; i++) {
            propIds[i] = tokenWrite.propertyKeyGetOrCreateForName("prop" + i);
        }
        commit();
    }

    private void setupConstraintDescriptor(int nbrOfProperties) throws KernelException {
        newTransaction();
        constraintDescriptor = transaction
                .schemaWrite()
                .uniquePropertyConstraintCreate(
                        IndexPrototype.uniqueForSchema(forLabel(label, propertyIds(nbrOfProperties))));
        commit();
    }

    private void setupRelConstraintDescriptor(int nbrOfProperties) throws KernelException {
        newTransaction();
        constraintDescriptor = transaction
                .schemaWrite()
                .uniquePropertyConstraintCreate(
                        IndexPrototype.uniqueForSchema(forRelType(type, propertyIds(nbrOfProperties))));
        commit();
    }

    @AfterEach
    public void clean() throws Exception {
        if (transaction != null) {
            transaction.close();
            transaction = null;
        }

        newTransaction();
        transaction.schemaWrite().constraintDrop(constraintDescriptor, false);
        commit();

        try (KernelTransaction tx =
                kernel.beginTransaction(KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED)) {
            try (RelationshipScanCursor rels = tx.cursors().allocateRelationshipScanCursor(tx.cursorContext())) {
                tx.dataRead().allRelationshipsScan(rels);
                while (rels.next()) {
                    tx.dataWrite().relationshipDelete(rels.relationshipReference());
                }
            }
            try (NodeCursor node = tx.cursors().allocateNodeCursor(tx.cursorContext())) {
                tx.dataRead().allNodesScan(node);
                while (node.next()) {
                    tx.dataWrite().nodeDelete(node.nodeReference());
                }
            }
            tx.commit();
        }
    }

    @ParameterizedTest(name = "{0}: lhs={1}, rhs={2}")
    @MethodSource("parameterValues")
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_DeleteNode(Object[] lhs, Object[] rhs)
            throws Exception {
        setupConstraintDescriptor(lhs.length);

        // given
        long node = createNodeWithLabelAndProps(label, lhs);

        // when
        newTransaction();
        transaction.dataWrite().nodeDelete(node);
        long newNode = createLabeledNode(label);
        setProperties(newNode, lhs);

        // then does not fail
        commit();
    }

    @ParameterizedTest(name = "{0}: lhs={1}, rhs={2}")
    @MethodSource("parameterValues")
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_DeleteRelationship(Object[] lhs, Object[] rhs)
            throws Exception {
        setupRelConstraintDescriptor(lhs.length);

        // given
        long rel = createRelWithProps(type, lhs);

        // when
        newTransaction();
        transaction.dataWrite().relationshipDelete(rel);
        createRelWithPropsInCurrentTx(type, lhs);

        // then does not fail
        commit();
    }

    @ParameterizedTest(name = "{0}: lhs={1}, rhs={2}")
    @MethodSource("parameterValues")
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_RemoveLabel(Object[] lhs, Object[] rhs)
            throws Exception {
        setupConstraintDescriptor(lhs.length);

        // given
        long node = createNodeWithLabelAndProps(label, lhs);

        // when
        newTransaction();
        transaction.dataWrite().nodeRemoveLabel(node, label);
        long newNode = createLabeledNode(label);
        setProperties(newNode, lhs);

        // then does not fail
        commit();
    }

    @ParameterizedTest(name = "{0}: lhs={1}, rhs={2}")
    @MethodSource("parameterValues")
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_RemoveProperty(Object[] lhs, Object[] rhs)
            throws Exception {
        setupConstraintDescriptor(lhs.length);

        // given
        long node = createNodeWithLabelAndProps(label, lhs);

        // when
        newTransaction();
        transaction.dataWrite().nodeRemoveProperty(node, propIds[0]);
        long newNode = createLabeledNode(label);
        setProperties(newNode, lhs);

        // then does not fail
        commit();
    }

    @ParameterizedTest(name = "{0}: lhs={1}, rhs={2}")
    @MethodSource("parameterValues")
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_RemoveRelProperty(Object[] lhs, Object[] rhs)
            throws Exception {
        setupRelConstraintDescriptor(lhs.length);

        // given
        long rel = createRelWithProps(type, lhs);

        // when
        newTransaction();
        transaction.dataWrite().relationshipRemoveProperty(rel, propIds[0]);
        createRelWithPropsInCurrentTx(type, lhs);

        // then does not fail
        commit();
    }

    @ParameterizedTest(name = "{0}: lhs={1}, rhs={2}")
    @MethodSource("parameterValues")
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_ChangeProperty(Object[] lhs, Object[] rhs)
            throws Exception {
        setupConstraintDescriptor(lhs.length);

        // given
        long node = createNodeWithLabelAndProps(label, lhs);

        // when
        newTransaction();
        transaction.dataWrite().nodeSetProperty(node, propIds[0], Values.of("Alive!"));
        long newNode = createLabeledNode(label);
        setProperties(newNode, lhs);

        // then does not fail
        commit();
    }

    @ParameterizedTest(name = "{0}: lhs={1}, rhs={2}")
    @MethodSource("parameterValues")
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_ChangeRelProperty(Object[] lhs, Object[] rhs)
            throws Exception {
        setupRelConstraintDescriptor(lhs.length);

        // given
        long rel = createRelWithProps(type, lhs);

        // when
        newTransaction();
        transaction.dataWrite().relationshipSetProperty(rel, propIds[0], Values.of("Alive!"));
        createRelWithPropsInCurrentTx(type, lhs);

        // then does not fail
        commit();
    }

    @ParameterizedTest(name = "{0}: lhs={1}, rhs={2}")
    @MethodSource("parameterValues")
    public void shouldPreventConflictingDataInTx(Object[] lhs, Object[] rhs) throws Throwable {
        setupConstraintDescriptor(lhs.length);

        // Given

        // When
        newTransaction();
        long n1 = createLabeledNode(label);
        long n2 = createLabeledNode(label);
        setProperties(n1, lhs);
        int lastPropertyOffset = lhs.length - 1;
        setProperties(n2, lhs, lastPropertyOffset);

        assertThatThrownBy(() -> setProperty(n2, propIds[lastPropertyOffset], lhs[lastPropertyOffset]))
                .isInstanceOf(UniquePropertyValueValidationException.class);

        // Then should fail
        commit();
    }

    @ParameterizedTest(name = "{0}: lhs={1}, rhs={2}")
    @MethodSource("parameterValues")
    public void shouldPreventConflictingDataInTx_Rels(Object[] lhs, Object[] rhs) throws Throwable {
        setupRelConstraintDescriptor(lhs.length);

        newTransaction();
        long r1 = createRel(type);
        long r2 = createRel(type);
        setRelProperties(r1, lhs);
        int lastPropertyOffset = lhs.length - 1;
        setRelProperties(r2, lhs, lastPropertyOffset);

        assertThatThrownBy(() -> setRelProperty(r2, propIds[lastPropertyOffset], lhs[lastPropertyOffset]))
                .isInstanceOf(UniquePropertyValueValidationException.class);

        // Then should fail
        commit();
    }

    @ParameterizedTest(name = "{0}: lhs={1}, rhs={2}")
    @MethodSource("parameterValues")
    public void shouldEnforceOnSetProperty(Object[] lhs, Object[] rhs) throws Exception {
        setupConstraintDescriptor(lhs.length);

        // given
        createNodeWithLabelAndProps(label, lhs);

        // when
        newTransaction();
        long node = createLabeledNode(label);
        int lastPropertyOffset = lhs.length - 1;
        setProperties(node, lhs, lastPropertyOffset);

        assertThatThrownBy(() -> setProperty(node, propIds[lastPropertyOffset], lhs[lastPropertyOffset]))
                .isInstanceOf(UniquePropertyValueValidationException.class);
        commit();
    }

    @ParameterizedTest(name = "{0}: lhs={1}, rhs={2}")
    @MethodSource("parameterValues")
    public void shouldEnforceOnSetRelProperty(Object[] lhs, Object[] rhs) throws Exception {
        setupRelConstraintDescriptor(lhs.length);

        // given
        createRelWithProps(type, lhs);

        // when
        newTransaction();
        long rel = createRel(type);
        int lastPropertyOffset = lhs.length - 1;
        setRelProperties(rel, lhs, lastPropertyOffset);

        assertThatThrownBy(() -> setRelProperty(rel, propIds[lastPropertyOffset], lhs[lastPropertyOffset]))
                .isInstanceOf(UniquePropertyValueValidationException.class);
        commit();
    }

    @ParameterizedTest(name = "{0}: lhs={1}, rhs={2}")
    @MethodSource("parameterValues")
    public void shouldEnforceOnSetLabel(Object[] lhs, Object[] rhs) throws Exception {
        setupConstraintDescriptor(lhs.length);

        // given
        createNodeWithLabelAndProps(label, lhs);

        // when
        newTransaction();
        long node = createNode();
        setProperties(node, rhs); // ok because no label is set

        assertThatThrownBy(() -> addLabel(node, label)).isInstanceOf(UniquePropertyValueValidationException.class);
        commit();
    }

    @ParameterizedTest(name = "{0}: lhs={1}, rhs={2}")
    @MethodSource("parameterValues")
    public void shouldEnforceOnSetPropertyInTx(Object[] lhs, Object[] rhs) throws Exception {
        setupConstraintDescriptor(lhs.length);

        // when
        newTransaction();
        long aNode = createLabeledNode(label);
        setProperties(aNode, lhs);

        long nodeB = createLabeledNode(label);
        int lastPropertyOffset = lhs.length - 1;
        setProperties(nodeB, rhs, lastPropertyOffset);

        assertThatThrownBy(() -> setProperty(nodeB, propIds[lastPropertyOffset], rhs[lastPropertyOffset]))
                .isInstanceOf(UniquePropertyValueValidationException.class);
        commit();
    }

    @ParameterizedTest(name = "{0}: lhs={1}, rhs={2}")
    @MethodSource("parameterValues")
    public void shouldEnforceOnSetRelPropertyInTx(Object[] lhs, Object[] rhs) throws Exception {
        setupRelConstraintDescriptor(lhs.length);

        // when
        newTransaction();
        long relA = createRelWithPropsInCurrentTx(type, lhs);

        long relB = createRel(type);
        int lastPropertyOffset = lhs.length - 1;
        setRelProperties(relB, rhs, lastPropertyOffset);

        assertThatThrownBy(() -> setRelProperty(relB, propIds[lastPropertyOffset], rhs[lastPropertyOffset]))
                .isInstanceOf(UniquePropertyValueValidationException.class);
        commit();
    }

    @ParameterizedTest(name = "{0}: lhs={1}, rhs={2}")
    @MethodSource("parameterValues")
    public void shouldEnforceOnSetLabelInTx(Object[] lhs, Object[] rhs) throws Exception {
        setupConstraintDescriptor(lhs.length);

        // given
        createNodeWithLabelAndProps(label, lhs);

        // when
        newTransaction();
        long nodeB = createNode();
        setProperties(nodeB, rhs);

        assertThatThrownBy(() -> addLabel(nodeB, label)).isInstanceOf(UniquePropertyValueValidationException.class);
        commit();
    }

    private void newTransaction() throws KernelException {
        assertThat(transaction).as("tx already opened").isNull();
        transaction = kernel.beginTransaction(KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED);
    }

    protected void commit() throws TransactionFailureException {
        transaction.commit();
        transaction = null;
    }

    private long createLabeledNode(int labelId) throws KernelException {
        long node = transaction.dataWrite().nodeCreate();
        transaction.dataWrite().nodeAddLabel(node, labelId);
        return node;
    }

    private void addLabel(long nodeId, int labelId) throws KernelException {
        transaction.dataWrite().nodeAddLabel(nodeId, labelId);
    }

    private void setProperty(long nodeId, int propertyId, Object value) throws KernelException {
        transaction.dataWrite().nodeSetProperty(nodeId, propertyId, Values.of(value));
    }

    private void setRelProperty(long relId, int propertyId, Object value) throws KernelException {
        transaction.dataWrite().relationshipSetProperty(relId, propertyId, Values.of(value));
    }

    private long createNode() throws KernelException {
        return transaction.dataWrite().nodeCreate();
    }

    private long createRel(int typeId) throws KernelException {
        long nodeId = createNode();
        return transaction.dataWrite().relationshipCreate(nodeId, type, nodeId);
    }

    private long createNodeWithLabelAndProps(int labelId, Object[] propertyValues) throws KernelException {
        newTransaction();
        long nodeId = createNode();
        addLabel(nodeId, labelId);
        setProperties(nodeId, propertyValues);
        commit();
        return nodeId;
    }

    private long createRelWithPropsInCurrentTx(int typeId, Object[] propertyValues) throws KernelException {
        long nodeId = createNode();
        long relId = transaction.dataWrite().relationshipCreate(nodeId, type, nodeId);
        setRelProperties(relId, propertyValues);
        return relId;
    }

    private long createRelWithProps(int typeId, Object[] propertyValues) throws KernelException {
        newTransaction();
        long nodeId = createNode();
        long relId = transaction.dataWrite().relationshipCreate(nodeId, type, nodeId);
        setRelProperties(relId, propertyValues);
        commit();
        return relId;
    }

    private void setProperties(long nodeId, Object[] propertyValues) throws KernelException {
        setProperties(nodeId, propertyValues, propertyValues.length);
    }

    private void setRelProperties(long relId, Object[] propertyValues) throws KernelException {
        setRelProperties(relId, propertyValues, propertyValues.length);
    }

    private void setProperties(long nodeId, Object[] propertyValues, int numOfValues) throws KernelException {
        assertThat(numOfValues).isLessThanOrEqualTo(propIds.length);
        for (int i = 0; i < numOfValues; i++) {
            setProperty(nodeId, propIds[i], propertyValues[i]);
        }
    }

    private void setRelProperties(long relId, Object[] propertyValues, int numOfValues) throws KernelException {
        assertThat(numOfValues).isLessThanOrEqualTo(propIds.length);
        for (int i = 0; i < numOfValues; i++) {
            setRelProperty(relId, propIds[i], propertyValues[i]);
        }
    }

    private int[] propertyIds(int numberOfProps) {
        assertThat(numberOfProps).isLessThanOrEqualTo(propIds.length);
        return Arrays.copyOf(propIds, numberOfProps);
    }
}
