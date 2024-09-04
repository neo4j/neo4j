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
package org.neo4j.kernel.impl.api.constraints;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.ResourceType;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.values.storable.Values;

class ConstraintIndexCreatorTest {
    private static final int PROPERTY_KEY_ID = 456;
    private static final int LABEL_ID = 123;
    private static final long INDEX_ID = 0L;

    private final IndexProviderDescriptor providerDescriptor = AllIndexProviderDescriptors.RANGE_DESCRIPTOR;
    private final LabelSchemaDescriptor schema = forLabel(LABEL_ID, PROPERTY_KEY_ID);
    private final UniquenessConstraintDescriptor constraint =
            ConstraintDescriptorFactory.uniqueForSchema(schema).withName("constraint");
    private final IndexPrototype prototype =
            IndexPrototype.uniqueForSchema(schema).withName("constraint").withIndexProvider(providerDescriptor);
    private final IndexDescriptor index = prototype.materialise(INDEX_ID);
    private final SchemaRead schemaRead = schemaRead();
    private final SchemaWrite schemaWrite = mock(SchemaWrite.class);
    private final TokenRead tokenRead = mock(TokenRead.class);
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private StubKernel kernel;

    @BeforeEach
    void setUp() throws Exception {
        kernel = new StubKernel();
        when(tokenRead.nodeLabelName(LABEL_ID)).thenReturn("Label");
        when(tokenRead.labelGetName(LABEL_ID)).thenReturn("Label");
        when(tokenRead.propertyKeyName(PROPERTY_KEY_ID)).thenReturn("prop");
        when(tokenRead.propertyKeyGetName(PROPERTY_KEY_ID)).thenReturn("prop");
    }

    @Test
    void shouldCreateIndexInAnotherTransaction() throws Exception {
        // given
        IndexProxy indexProxy = mock(IndexProxy.class);
        IndexingService indexingService = mock(IndexingService.class);
        when(indexingService.getIndexProxy(index)).thenReturn(indexProxy);
        when(indexProxy.getDescriptor()).thenReturn(index);
        when(schemaRead.indexGetForName(constraint.getName())).thenReturn(IndexDescriptor.NO_INDEX);
        ConstraintIndexCreator creator = new ConstraintIndexCreator(() -> kernel, indexingService, logProvider);

        // when
        IndexDescriptor constraintIndex =
                creator.createUniquenessConstraintIndex(createTransaction(), constraint, prototype, x -> {});

        // then
        assertEquals(INDEX_ID, constraintIndex.getId());
        verify(schemaRead).indexGetForName(constraint.getName());
        verifyNoMoreInteractions(schemaRead);
        verify(indexProxy).awaitStoreScanCompleted(anyLong(), any());
    }

    @Test
    void shouldDropIndexIfPopulationFails() throws Exception {
        // given
        IndexingService indexingService = mock(IndexingService.class);
        IndexProxy indexProxy = mock(IndexProxy.class);
        when(indexingService.getIndexProxy(index)).thenReturn(indexProxy);
        when(indexProxy.getDescriptor()).thenReturn(index);
        when(schemaRead.indexGetForName(constraint.getName())).thenReturn(IndexDescriptor.NO_INDEX, index);

        IndexEntryConflictException cause = new IndexEntryConflictException(index.schema(), 2, 1, Values.of("a"));
        doThrow(new IndexPopulationFailedKernelException("some index", cause))
                .when(indexProxy)
                .awaitStoreScanCompleted(anyLong(), any());
        when(schemaRead.index(any(SchemaDescriptor.class)))
                .thenReturn(Iterators.emptyResourceIterator()) // first claim it doesn't exist, because it doesn't... so
                // that it gets created
                .thenReturn(Iterators.iterator(index)); // then after it failed claim it does exist
        ConstraintIndexCreator creator = new ConstraintIndexCreator(() -> kernel, indexingService, logProvider);

        // when
        KernelTransactionImplementation transaction = createTransaction();
        UniquePropertyValueValidationException exception = assertThrows(
                UniquePropertyValueValidationException.class,
                () -> creator.createUniquenessConstraintIndex(transaction, constraint, prototype, x -> {}));
        assertEquals(
                "Existing data does not satisfy Constraint( name='constraint', type='UNIQUENESS', schema=(:Label {prop}) ): "
                        + "Both Node(2) and Node(1) have the label `Label[123]` and property `PropertyKey[456]` = 'a'",
                exception.getMessage());
        assertEquals(2, kernel.transactions.size());
        KernelTransactionImplementation tx1 = kernel.transactions.get(0);
        verify(tx1).indexUniqueCreate(prototype);
        verify(schemaRead, times(2)).indexGetForName(constraint.getName());
        verifyNoMoreInteractions(schemaRead);
        KernelTransactionImplementation kti2 = kernel.transactions.get(1);
        verify(kti2).addIndexDoDropToTxState(index);
    }

    @Test
    void shouldDropIndexInAnotherTransaction() throws Exception {
        // given
        IndexingService indexingService = mock(IndexingService.class);

        ConstraintIndexCreator creator = new ConstraintIndexCreator(() -> kernel, indexingService, logProvider);

        // when
        creator.dropUniquenessConstraintIndex(index);

        // then
        assertEquals(1, kernel.transactions.size());
        verify(kernel.transactions.get(0)).addIndexDoDropToTxState(index);
        verifyNoInteractions(indexingService);
    }

    @Test
    void shouldReleaseLabelLockWhileAwaitingIndexPopulation() throws Exception {
        // given
        IndexingService indexingService = mock(IndexingService.class);

        IndexProxy indexProxy = mock(IndexProxy.class);
        when(indexingService.getIndexProxy(index)).thenReturn(indexProxy);
        when(schemaRead.index(schema)).thenReturn(Iterators.emptyResourceIterator());
        when(schemaRead.indexGetForName(constraint.getName())).thenReturn(IndexDescriptor.NO_INDEX);

        ConstraintIndexCreator creator = new ConstraintIndexCreator(() -> kernel, indexingService, logProvider);

        // when
        KernelTransactionImplementation transaction = createTransaction();
        creator.createUniquenessConstraintIndex(transaction, constraint, prototype, x -> {});

        // then
        verify(transaction.lockClient()).releaseExclusive(ResourceType.LABEL, schema.getLabelId());

        verify(transaction.lockClient())
                .acquireExclusive(transaction.lockTracer(), ResourceType.LABEL, schema.getLabelId());
    }

    @Test
    void shouldThrowOnExistingOrphanedConstraintIndexWithSameName() throws Exception {
        // given
        IndexingService indexingService = mock(IndexingService.class);

        long orphanedConstraintIndexId = 111;
        String orphanedName = "constraint";
        IndexDescriptor orphanedIndex =
                IndexPrototype.uniqueForSchema(schema).withName(orphanedName).materialise(orphanedConstraintIndexId);
        IndexProxy indexProxy = mock(IndexProxy.class);
        when(indexingService.getIndexProxy(orphanedIndex)).thenReturn(indexProxy);
        when(schemaRead.index(schema)).thenReturn(Iterators.iterator(orphanedIndex));
        when(schemaRead.indexGetForName(orphanedName)).thenReturn(orphanedIndex);
        when(schemaRead.indexGetOwningUniquenessConstraintId(orphanedIndex))
                .thenReturn(null); // which means it has no owner
        ConstraintIndexCreator creator = new ConstraintIndexCreator(() -> kernel, indexingService, logProvider);

        // when
        KernelTransactionImplementation transaction = createTransaction();
        assertThrows(
                AlreadyConstrainedException.class,
                () -> creator.createUniquenessConstraintIndex(transaction, constraint, prototype, x -> {}));

        // then
        assertEquals(
                0,
                kernel.transactions.size(),
                "There should have been no need to acquire a statement to create the constraint index");
        verify(schemaRead).indexGetForName(constraint.getName());
        verifyNoMoreInteractions(schemaRead);
    }

    @Test
    void shouldIgnoreExistingOrphanedConstraintIndexWithDifferentName() throws Exception {
        // given
        IndexingService indexingService = mock(IndexingService.class);

        long orphanedConstraintIndexId = 111;
        String orphanedName = "blabla";
        IndexDescriptor orphanedIndex =
                IndexPrototype.uniqueForSchema(schema).withName(orphanedName).materialise(orphanedConstraintIndexId);
        IndexProxy indexProxy = mock(IndexProxy.class);
        when(indexingService.getIndexProxy(orphanedIndex)).thenReturn(indexProxy);
        when(indexingService.getIndexProxy(index)).thenReturn(indexProxy);
        when(schemaRead.index(schema)).thenReturn(Iterators.iterator(orphanedIndex));
        when(schemaRead.indexGetForName(constraint.getName())).thenReturn(IndexDescriptor.NO_INDEX);
        when(schemaRead.indexGetForName(orphanedName)).thenReturn(orphanedIndex);
        when(schemaRead.indexGetOwningUniquenessConstraintId(orphanedIndex))
                .thenReturn(null); // which means it has no owner
        ConstraintIndexCreator creator = new ConstraintIndexCreator(() -> kernel, indexingService, logProvider);

        // when
        KernelTransactionImplementation transaction = createTransaction();
        creator.createUniquenessConstraintIndex(transaction, constraint, prototype, x -> {});

        // then
        assertEquals(1, kernel.transactions.size());
        verify(schemaRead).indexGetForName(constraint.getName());
        verifyNoMoreInteractions(schemaRead);
    }

    @Test
    void shouldFailOnExistingOwnedConstraintIndex() {
        // given
        IndexingService indexingService = mock(IndexingService.class);

        long constraintIndexOwnerId = 222;
        when(schemaRead.index(schema)).thenReturn(Iterators.iterator(index));
        when(schemaRead.indexGetForName(constraint.getName())).thenReturn(index);
        when(schemaRead.indexGetOwningUniquenessConstraintId(index))
                .thenReturn(constraintIndexOwnerId); // which means there's an owner
        ConstraintIndexCreator creator = new ConstraintIndexCreator(() -> kernel, indexingService, logProvider);

        // when
        assertThrows(AlreadyConstrainedException.class, () -> {
            KernelTransactionImplementation transaction = createTransaction();
            creator.createUniquenessConstraintIndex(transaction, constraint, prototype, x -> {});
        });

        // then
        assertEquals(
                0,
                kernel.transactions.size(),
                "There should have been no need to acquire a statement to create the constraint index");
        verify(schemaRead).indexGetForName(constraint.getName());
        verifyNoMoreInteractions(schemaRead);
    }

    @Test
    void shouldCreateConstraintIndexForSpecifiedProvider() throws Exception {
        // given
        IndexingService indexingService = mock(IndexingService.class);

        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor("Groovy", "1.2");
        IndexPrototype prototype = this.prototype.withIndexProvider(providerDescriptor);
        IndexDescriptor index = prototype.materialise(this.index.getId());
        IndexProxy indexProxy = mock(IndexProxy.class);
        when(indexingService.getIndexProxy(index)).thenReturn(indexProxy);
        ConstraintIndexCreator creator = new ConstraintIndexCreator(() -> kernel, indexingService, logProvider);
        when(schemaRead.indexGetForName(constraint.getName())).thenReturn(IndexDescriptor.NO_INDEX);

        // when
        KernelTransactionImplementation transaction = createTransaction();
        creator.createUniquenessConstraintIndex(transaction, constraint, prototype, x -> {});

        // then
        assertEquals(1, kernel.transactions.size());
        KernelTransactionImplementation transactionInstance = kernel.transactions.get(0);
        verify(transactionInstance).indexUniqueCreate(prototype);
        verify(schemaRead).indexGetForName(constraint.getName());
        verifyNoMoreInteractions(schemaRead);
    }

    @Test
    void logMessagesAboutConstraintCreation() throws KernelException {
        IndexProxy indexProxy = mock(IndexProxy.class);
        IndexingService indexingService = mock(IndexingService.class);
        when(indexingService.getIndexProxy(index)).thenReturn(indexProxy);
        when(indexProxy.getDescriptor()).thenReturn(index);
        when(schemaRead.indexGetForName(constraint.getName())).thenReturn(IndexDescriptor.NO_INDEX);
        ConstraintIndexCreator creator = new ConstraintIndexCreator(() -> kernel, indexingService, logProvider);
        KernelTransactionImplementation transaction = createTransaction();

        creator.createUniquenessConstraintIndex(transaction, constraint, prototype, x -> {});

        String constraintString = constraint.userDescription(tokenRead);
        assertThat(logProvider)
                .containsMessages(
                        format("Starting constraint creation: %s.", constraintString),
                        format("Constraint %s populated, starting verification.", constraintString),
                        format("Constraint %s verified.", constraintString));
    }

    private class StubKernel implements Kernel {
        private final List<KernelTransactionImplementation> transactions = new ArrayList<>();

        private KernelTransaction remember(KernelTransactionImplementation kernelTransaction) {
            transactions.add(kernelTransaction);
            return kernelTransaction;
        }

        @Override
        public KernelTransaction beginTransaction(
                KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo clientInfo, long timeout) {
            return remember(createTransaction());
        }

        @Override
        public KernelTransaction beginTransaction(
                KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo connectionInfo) {
            return remember(createTransaction());
        }

        @Override
        public KernelTransaction beginTransaction(KernelTransaction.Type type, LoginContext loginContext) {
            return remember(createTransaction());
        }

        @Override
        public void registerProcedure(CallableProcedure procedure) {}

        @Override
        public void registerUserFunction(CallableUserFunction function) {}

        @Override
        public void registerUserAggregationFunction(CallableUserAggregationFunction function) {}

        @Override
        public CursorFactory cursors() {
            throw new UnsupportedOperationException("not implemented");
        }
    }

    private SchemaRead schemaRead() {
        SchemaRead schemaRead = mock(SchemaRead.class);
        when(schemaRead.index(schema)).thenReturn(Iterators.emptyResourceIterator());
        return schemaRead;
    }

    private KernelTransactionImplementation createTransaction() {
        KernelTransactionImplementation transaction = mock(KernelTransactionImplementation.class);
        try {
            StorageEngine storageEngine = mock(StorageEngine.class);
            StorageReader storageReader = mock(StorageReader.class);
            when(storageEngine.newReader()).thenReturn(storageReader);

            LockManager.Client locks = mock(LockManager.Client.class);
            when(transaction.lockClient()).thenReturn(locks);
            when(transaction.tokenRead()).thenReturn(tokenRead);
            when(transaction.schemaRead()).thenReturn(schemaRead);
            when(transaction.schemaWrite()).thenReturn(schemaWrite);
            TransactionState transactionState = mock(TransactionState.class);
            when(transaction.txState()).thenReturn(transactionState);
            when(transaction.indexUniqueCreate(any(IndexPrototype.class)))
                    .thenAnswer(i -> i.<IndexPrototype>getArgument(0).materialise(INDEX_ID));
            when(transaction.newStorageReader()).thenReturn(mock(StorageReader.class));
        } catch (InvalidTransactionTypeKernelException e) {
            fail("Expected write transaction");
        }
        return transaction;
    }
}
