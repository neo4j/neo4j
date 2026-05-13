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
package org.neo4j.kernel.impl.api.state;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException.Phase.VERIFICATION;
import static org.neo4j.internal.kernel.api.security.SecurityContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.KernelTransaction.Type;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.TransactionTerminatedHelper;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.IncompleteConstraintValidationException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.locking.LockManager.Client;
import org.neo4j.lock.ResourceType;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

public class ConstraintIndexCreator {
    private final IndexingService indexingService;
    private final Supplier<Kernel> kernelSupplier;
    protected final InternalLog log;

    public ConstraintIndexCreator(
            Supplier<Kernel> kernelSupplier, IndexingService indexingService, InternalLogProvider logProvider) {
        this.kernelSupplier = kernelSupplier;
        this.indexingService = indexingService;
        this.log = logProvider.getLog(ConstraintIndexCreator.class);
    }

    public Supplier<Kernel> getKernelSupplier() {
        return kernelSupplier;
    }

    @FunctionalInterface
    public interface PropertyExistenceEnforcer {
        void existenceEnforcement(SchemaDescriptor schemaDescriptor) throws KernelException;
    }

    /**
     * You MUST hold a label/type write lock before you call this method.
     * However the label/type write lock is temporarily released while populating the index backing the constraint.
     * It goes a little like this:
     * <ol>
     * <li>Prerequisite: Getting here means that there's an open schema transaction which has acquired the
     * LABEL/TYPE WRITE lock.</li>
     * <li>Index schema rule which is backing the constraint is created in a nested mini-transaction
     * which doesn't acquire any locking, merely adds tx state and commits so that the index rule is applied
     * to the store, which triggers the index population</li>
     * <li>Release the LABEL/TYPE WRITE lock</li>
     * <li>Await index population to complete</li>
     * <li>Acquire the LABEL/TYPE WRITE lock (effectively blocking concurrent transactions changing
     * data related to this constraint, and it so happens, most other transactions as well) and verify
     * the uniqueness of the built index</li>
     * <li>Verify property existence (if required)</li>
     * <li>Leave this method, knowing that the uniqueness constraint rule will be added to tx state
     * and this tx committed, which will create the uniqueness constraint</li>
     * </ol>
     */
    public IndexDescriptor createUniquenessConstraintIndex(
            KernelTransactionImplementation transaction,
            IndexBackedConstraintDescriptor constraint,
            IndexPrototype prototype,
            PropertyExistenceEnforcer propertyExistenceEnforcer)
            throws KernelException {
        String constraintString = constraint.userDescription(transaction.tokenRead());
        log.debug("Starting constraint creation: %s.", constraintString);

        IndexDescriptor index;
        SchemaRead schemaRead = transaction.schemaRead();
        try {
            index = checkAndCreateConstraintIndex(schemaRead, transaction.tokenRead(), constraint, prototype);
        } catch (AlreadyConstrainedException e) {
            throw e;
        } catch (TransactionFailureException e) {
            // Transient errors (e.g. LeaseExpired) are retryable by drivers — don't bury them
            // inside a CreateConstraintFailureException (a database error), or the retry is lost.
            // Non-transient transaction failures stay wrapped as before.
            if (e.status().code().classification() == Status.Classification.TransientError) {
                throw e;
            }
            throw CreateConstraintFailureException.constraintCreationFailed(constraint, transaction.tokenRead(), e);
        } catch (KernelException e) {
            throw CreateConstraintFailureException.constraintCreationFailed(constraint, transaction.tokenRead(), e);
        }

        // At this point we should have a populating index created from an internal transaction
        boolean success = false;
        boolean reacquiredLock = false;
        Client locks = transaction.lockClient();
        ResourceType keyType = constraint.schema().keyType();
        long[] lockingKeys = constraint.schema().lockingKeys();
        try {
            try {
                locks.acquireShared(transaction.lockTracer(), keyType, lockingKeys);
                IndexProxy proxy = indexingService.getIndexProxy(index);

                // Release the LABEL/TYPE WRITE lock during index population.
                // At this point the integrity of the constraint to be created was checked
                // while holding the lock and the index rule backing the soon-to-be-created constraint
                // has been created. Now it's just the population left, which can take a long time
                locks.releaseExclusive(keyType, lockingKeys);

                awaitConstraintIndexPopulation(constraint, proxy, transaction, index);
                log.debug("Constraint %s populated, starting verification.", constraintString);

                // Index population was successful, but at this point we don't know if the uniqueness constraint holds.
                // Acquire LABEL/TYPE WRITE lock and verify the constraints here in this user transaction
                // and if everything checks out then it will be held until after the constraint has been
                // created and activated.
                locks.acquireExclusive(transaction.lockTracer(), keyType, lockingKeys);
                reacquiredLock = true;

                validateIndex(index, constraint, transaction);
            } catch (IncompleteConstraintValidationException e) {
                throw e.turnIntoRealException(constraint, transaction.tokenRead());
            } catch (IndexNotFoundKernelException e) {
                String indexString = index.userDescription(transaction.tokenRead());
                throw TransactionFailureException.internalError(
                        this.getClass().getSimpleName(),
                        format("Index (%s) that we just created does not exist.", indexString),
                        e);
            } catch (IndexPopulationFailedKernelException | InterruptedException e) {
                throw CreateConstraintFailureException.constraintCreationFailed(constraint, transaction.tokenRead(), e);
            }

            propertyExistenceEnforcer.existenceEnforcement(index.schema());

            log.debug("Constraint %s verified.", constraintString);
            success = true;
            return index;
        } finally {
            if (!success) {
                try {
                    // A terminating transaction will have released its locks and can't take any new.
                    // It can't even check if the index exist since that require the tx to still be open
                    // Do all checks in a new transaction for this case
                    if (transaction.isTerminated()) {
                        try (KernelTransaction dropTransaction =
                                kernelSupplier.get().beginTransaction(Type.IMPLICIT, AUTH_DISABLED)) {
                            if (indexStillExists(dropTransaction.schemaRead(), index)) {
                                // Need to take exclusive for drop since it has been released
                                dropTransaction.schemaWrite().indexDrop(index);
                            }
                            dropTransaction.commit();
                        }
                    } else {
                        if (!reacquiredLock) {
                            locks.acquireExclusive(transaction.lockTracer(), keyType, lockingKeys);
                        }

                        if (indexStillExists(schemaRead, index)) {
                            dropUniquenessConstraintIndex(index);
                        }
                    }
                } catch (Exception e) {
                    log.error("Error while removing index created for failed constraint creation '" + index.getName()
                            + "'. " + e);
                }
            }
        }
    }

    protected void validateIndex(
            IndexDescriptor index,
            IndexBackedConstraintDescriptor constraint,
            KernelTransactionImplementation transaction)
            throws KernelException, IncompleteConstraintValidationException {
        indexingService.getIndexProxy(index).validate();
    }

    private static boolean indexStillExists(SchemaRead schemaRead, IndexDescriptor index) {
        IndexDescriptor existingIndex = schemaRead.indexGetForName(index.getName());
        return existingIndex != IndexDescriptor.NO_INDEX && existingIndex.equals(index);
    }

    /**
     * You MUST hold a schema write lock before you call this method.
     */
    public void dropUniquenessConstraintIndex(IndexDescriptor index) throws TransactionFailureException {
        try (KernelTransaction transaction = kernelSupplier.get().beginTransaction(Type.IMPLICIT, AUTH_DISABLED)) {
            ((KernelTransactionImplementation) transaction).addIndexDoDropToTxState(index);
            transaction.commit();
        }
    }

    protected void awaitConstraintIndexPopulation(
            IndexBackedConstraintDescriptor constraint,
            IndexProxy proxy,
            KernelTransactionImplementation transaction,
            IndexDescriptor index)
            throws InterruptedException, UniquePropertyValueValidationException, IndexNotFoundKernelException {
        try {
            boolean stillGoing;
            do {
                // Transaction version reset also resets highest gap free for this transaction, which allows global
                // visibility horizon to move forward
                // Which, in turn, allows index population to complete in multiversion database, because it waits for
                // horizon being above certain threshold
                transaction.schemaTransactionVersionReset();
                stillGoing = proxy.awaitStoreScanCompleted(1, TimeUnit.SECONDS);
                if (transaction.isTerminated()) {
                    Optional<Status> reasonIfTerminated = transaction.getReasonIfTerminated();
                    assert reasonIfTerminated.isPresent();
                    throw TransactionTerminatedHelper.transactionTerminated(reasonIfTerminated.get());
                }
            } while (stillGoing);
        } catch (IndexPopulationFailedKernelException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IndexEntryConflictException exc) {
                throw UniquePropertyValueValidationException.propertyUniquenessViolation(
                        constraint, VERIFICATION, exc, transaction.tokenRead());
            } else if (cause instanceof IllegalArgumentException exc) {
                throw UniquePropertyValueValidationException.propertyUniquenessViolation(
                        constraint, VERIFICATION, exc, transaction.tokenRead());
            } else {
                throw UniquePropertyValueValidationException.propertyUniquenessViolation(
                        constraint, VERIFICATION, e, transaction.tokenRead());
            }
        }
    }

    private IndexDescriptor checkAndCreateConstraintIndex(
            SchemaRead schemaRead,
            TokenNameLookup tokenLookup,
            IndexBackedConstraintDescriptor constraint,
            IndexPrototype prototype)
            throws KernelException {
        IndexDescriptor descriptor = schemaRead.indexGetForName(constraint.getName());
        if (descriptor != IndexDescriptor.NO_INDEX) {
            if (descriptor.isUnique()) {
                // Looks like there is already a constraint like this.
                throw AlreadyConstrainedException.cannotCreateConstraint(constraint, tokenLookup);
            }
            // There's already an index for the schema of this constraint, which isn't of the type we're after.
            throw AlreadyIndexedException.cannotCreateConstraint(constraint.schema(), tokenLookup);
        }
        return createConstraintIndex(prototype);
    }

    public IndexDescriptor createConstraintIndex(IndexPrototype prototype) throws KernelException {
        try (KernelTransaction transaction = kernelSupplier.get().beginTransaction(Type.IMPLICIT, AUTH_DISABLED)) {
            IndexDescriptor index = transaction.indexUniqueCreate(prototype);
            transaction.commit();
            return index;
        }
    }
}
