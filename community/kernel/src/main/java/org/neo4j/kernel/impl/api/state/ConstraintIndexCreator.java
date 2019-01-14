/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.state;

import java.io.IOException;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.Session;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException.OperationContext;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor.Type;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.store.DefaultCapableIndexReference;
import org.neo4j.kernel.impl.locking.Locks.Client;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException.Phase.VERIFICATION;
import static org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException.OperationContext.CONSTRAINT_CREATION;
import static org.neo4j.internal.kernel.api.security.SecurityContext.AUTH_DISABLED;
import static org.neo4j.kernel.impl.locking.ResourceTypes.LABEL;

public class ConstraintIndexCreator
{
    private final IndexingService indexingService;
    private final Supplier<Kernel> kernelSupplier;
    private final PropertyAccessor propertyAccessor;
    private final Log log;

    public ConstraintIndexCreator( Supplier<Kernel> kernelSupplier, IndexingService indexingService, PropertyAccessor propertyAccessor,
            LogProvider logProvider )
    {
        this.kernelSupplier = kernelSupplier;
        this.indexingService = indexingService;
        this.propertyAccessor = propertyAccessor;
        this.log = logProvider.getLog( ConstraintIndexCreator.class );
    }

    /**
     * You MUST hold a label write lock before you call this method.
     * However the label write lock is temporarily released while populating the index backing the constraint.
     * It goes a little like this:
     * <ol>
     * <li>Prerequisite: Getting here means that there's an open schema transaction which has acquired the
     * LABEL WRITE lock.</li>
     * <li>Index schema rule which is backing the constraint is created in a nested mini-transaction
     * which doesn't acquire any locking, merely adds tx state and commits so that the index rule is applied
     * to the store, which triggers the index population</li>
     * <li>Release the LABEL WRITE lock</li>
     * <li>Await index population to complete</li>
     * <li>Acquire the LABEL WRITE lock (effectively blocking concurrent transactions changing
     * data related to this constraint, and it so happens, most other transactions as well) and verify
     * the uniqueness of the built index</li>
     * <li>Leave this method, knowing that the uniqueness constraint rule will be added to tx state
     * and this tx committed, which will create the uniqueness constraint</li>
     * </ol>
     *
     * Btw providerDescriptor is allowed to be null, where default configured will be used.
     */
    public long createUniquenessConstraintIndex( KernelTransactionImplementation transaction,
            SchemaDescriptor descriptor, @Nullable IndexProvider.Descriptor providerDescriptor )
            throws TransactionFailureException, CreateConstraintFailureException, UniquePropertyValueValidationException, AlreadyConstrainedException
    {
        UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForSchema( descriptor );
        log.info( "Starting constraint creation: %s.", constraint.ownedIndexDescriptor() );

        CapableIndexReference index;
        SchemaRead schemaRead = transaction.schemaRead();
        try
        {
            index = getOrCreateUniquenessConstraintIndex( schemaRead, transaction.tokenRead(), descriptor, providerDescriptor );
        }
        catch ( AlreadyConstrainedException e )
        {
            throw e;
        }
        catch ( SchemaKernelException | IndexNotFoundKernelException e )
        {
            throw new CreateConstraintFailureException( constraint, e );
        }

        boolean success = false;
        boolean reacquiredLabelLock = false;
        Client locks = transaction.statementLocks().pessimistic();
        try
        {
            long indexId = schemaRead.indexGetCommittedId( index );
            IndexProxy proxy = indexingService.getIndexProxy( indexId );

            // Release the LABEL WRITE lock during index population.
            // At this point the integrity of the constraint to be created was checked
            // while holding the lock and the index rule backing the soon-to-be-created constraint
            // has been created. Now it's just the population left, which can take a long time
            releaseLabelLock( locks, descriptor.keyId() );

            awaitConstrainIndexPopulation( constraint, proxy );
            log.info( "Constraint %s populated, starting verification.", constraint.ownedIndexDescriptor() );

            // Index population was successful, but at this point we don't know if the uniqueness constraint holds.
            // Acquire LABEL WRITE lock and verify the constraints here in this user transaction
            // and if everything checks out then it will be held until after the constraint has been
            // created and activated.
            acquireLabelLock( transaction, locks, descriptor.keyId() );
            reacquiredLabelLock = true;

            indexingService.getIndexProxy( indexId ).verifyDeferredConstraints( propertyAccessor );
            log.info( "Constraint %s verified.", constraint.ownedIndexDescriptor() );

            success = true;
            return indexId;
        }
        catch ( SchemaKernelException e )
        {
            throw new IllegalStateException(
                    String.format( "Index (%s) that we just created does not exist.", descriptor ), e );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new TransactionFailureException(
                    String.format( "Index (%s) that we just created does not exist.", descriptor ), e );
        }
        catch ( IndexEntryConflictException e )
        {
            throw new UniquePropertyValueValidationException( constraint, VERIFICATION, e );
        }
        catch ( InterruptedException | IOException e )
        {
            throw new CreateConstraintFailureException( constraint, e );
        }
        finally
        {
            if ( !success )
            {
                if ( !reacquiredLabelLock )
                {
                    acquireLabelLock( transaction, locks, descriptor.keyId() );
                }

                if ( indexStillExists( schemaRead, descriptor, index ) )
                {
                    dropUniquenessConstraintIndex( asDescriptor( index ) );
                }
            }
        }
    }

    private boolean indexStillExists( SchemaRead schemaRead, SchemaDescriptor descriptor,
            CapableIndexReference index )
    {
        CapableIndexReference existingIndex = schemaRead.index( descriptor.keyId(), descriptor.getPropertyIds() );
        return existingIndex != CapableIndexReference.NO_INDEX && existingIndex.equals( index );
    }

    private void acquireLabelLock( KernelTransactionImplementation state, Client locks, int labelId )
    {
        locks.acquireExclusive( state.lockTracer(), LABEL, labelId );
    }

    private void releaseLabelLock( Client locks, int labelId )
    {
        locks.releaseExclusive( LABEL, labelId );
    }

    /**
     * You MUST hold a schema write lock before you call this method.
     */
    public void dropUniquenessConstraintIndex( SchemaIndexDescriptor descriptor )
            throws TransactionFailureException
    {
        try ( Session session = kernelSupplier.get().beginSession( AUTH_DISABLED );
              Transaction transaction = session.beginTransaction( Transaction.Type.implicit );
              Statement ignore = ((KernelTransaction)transaction).acquireStatement() )
        {
            ((KernelTransactionImplementation) transaction).txState().indexDoDrop( descriptor );
            transaction.success();
        }
    }

    private SchemaIndexDescriptor asDescriptor( CapableIndexReference indexReference )
    {
        if ( indexReference.isUnique() )
        {
            return SchemaIndexDescriptorFactory.uniqueForLabel( indexReference.label(), indexReference.properties() );
        }
        else
        {
            return SchemaIndexDescriptorFactory.forLabel( indexReference.label(), indexReference.properties() );
        }
    }

    private void awaitConstrainIndexPopulation( UniquenessConstraintDescriptor constraint, IndexProxy proxy )
            throws InterruptedException, UniquePropertyValueValidationException
    {
        try
        {
            proxy.awaitStoreScanCompleted();
        }
        catch ( IndexPopulationFailedKernelException e )
        {
            Throwable cause = e.getCause();
            if ( cause instanceof IndexEntryConflictException )
            {
                throw new UniquePropertyValueValidationException(
                        constraint, VERIFICATION, (IndexEntryConflictException) cause );
            }
            else
            {
                throw new UniquePropertyValueValidationException( constraint, VERIFICATION, e );
            }
        }
    }

    private CapableIndexReference getOrCreateUniquenessConstraintIndex( SchemaRead schemaRead,
            TokenRead tokenRead, SchemaDescriptor schema, IndexProvider.Descriptor providerDescriptor )
            throws SchemaKernelException, IndexNotFoundKernelException
    {
        CapableIndexReference descriptor = schemaRead.index( schema.keyId(), schema.getPropertyIds() );
        if ( descriptor != CapableIndexReference.NO_INDEX )
        {
            if ( descriptor.isUnique() )
            {
                // OK so we found a matching constraint index. We check whether or not it has an owner
                // because this may have been a left-over constraint index from a previously failed
                // constraint creation, due to crash or similar, hence the missing owner.
                if ( schemaRead.indexGetOwningUniquenessConstraintId( descriptor ) == null )
                {
                    return descriptor;
                }
                throw new AlreadyConstrainedException(
                        ConstraintDescriptorFactory.uniqueForSchema( schema ),
                        OperationContext.CONSTRAINT_CREATION,
                        new SilentTokenNameLookup( tokenRead ) );
            }
            // There's already an index for this schema descriptor, which isn't of the type we're after.
            throw new AlreadyIndexedException( schema, CONSTRAINT_CREATION );
        }
        SchemaIndexDescriptor indexDescriptor = createConstraintIndex( schema, providerDescriptor );
        IndexProxy indexProxy = indexingService.getIndexProxy( indexDescriptor.schema() );
        return new DefaultCapableIndexReference( indexDescriptor.type() == Type.UNIQUE, indexProxy.getIndexCapability(),
                indexProxy.getProviderDescriptor(), indexDescriptor.schema().keyId(),
                indexDescriptor.schema().getPropertyIds() );
    }

    public SchemaIndexDescriptor createConstraintIndex( final SchemaDescriptor schema, IndexProvider.Descriptor providerDescriptor )
    {
        try ( Session session = kernelSupplier.get().beginSession( AUTH_DISABLED );
              Transaction transaction = session.beginTransaction( Transaction.Type.implicit );
              Statement ignore = ((KernelTransaction)transaction).acquireStatement() )
        {
            SchemaIndexDescriptor index = SchemaIndexDescriptorFactory.uniqueForSchema( schema );
            TransactionState transactionState = ((KernelTransactionImplementation) transaction).txState();
            transactionState.indexRuleDoAdd( index, providerDescriptor );
            transaction.success();
            return index;
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
    }
}
