/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException.OperationContext;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor.Type;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.locking.Locks.Client;

import static org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException.Phase.VERIFICATION;
import static org.neo4j.kernel.api.exceptions.schema.SchemaKernelException.OperationContext.CONSTRAINT_CREATION;
import static org.neo4j.kernel.api.security.SecurityContext.AUTH_DISABLED;
import static org.neo4j.kernel.impl.locking.ResourceTypes.LABEL;

public class ConstraintIndexCreator
{
    private final IndexingService indexingService;
    private final Supplier<KernelAPI> kernelSupplier;
    private final PropertyAccessor propertyAccessor;

    public ConstraintIndexCreator( Supplier<KernelAPI> kernelSupplier, IndexingService indexingService,
            PropertyAccessor propertyAccessor )
    {
        this.kernelSupplier = kernelSupplier;
        this.indexingService = indexingService;
        this.propertyAccessor = propertyAccessor;
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
     */
    public long createUniquenessConstraintIndex(
            KernelStatement state, SchemaReadOperations schemaOps, LabelSchemaDescriptor descriptor
    ) throws TransactionFailureException, CreateConstraintFailureException,
            DropIndexFailureException, UniquePropertyValueValidationException, AlreadyConstrainedException
    {
        UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForSchema( descriptor );
        IndexDescriptor index;
        try
        {
            index = getOrCreateUniquenessConstraintIndex( state, schemaOps, descriptor );
        }
        catch ( AlreadyConstrainedException e )
        {
            throw e;
        }
        catch ( SchemaKernelException e )
        {
            throw new CreateConstraintFailureException( constraint, e );
        }

        boolean success = false;
        boolean reacquiredLabelLock = false;
        Client locks = state.locks().pessimistic();
        try
        {
            long indexId = schemaOps.indexGetCommittedId( state, index );

            // Release the LABEL WRITE lock during index population.
            // At this point the integrity of the constraint to be created was checked
            // while holding the lock and the index rule backing the soon-to-be-created constraint
            // has been created. Now it's just the population left, which can take a long time
            releaseLabelLock( locks, descriptor.getLabelId() );

            awaitConstrainIndexPopulation( constraint, indexId );

            // Index population was successful, but at this point we don't know if the uniqueness constraint holds.
            // Acquire LABEL WRITE lock and verify the constraints here in this user transaction
            // and if everything checks out then it will be held until after the constraint has been
            // created and activated.
            acquireLabelLock( state, locks, descriptor.getLabelId() );
            reacquiredLabelLock = true;
            indexingService.getIndexProxy( indexId ).verifyDeferredConstraints( propertyAccessor );
            success = true;
            return indexId;
        }
        catch ( SchemaRuleNotFoundException | IndexNotFoundKernelException e )
        {
            throw new IllegalStateException(
                    String.format( "Index (%s) that we just created does not exist.", descriptor ) );
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
                    acquireLabelLock( state, locks, descriptor.getLabelId() );
                }
                dropUniquenessConstraintIndex( index );
            }
        }
    }

    private void acquireLabelLock( KernelStatement state, Client locks, int labelId )
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
    public void dropUniquenessConstraintIndex( IndexDescriptor descriptor )
            throws TransactionFailureException, DropIndexFailureException
    {
        try ( KernelTransaction transaction =
                      kernelSupplier.get().newTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED );
              Statement statement = transaction.acquireStatement() )
        {
            ((KernelStatement) statement).txState().indexDoDrop( descriptor );
            transaction.success();
        }
    }

    private void awaitConstrainIndexPopulation( UniquenessConstraintDescriptor constraint, long indexId )
            throws InterruptedException, UniquePropertyValueValidationException
    {
        try
        {
            indexingService.getIndexProxy( indexId ).awaitStoreScanCompleted();
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new IllegalStateException(
                    String.format( "Index (indexId=%d) that we just created does not exist.", indexId ) );
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
                throw new UniquePropertyValueValidationException( constraint, VERIFICATION, cause );
            }
        }
    }

    public IndexDescriptor getOrCreateUniquenessConstraintIndex( KernelStatement state,
            SchemaReadOperations schemaOps, LabelSchemaDescriptor schema ) throws SchemaKernelException
    {
        IndexDescriptor descriptor = schemaOps.indexGetForSchema( state, schema );
        if ( descriptor != null )
        {
            if ( descriptor.type() == Type.UNIQUE )
            {
                // OK so we found a matching constraint index. We check whether or not it has an owner
                // because this may have been a left-over constraint index from a previously failed
                // constraint creation, due to crash or similar, hence the missing owner.
                if ( schemaOps.indexGetOwningUniquenessConstraintId( state, descriptor ) == null )
                {
                    return descriptor;
                }
                throw new AlreadyConstrainedException(
                        ConstraintDescriptorFactory.uniqueForSchema( schema ),
                        OperationContext.CONSTRAINT_CREATION,
                        new StatementTokenNameLookup( state.readOperations() ) );
            }
            // There's already an index for this schema descriptor, which isn't of the type we're after.
            throw new AlreadyIndexedException( schema, CONSTRAINT_CREATION );
        }

        return createConstraintIndex( schema );
    }

    public IndexDescriptor createConstraintIndex( final LabelSchemaDescriptor schema )
    {
        try ( KernelTransaction transaction =
                      kernelSupplier.get().newTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED );
              Statement statement = transaction.acquireStatement() )
        {
            IndexDescriptor index = IndexDescriptorFactory.uniqueForSchema( schema );
            ((KernelStatement) statement).txState().indexRuleDoAdd( index );
            transaction.success();
            return index;
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
    }
}
