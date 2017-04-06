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
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.locking.Locks.Client;

import static org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException.Phase.VERIFICATION;
import static org.neo4j.kernel.api.security.SecurityContext.AUTH_DISABLED;
import static org.neo4j.kernel.impl.locking.ResourceTypes.SCHEMA;
import static org.neo4j.kernel.impl.locking.ResourceTypes.schemaResource;

public class ConstraintIndexCreator
{
    private final IndexingService indexingService;
    private final Supplier<KernelAPI> kernelSupplier;
    private final PropertyAccessor propertyAccessor;
    private final boolean releaseSchemaLockWhenCreatingConstraint;

    public ConstraintIndexCreator( Supplier<KernelAPI> kernelSupplier, IndexingService indexingService,
            PropertyAccessor propertyAccessor, boolean releaseSchemaLockWhenCreatingConstraint )
    {
        this.kernelSupplier = kernelSupplier;
        this.indexingService = indexingService;
        this.propertyAccessor = propertyAccessor;
        this.releaseSchemaLockWhenCreatingConstraint = releaseSchemaLockWhenCreatingConstraint;
    }

    /**
     * You MUST hold a schema write lock before you call this method.
     * However the schema write lock is temporarily released while populating the index backing the constraint.
     * It goes a little like this:
     * <ol>
     * <li>Prerequisite: Getting here means that there's an open schema transaction which has acquired the
     * SCHEMA WRITE lock.</li>
     * <li>Index schema rule which is backing the constraint is created in a nested mini-transaction
     * which doesn't acquire any locking, merely adds tx state and commits so that the index rule is applied
     * to the store, which triggers the index population</li>
     * <li>Release the SCHEMA WRITE lock</li>
     * <li>Await index population to complete</li>
     * <li>Acquire the SCHEMA WRITE lock (effectively blocking concurrent transactions changing
     * data related to this constraint, and it so happens, most other transactions as well) and verify
     * the uniqueness of the built index</li>
     * <li>Leave this method, knowing that the uniqueness constraint rule will be added to tx state
     * and this tx committed, which will create the uniqueness constraint</li>
     * </ol>
     */
    public long createUniquenessConstraintIndex(
            KernelStatement state, SchemaReadOperations schema, LabelSchemaDescriptor descriptor
    ) throws TransactionFailureException, CreateConstraintFailureException,
            DropIndexFailureException, UniquePropertyValueValidationException
    {
        UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForSchema( descriptor );
        IndexDescriptor index = createConstraintIndex( descriptor );
        boolean success = false;
        boolean reacquiredSchemaLock = false;
        Client locks = state.locks().pessimistic();
        try
        {
            long indexId = schema.indexGetCommittedId( state, index );

            // Release the SCHEMA WRITE lock during index population.
            // At this point the integrity of the constraint to be created was checked
            // while holding the lock and the index rule backing the soon-to-be-created constraint
            // has been created. Now it's just the population left, which can take a long time
            releaseSchemaLock( locks );

            awaitConstrainIndexPopulation( constraint, indexId );

            // Index population was successful, but at this point we don't know if the uniqueness constraint holds.
            // Acquire SCHEMA WRITE lock and verify the constraints here in this user transaction
            // and if everything checks out then it will be held until after the constraint has been
            // created and activated.
            acquireSchemaLock( state, locks );
            reacquiredSchemaLock = true;
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
                if ( !reacquiredSchemaLock )
                {
                    acquireSchemaLock( state, locks );
                }
                dropUniquenessConstraintIndex( index );
            }
        }
    }

    private void acquireSchemaLock( KernelStatement state, Client locks )
    {
        if ( releaseSchemaLockWhenCreatingConstraint )
        {
            locks.acquireExclusive( state.lockTracer(), SCHEMA, schemaResource() );
        }
    }

    private void releaseSchemaLock( Client locks )
    {
        if ( releaseSchemaLockWhenCreatingConstraint )
        {
            locks.releaseExclusive( SCHEMA, schemaResource() );
        }
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
            // NOTE: This creates the index (obviously) but it DOES NOT grab a schema
            // write lock. It is assumed that the transaction that invoked this "inner" transaction
            // holds a schema write lock, and that it will wait for this inner transaction to do its
            // work.
            // TODO (Ben+Jake): The Transactor is really part of the kernel internals, so it needs access to the
            // internal implementation of Statement. However it is currently used by the external
            // RemoveOrphanConstraintIndexesOnStartup job. This needs revisiting.
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

    public IndexDescriptor createConstraintIndex( final LabelSchemaDescriptor schema )
    {
        try ( KernelTransaction transaction =
                      kernelSupplier.get().newTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED );
              Statement statement = transaction.acquireStatement() )
        {
            // NOTE: This creates the index (obviously) but it DOES NOT grab a schema
            // write lock. It is assumed that the transaction that invoked this "inner" transaction
            // holds a schema write lock, and that it will wait for this inner transaction to do its
            // work.
            // TODO (Ben+Jake): The Transactor is really part of the kernel internals, so it needs access to the
            // internal implementation of Statement. However it is currently used by the external
            // RemoveOrphanConstraintIndexesOnStartup job. This needs revisiting.
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
