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

import java.util.function.Supplier;

import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.schema.NodePropertyDescriptor;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.UniquenessConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.schema_new.index.IndexBoundary;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;

import static java.util.Collections.singleton;
import static org.neo4j.kernel.api.security.SecurityContext.AUTH_DISABLED;

public class ConstraintIndexCreator
{
    private final IndexingService indexingService;
    private final Supplier<KernelAPI> kernelSupplier;

    public ConstraintIndexCreator( Supplier<KernelAPI> kernelSupplier, IndexingService indexingService )
    {
        this.kernelSupplier = kernelSupplier;
        this.indexingService = indexingService;
    }

    /**
     * You MUST hold a schema write lock before you call this method.
     */
    public long createUniquenessConstraintIndex( KernelStatement state, SchemaReadOperations schema,
            NodePropertyDescriptor descriptor )
            throws ConstraintVerificationFailedKernelException, TransactionFailureException,
            CreateConstraintFailureException, DropIndexFailureException
    {
        UniquenessConstraint constraint = new UniquenessConstraint( descriptor );
        NewIndexDescriptor index = createConstraintIndex( constraint );
        boolean success = false;
        try
        {
            long indexId = schema.indexGetCommittedId( state, index, NewIndexDescriptor.Filter.UNIQUE );

            awaitConstrainIndexPopulation( constraint, indexId );
            success = true;
            return indexId;
        }
        catch ( SchemaRuleNotFoundException e )
        {
            throw new IllegalStateException(
                    String.format( "Index (%s) that we just created does not exist.", descriptor ) );
        }
        catch ( InterruptedException e )
        {
            throw new CreateConstraintFailureException( constraint, e );
        }
        finally
        {
            if ( !success )
            {
                dropUniquenessConstraintIndex( index );
            }
        }
    }

    /**
     * You MUST hold a schema write lock before you call this method.
     */
    public void dropUniquenessConstraintIndex( NewIndexDescriptor descriptor )
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

    private void awaitConstrainIndexPopulation( UniquenessConstraint constraint, long indexId )
            throws InterruptedException, ConstraintVerificationFailedKernelException
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
                throw new UniquenessConstraintVerificationFailedKernelException( constraint,
                        singleton( (IndexEntryConflictException) cause ) );
            }
            else
            {
                throw new UniquenessConstraintVerificationFailedKernelException( constraint, cause );
            }
        }
    }

    public NewIndexDescriptor createConstraintIndex( final UniquenessConstraint constraint )
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
            NewIndexDescriptor index = IndexBoundary.mapUnique( constraint.indexDescriptor() );
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
