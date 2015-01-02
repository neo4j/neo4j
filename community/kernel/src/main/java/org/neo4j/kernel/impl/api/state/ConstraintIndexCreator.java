/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.Transactor;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.TransactionalException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.nioneo.store.SchemaStorage;

import static java.util.Collections.singleton;

public class ConstraintIndexCreator
{
    private final Transactor transactor;
    private final IndexingService indexingService;

    public ConstraintIndexCreator( Transactor transactor, IndexingService indexingService )
    {
        this.transactor = transactor;
        this.indexingService = indexingService;
    }

    /**
     * You MUST hold a schema write lock before you call this method.
     */
    public long createUniquenessConstraintIndex( KernelStatement state, SchemaReadOperations schema,
            int labelId, int propertyKeyId )
            throws ConstraintVerificationFailedKernelException, TransactionalException,
                   CreateConstraintFailureException, DropIndexFailureException
    {
        IndexDescriptor descriptor = transactor.execute( createConstraintIndex( labelId, propertyKeyId ) );
        UniquenessConstraint constraint = new UniquenessConstraint( labelId, propertyKeyId );

        boolean success = false;
        try
        {
            long indexId = schema.indexGetCommittedId( state, descriptor, SchemaStorage.IndexRuleKind.CONSTRAINT );
            awaitIndexPopulation( constraint, indexId );
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
                dropUniquenessConstraintIndex( descriptor );
            }
        }
    }

    /**
     * You MUST hold a schema write lock before you call this method.
     */
    public void dropUniquenessConstraintIndex( IndexDescriptor descriptor )
            throws TransactionalException, DropIndexFailureException
    {
        transactor.execute( dropConstraintIndex( descriptor ) );
    }

    private void awaitIndexPopulation( UniquenessConstraint constraint, long indexId )
            throws InterruptedException, ConstraintVerificationFailedKernelException
    {
        try
        {
            indexingService.getProxyForRule( indexId ).awaitStoreScanCompleted();
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
                throw new ConstraintVerificationFailedKernelException( constraint, singleton(
                        new ConstraintVerificationFailedKernelException.Evidence(
                                (IndexEntryConflictException) cause ) ) );
            }
            else
            {
                throw new ConstraintVerificationFailedKernelException( constraint, cause );
            }
        }
    }

    public static Transactor.Work<IndexDescriptor, CreateConstraintFailureException> createConstraintIndex(
            final int labelId, final int propertyKeyId )
    {
        return new Transactor.Work<IndexDescriptor, CreateConstraintFailureException>()
        {
            @Override
            public IndexDescriptor perform( Statement kernelStatement )
            {
                // NOTE: This creates the index (obviously) but it DOES NOT grab a schema
                // write lock. It is assumed that the transaction that invoked this "inner" transaction
                // holds a schema write lock, and that it will wait for this inner transaction to do its
                // work.
                IndexDescriptor descriptor = new IndexDescriptor( labelId, propertyKeyId );
                // TODO (Ben+Jake): The Transactor is really part of the kernel internals, so it needs access to the
                // internal implementation of Statement. However it is currently used by the external
                // RemoveOrphanConstraintIndexesOnStartup job. This needs revisiting.
                ((KernelStatement) kernelStatement).txState().constraintIndexRuleDoAdd( descriptor );
                return descriptor;
            }
        };
    }

    private static Transactor.Work<Void, DropIndexFailureException> dropConstraintIndex(
            final IndexDescriptor descriptor )
    {
        return new Transactor.Work<Void, DropIndexFailureException>()
        {
            @Override
            public Void perform( Statement kernelStatement )
            {
                // NOTE: This creates the index (obviously) but it DOES NOT grab a schema
                // write lock. It is assumed that the transaction that invoked this "inner" transaction
                // holds a schema write lock, and that it will wait for this inner transaction to do its
                // work.
                // TODO (Ben+Jake): The Transactor is really part of the kernel internals, so it needs access to the
                // internal implementation of Statement. However it is currently used by the external
                // RemoveOrphanConstraintIndexesOnStartup job. This needs revisiting.
                ((KernelStatement) kernelStatement).txState().constraintIndexDoDrop( descriptor );
                return null;
            }
        };
    }
}
