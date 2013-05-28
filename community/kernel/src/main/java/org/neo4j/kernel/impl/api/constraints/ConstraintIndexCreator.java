/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.constraints;

import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.TransactionalException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintCreationKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.Transactor;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexingService;

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

    public long createUniquenessConstraintIndex( SchemaReadOperations schema, long labelId, long propertyKeyId )
            throws SchemaKernelException, ConstraintVerificationFailedKernelException, TransactionalException
    {
        UniquenessConstraint constraint = new UniquenessConstraint( labelId, propertyKeyId );
        IndexDescriptor descriptor = transactor.execute( createConstraintIndex( labelId, propertyKeyId ) );
        long indexId;
        try
        {
            indexId = schema.indexGetCommittedId( descriptor );
        }
        catch ( SchemaRuleNotFoundException e )
        {
            throw new IllegalStateException(
                    String.format( "Index (%s) that we just created does not exist.", descriptor ) );
        }
        boolean success = false;
        try
        {
            awaitIndexPopulation( constraint, indexId );
            success = true;
        }
        catch ( InterruptedException exception )
        {
            throw new ConstraintVerificationFailedKernelException( constraint, exception );
        }
        finally
        {
            if ( !success )
            {
                dropUniquenessConstraintIndex( descriptor );
            }
        }
        return indexId;
    }

    public void validateConstraintIndex( UniquenessConstraint constraint, long indexId )
            throws ConstraintCreationKernelException
    {
        try
        {
            indexingService.validateIndex( indexId );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new IllegalStateException(
                    String.format( "Index (indexId=%d) that we just created does not exist.", indexId ) );
        }
        catch ( IndexPopulationFailedKernelException e )
        {
            Throwable failure = e.getCause();
            if ( failure instanceof ConstraintVerificationFailedKernelException )
            {
                throw new ConstraintCreationKernelException( constraint, failure );
            }
            if ( failure instanceof IndexEntryConflictException )
            {
                IndexEntryConflictException conflict = (IndexEntryConflictException) failure;
                throw new ConstraintCreationKernelException(
                        constraint, new ConstraintVerificationFailedKernelException( constraint, singleton(
                        new ConstraintVerificationFailedKernelException.Evidence( conflict ) ) ) );
            }
            throw new ConstraintCreationKernelException( constraint, failure );
        }
    }

    public void dropUniquenessConstraintIndex( IndexDescriptor descriptor )
            throws SchemaKernelException, TransactionalException
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

    private static Transactor.Statement<IndexDescriptor, SchemaKernelException> createConstraintIndex(
            final long labelId, final long propertyKeyId )
    {
        return new Transactor.Statement<IndexDescriptor, SchemaKernelException>()
        {
            @Override
            public IndexDescriptor perform( StatementContext statement ) throws
                    SchemaKernelException
            {
                return statement.uniqueIndexCreate( labelId, propertyKeyId );
            }
        };
    }

    private static Transactor.Statement<Void, SchemaKernelException> dropConstraintIndex(
            final IndexDescriptor descriptor )
    {
        return new Transactor.Statement<Void, SchemaKernelException>()
        {
            @Override
            public Void perform( StatementContext statement ) throws SchemaKernelException
            {
                statement.uniqueIndexDrop( descriptor );
                return null;
            }
        };
    }
}
