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

import org.neo4j.kernel.api.DataIntegrityKernelException;
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionalException;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.Transactor;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.impl.api.index.IndexingService;

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
            throws DataIntegrityKernelException, IndexPopulationFailedKernelException,
                   TransactionalException
    {
        IndexDescriptor descriptor = transactor.execute( createConstraintIndex( labelId, propertyKeyId ) );
        long indexId;
        try
        {
            indexId = schema.getCommittedIndexId( descriptor );
        }
        catch ( SchemaRuleNotFoundException e )
        {
            throw new IllegalStateException(
                    String.format( "Index (%s) that we just created does not exist.", descriptor ) );
        }
        boolean success = false;
        try
        {
            awaitIndexPopulation( indexId );
            success = true;
        }
        catch ( InterruptedException exception )
        {
            throw new IndexPopulationFailedKernelException( descriptor, exception );
        }
        finally
        {
            if ( !success )
            {
                transactor.execute( dropConstraintIndex( descriptor ) );
            }
        }
        return indexId;
    }

    private void awaitIndexPopulation( long indexId ) throws IndexPopulationFailedKernelException, InterruptedException
    {
        try
        {
            indexingService.getProxyForRule( indexId ).awaitPopulationCompleted();
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new IllegalStateException(
                    String.format( "Index (indexId=%d) that we just created does not exist.", indexId ) );
        }
    }

    private static Transactor.Statement<IndexDescriptor, DataIntegrityKernelException> createConstraintIndex(
            final long labelId, final long propertyKeyId )
    {
        return new Transactor.Statement<IndexDescriptor, DataIntegrityKernelException>()
        {
            @Override
            public IndexDescriptor perform( StatementContext statement ) throws
                                                                         DataIntegrityKernelException
            {
                return statement.addConstraintIndex( labelId, propertyKeyId );
            }
        };
    }

    private static Transactor.Statement<Void, DataIntegrityKernelException> dropConstraintIndex(
            final IndexDescriptor descriptor )
    {
        return new Transactor.Statement<Void, DataIntegrityKernelException>()
        {
            @Override
            public Void perform( StatementContext statement ) throws DataIntegrityKernelException
            {
                statement.dropConstraintIndex( descriptor );
                return null;
            }
        };
    }
}
