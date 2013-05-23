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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.api.DataIntegrityKernelException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

import static org.neo4j.helpers.collection.IteratorUtil.loop;

public class DataIntegrityValidatingStatementContext extends CompositeStatementContext
{
    private final StatementContext delegate;

    public DataIntegrityValidatingStatementContext( StatementContext delegate )
    {
        super( delegate );
        this.delegate = delegate;
    }

    @Override
    public long propertyKeyGetOrCreateForName( String propertyKey ) throws DataIntegrityKernelException
    {
        // KISS - but refactor into a general purpose constraint checker later on
        if ( propertyKey == null )
        {
            throw new DataIntegrityKernelException.IllegalTokenNameException( null );
        }

        return delegate.propertyKeyGetOrCreateForName( propertyKey );
    }

    @Override
    public long labelGetOrCreateForName( String label ) throws DataIntegrityKernelException
    {
        // KISS - but refactor into a general purpose constraint checker later on
        if ( label == null || label.length() == 0 )
        {
            throw new DataIntegrityKernelException.IllegalTokenNameException( label );
        }

        return delegate.labelGetOrCreateForName( label );
    }

    @Override
    public IndexDescriptor indexCreate( long labelId, long propertyKey )
            throws DataIntegrityKernelException
    {
        checkIndexExistence( labelId, propertyKey );
        return delegate.indexCreate( labelId, propertyKey );
    }

    @Override
    public IndexDescriptor uniqueIndexCreate( long labelId, long propertyKey )
            throws DataIntegrityKernelException
    {
        checkIndexExistence( labelId, propertyKey );
        return delegate.uniqueIndexCreate( labelId, propertyKey );
    }

    private void checkIndexExistence( long labelId, long propertyKey ) throws DataIntegrityKernelException
    {
        for ( IndexDescriptor descriptor : loop( indexesGetForLabel( labelId ) ) )
        {
            if ( descriptor.getPropertyKeyId() == propertyKey )
            {
                throw new DataIntegrityKernelException.AlreadyIndexedException( labelId, propertyKey );
            }
        }
        for ( IndexDescriptor descriptor : loop( uniqueIndexesGetForLabel( labelId ) ) )
        {
            if ( descriptor.getPropertyKeyId() == propertyKey )
            {
                throw new DataIntegrityKernelException.AlreadyConstrainedException( labelId, propertyKey );
            }
        }
    }

    @Override
    public void indexDrop( IndexDescriptor descriptor ) throws DataIntegrityKernelException
    {
        for ( IndexDescriptor existing : loop( indexesGetForLabel( descriptor.getLabelId() ) ) )
        {
            if ( existing.getPropertyKeyId() == descriptor.getPropertyKeyId() )
            {
                delegate.indexDrop( descriptor );
                return;
            }
        }
        throw new DataIntegrityKernelException.
                NoSuchIndexException( descriptor.getLabelId(), descriptor.getPropertyKeyId() );
    }

    @Override
    public void uniqueIndexDrop( IndexDescriptor descriptor ) throws DataIntegrityKernelException
    {
        for ( IndexDescriptor existing : loop( uniqueIndexesGetForLabel( descriptor.getLabelId() ) ) )
        {
            if ( existing.getPropertyKeyId() == descriptor.getPropertyKeyId() )
            {
                delegate.uniqueIndexDrop( descriptor );
                return;
            }
        }
        throw new DataIntegrityKernelException.
                NoSuchIndexException( descriptor.getLabelId(), descriptor.getPropertyKeyId() );
    }

    @Override
    public UniquenessConstraint uniquenessConstraintCreate( long labelId, long propertyKey )
            throws DataIntegrityKernelException, ConstraintCreationKernelException
    {
        if ( constraintsGetForLabelAndPropertyKey( labelId, propertyKey ).hasNext() )
        {
            throw new DataIntegrityKernelException.AlreadyConstrainedException( labelId, propertyKey );
        }

        return delegate.uniquenessConstraintCreate( labelId, propertyKey );
    }
}
