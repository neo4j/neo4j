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

import static org.neo4j.helpers.collection.IteratorUtil.loop;

import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

public class DataIntegrityValidatingStatementContext extends CompositeStatementContext
{
    private final StatementContext delegate;

    public DataIntegrityValidatingStatementContext( StatementContext delegate )
    {
        super( delegate );
        this.delegate = delegate;
    }

    @Override
    public long getOrCreatePropertyKeyId( String propertyKey ) throws ConstraintViolationKernelException
    {
        // KISS - but refactor into a general purpose constraint checker later on
        if ( propertyKey == null )
        {
            throw new ConstraintViolationKernelException(
                    String.format( "Null is not a valid property name. Only non-null strings are allowed." ) );
        }

        return delegate.getOrCreatePropertyKeyId( propertyKey );
    }

    @Override
    public long getOrCreateLabelId( String label ) throws ConstraintViolationKernelException
    {
        // KISS - but refactor into a general purpose constraint checker later on
        if ( label == null || label.length() == 0 )
        {
            throw new ConstraintViolationKernelException(
                    String.format( "%s is not a valid label name. Only non-empty strings are allowed.",
                                   label == null ? "null" : "'" + label + "'" ) );
        }

        return delegate.getOrCreateLabelId( label );
    }

    @Override
    public IndexDescriptor addIndex( long labelId, long propertyKey )
            throws ConstraintViolationKernelException
    {
        checkIndexExistence( labelId, propertyKey );
        return delegate.addIndex( labelId, propertyKey );
    }

    @Override
    public IndexDescriptor addConstraintIndex( long labelId, long propertyKey )
            throws ConstraintViolationKernelException
    {
        checkIndexExistence( labelId, propertyKey );
        return delegate.addConstraintIndex( labelId, propertyKey );
    }

    private void checkIndexExistence( long labelId, long propertyKey ) throws ConstraintViolationKernelException
    {
        for ( IndexDescriptor descriptor : loop( getIndexes( labelId ) ) )
        {
            if ( descriptor.getPropertyKeyId() == propertyKey )
            {
                throw new ConstraintViolationKernelException( "Property " + propertyKey +
                                                              " is already indexed for label " + labelId + "." );
            }
        }
        for ( IndexDescriptor descriptor : loop( getConstraintIndexes( labelId ) ) )
        {
            if ( descriptor.getPropertyKeyId() == propertyKey )
            {
                throw new ConstraintViolationKernelException( "Property " + propertyKey +
                                                              " is already indexed for label " + labelId +
                                                              " through a constraint." );
            }
        }
    }

    @Override
    public void dropIndex( IndexDescriptor descriptor ) throws ConstraintViolationKernelException
    {
        for ( IndexDescriptor existing : loop( getIndexes( descriptor.getLabelId() ) ) )
        {
            if ( existing.getPropertyKeyId() == descriptor.getPropertyKeyId() )
            {
                delegate.dropIndex( descriptor );
                return;
            }
        }
        throw new ConstraintViolationKernelException( String.format(
                "There is no index for property %d for label %d.",
                descriptor.getPropertyKeyId(), descriptor.getLabelId() ) );
    }

    @Override
    public void dropConstraintIndex( IndexDescriptor descriptor ) throws ConstraintViolationKernelException
    {
        for ( IndexDescriptor existing : loop( getConstraintIndexes( descriptor.getLabelId() ) ) )
        {
            if ( existing.getPropertyKeyId() == descriptor.getPropertyKeyId() )
            {
                delegate.dropConstraintIndex( descriptor );
                return;
            }
        }
        throw new ConstraintViolationKernelException( String.format(
                "There is no constraint index for property %d for label %d.",
                descriptor.getPropertyKeyId(), descriptor.getLabelId() ) );
    }

    @Override
    public UniquenessConstraint addUniquenessConstraint( long labelId, long propertyKey )
            throws ConstraintViolationKernelException
    {
        if ( getConstraints( labelId, propertyKey ).hasNext() )
        {
            throw new ConstraintViolationKernelException( "Property " + propertyKey +
                                                          " already has a uniqueness constraint for label " + labelId +
                                                          "." );
        }

        return delegate.addUniquenessConstraint( labelId, propertyKey );
    }
}
