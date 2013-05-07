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

import java.util.Iterator;

import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.DataIntegrityKernelException;
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.SchemaOperations;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

public class DelegatingSchemaOperations implements SchemaOperations
{
    private final SchemaOperations delegate;

    public DelegatingSchemaOperations( SchemaOperations delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public IndexDescriptor getIndex( long labelId, long propertyKey ) throws SchemaRuleNotFoundException
    {
        return delegate.getIndex( labelId, propertyKey );
    }

    @Override
    public Iterator<IndexDescriptor> getIndexes( long labelId )
    {
        return delegate.getIndexes( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> getIndexes()
    {
        return delegate.getIndexes();
    }

    @Override
    public Iterator<IndexDescriptor> getConstraintIndexes( long labelId )
    {
        return delegate.getConstraintIndexes( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> getConstraintIndexes()
    {
        return delegate.getConstraintIndexes();
    }

    @Override
    public InternalIndexState getIndexState( IndexDescriptor indexRule ) throws IndexNotFoundKernelException
    {
        return delegate.getIndexState( indexRule );
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints( long labelId, long propertyKeyId )
    {
        return delegate.getConstraints( labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints( long labelId )
    {
        return delegate.getConstraints( labelId );
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints()
    {
        return delegate.getConstraints();
    }

    @Override
    public Long getOwningConstraint( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        return delegate.getOwningConstraint( index );
    }

    @Override
    public long getCommittedIndexId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        return delegate.getCommittedIndexId( index );
    }

    @Override
    public IndexDescriptor addIndex( long labelId, long propertyKey ) throws
                                                                      DataIntegrityKernelException
    {
        return delegate.addIndex( labelId, propertyKey );
    }

    @Override
    public IndexDescriptor addConstraintIndex( long labelId, long propertyKey )
            throws DataIntegrityKernelException
    {
        return delegate.addConstraintIndex( labelId, propertyKey );
    }

    @Override
    public void dropIndex( IndexDescriptor indexRule ) throws DataIntegrityKernelException
    {
        delegate.dropIndex( indexRule );
    }

    @Override
    public void dropConstraintIndex( IndexDescriptor descriptor ) throws DataIntegrityKernelException
    {
        delegate.dropConstraintIndex( descriptor );
    }

    @Override
    public UniquenessConstraint addUniquenessConstraint( long labelId, long propertyKeyId )
            throws DataIntegrityKernelException, ConstraintCreationKernelException
    {
        return delegate.addUniquenessConstraint( labelId, propertyKeyId );
    }

    @Override
    public void dropConstraint( UniquenessConstraint constraint )
    {
        delegate.dropConstraint( constraint );
    }

    @Override
    public <K, V> V getOrCreateFromSchemaState( K key, Function<K, V> creator )
    {
        return delegate.getOrCreateFromSchemaState( key, creator );
    }

    @Override
    public <K> boolean schemaStateContains( K key )
    {
        return delegate.schemaStateContains( key );
    }
}
