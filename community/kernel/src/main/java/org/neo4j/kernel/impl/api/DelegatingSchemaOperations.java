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
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
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
    public IndexDescriptor indexesGetForLabelAndPropertyKey( long labelId, long propertyKey ) throws SchemaRuleNotFoundException
    {
        return delegate.indexesGetForLabelAndPropertyKey( labelId, propertyKey );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( long labelId )
    {
        return delegate.indexesGetForLabel( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        return delegate.indexesGetAll();
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( long labelId )
    {
        return delegate.uniqueIndexesGetForLabel( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll()
    {
        return delegate.uniqueIndexesGetAll();
    }

    @Override
    public InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return delegate.indexGetState( descriptor );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( long labelId, long propertyKeyId )
    {
        return delegate.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabel( long labelId )
    {
        return delegate.constraintsGetForLabel( labelId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetAll()
    {
        return delegate.constraintsGetAll();
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        return delegate.indexGetOwningUniquenessConstraintId( index );
    }

    @Override
    public long indexGetCommittedId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        return delegate.indexGetCommittedId( index );
    }

    @Override
    public IndexDescriptor indexCreate( long labelId, long propertyKey ) throws SchemaKernelException
    {
        return delegate.indexCreate( labelId, propertyKey );
    }

    @Override
    public IndexDescriptor uniqueIndexCreate( long labelId, long propertyKey ) throws SchemaKernelException
    {
        return delegate.uniqueIndexCreate( labelId, propertyKey );
    }

    @Override
    public void indexDrop( IndexDescriptor indexRule ) throws DropIndexFailureException
    {
        delegate.indexDrop( indexRule );
    }

    @Override
    public void uniqueIndexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        delegate.uniqueIndexDrop( descriptor );
    }

    @Override
    public UniquenessConstraint uniquenessConstraintCreate( long labelId, long propertyKeyId )
            throws SchemaKernelException
    {
        return delegate.uniquenessConstraintCreate( labelId, propertyKeyId );
    }

    @Override
    public void constraintDrop( UniquenessConstraint constraint )
    {
        delegate.constraintDrop( constraint );
    }

    @Override
    public <K, V> V schemaStateGetOrCreate( K key, Function<K, V> creator )
    {
        return delegate.schemaStateGetOrCreate( key, creator );
    }

    @Override
    public <K> boolean schemaStateContains( K key )
    {
        return delegate.schemaStateContains( key );
    }
}
