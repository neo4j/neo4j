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
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

public class LockingStatementContext extends CompositeStatementContext
{
    private final LockHolder lockHolder;
    private final StatementContext delegate;

    public LockingStatementContext( StatementContext actual, LockHolder lockHolder )
    {
        super( actual );
        this.lockHolder = lockHolder;
        this.delegate = actual;
    }

    @Override
    public boolean nodeAddLabel( long nodeId, long labelId ) throws EntityNotFoundException
    {
        lockHolder.acquireNodeWriteLock( nodeId );
        return delegate.nodeAddLabel( nodeId, labelId );
    }

    @Override
    public boolean nodeRemoveLabel( long nodeId, long labelId ) throws EntityNotFoundException
    {
        lockHolder.acquireNodeWriteLock( nodeId );
        return delegate.nodeRemoveLabel( nodeId, labelId );
    }

    @Override
    public IndexDescriptor indexCreate( long labelId, long propertyKey ) throws SchemaKernelException
    {
        lockHolder.acquireSchemaWriteLock();
        return delegate.indexCreate( labelId, propertyKey );
    }

    @Override
    public IndexDescriptor uniqueIndexCreate( long labelId, long propertyKey ) throws SchemaKernelException
    {
        lockHolder.acquireSchemaWriteLock();
        return delegate.uniqueIndexCreate( labelId, propertyKey );
    }

    @Override
    public void indexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        lockHolder.acquireSchemaWriteLock();
        delegate.indexDrop( descriptor );
    }

    @Override
    public void uniqueIndexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        lockHolder.acquireSchemaWriteLock();
        delegate.uniqueIndexDrop( descriptor );
    }

    @Override
    public <K, V> V schemaStateGetOrCreate( K key, Function<K, V> creator )
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.schemaStateGetOrCreate( key, creator );
    }

    @Override
    public <K> boolean schemaStateContains( K key )
    {
        lockHolder.acquireSchemaReadLock();
        return super.schemaStateContains( key );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( long labelId )
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.indexesGetForLabel( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.indexesGetAll();
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( long labelId )
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.uniqueIndexesGetForLabel( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll()
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.uniqueIndexesGetAll();
    }

    @Override
    public void nodeDelete( long nodeId )
    {
        lockHolder.acquireNodeWriteLock( nodeId );
        delegate.nodeDelete( nodeId );
    }

    @Override
    public UniquenessConstraint uniquenessConstraintCreate( long labelId, long propertyKeyId )
            throws SchemaKernelException
    {
        lockHolder.acquireSchemaWriteLock();
        return delegate.uniquenessConstraintCreate( labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( long labelId, long propertyKeyId )
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabel( long labelId )
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.constraintsGetForLabel( labelId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetAll()
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.constraintsGetAll();
    }

    @Override
    public void constraintDrop( UniquenessConstraint constraint )
    {
        lockHolder.acquireSchemaWriteLock();
        delegate.constraintDrop( constraint );
    }
}
