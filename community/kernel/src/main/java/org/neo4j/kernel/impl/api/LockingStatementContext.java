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
import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.EntityNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
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
    public boolean addLabelToNode( long labelId, long nodeId ) throws EntityNotFoundException
    {
        lockHolder.acquireNodeWriteLock( nodeId );
        return delegate.addLabelToNode( labelId, nodeId );
    }

    @Override
    public boolean removeLabelFromNode( long labelId, long nodeId ) throws EntityNotFoundException
    {
        lockHolder.acquireNodeWriteLock( nodeId );
        return delegate.removeLabelFromNode( labelId, nodeId );
    }

    @Override
    public IndexDescriptor addIndexRule( long labelId, long propertyKey ) throws ConstraintViolationKernelException
    {
        lockHolder.acquireSchemaWriteLock();
        return delegate.addIndexRule( labelId, propertyKey );
    }

    @Override
    public void dropIndexRule( IndexDescriptor indexRule ) throws ConstraintViolationKernelException
    {
        lockHolder.acquireSchemaWriteLock();
        delegate.dropIndexRule( indexRule );
    }

    @Override
    public <K, V> V getOrCreateFromSchemaState( K key, Function<K, V> creator )
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.getOrCreateFromSchemaState( key, creator );
    }

    @Override
    public <K> boolean schemaStateContains( K key )
    {
        lockHolder.acquireSchemaReadLock();
        return super.schemaStateContains( key );
    }

    @Override
    public Iterator<IndexDescriptor> getIndexRules( long labelId )
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.getIndexRules( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> getIndexRules()
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.getIndexRules();
    }

    @Override
    public void deleteNode( long nodeId )
    {
        lockHolder.acquireNodeWriteLock( nodeId );
        delegate.deleteNode( nodeId );
    }

    @Override
    public UniquenessConstraint addUniquenessConstraint( long labelId, long propertyKeyId )
    {
        lockHolder.acquireSchemaWriteLock();
        return delegate.addUniquenessConstraint( labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints( long labelId, long propertyKeyId )
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.getConstraints( labelId, propertyKeyId );
    }

    @Override
    public void dropConstraint( UniquenessConstraint constraint )
    {
        lockHolder.acquireSchemaWriteLock();
        delegate.dropConstraint( constraint );
    }
}
