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
    public IndexDescriptor addIndex( long labelId, long propertyKey ) throws
                                                                      DataIntegrityKernelException
    {
        lockHolder.acquireSchemaWriteLock();
        return delegate.addIndex( labelId, propertyKey );
    }

    @Override
    public IndexDescriptor addConstraintIndex( long labelId, long propertyKey )
            throws DataIntegrityKernelException
    {
        lockHolder.acquireSchemaWriteLock();
        return delegate.addConstraintIndex( labelId, propertyKey );
    }

    @Override
    public void dropIndex( IndexDescriptor descriptor ) throws DataIntegrityKernelException
    {
        lockHolder.acquireSchemaWriteLock();
        delegate.dropIndex( descriptor );
    }

    @Override
    public void dropConstraintIndex( IndexDescriptor descriptor ) throws DataIntegrityKernelException
    {
        lockHolder.acquireSchemaWriteLock();
        delegate.dropConstraintIndex( descriptor );
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
    public Iterator<IndexDescriptor> getIndexes( long labelId )
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.getIndexes( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> getIndexes()
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.getIndexes();
    }

    @Override
    public Iterator<IndexDescriptor> getConstraintIndexes( long labelId )
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.getConstraintIndexes( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> getConstraintIndexes()
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.getConstraintIndexes();
    }

    @Override
    public void deleteNode( long nodeId )
    {
        lockHolder.acquireNodeWriteLock( nodeId );
        delegate.deleteNode( nodeId );
    }

    @Override
    public UniquenessConstraint addUniquenessConstraint( long labelId, long propertyKeyId )
            throws DataIntegrityKernelException, ConstraintCreationKernelException
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
    public Iterator<UniquenessConstraint> getConstraints( long labelId )
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.getConstraints( labelId );
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints()
    {
        lockHolder.acquireSchemaReadLock();
        return delegate.getConstraints();
    }

    @Override
    public void dropConstraint( UniquenessConstraint constraint )
    {
        lockHolder.acquireSchemaWriteLock();
        delegate.dropConstraint( constraint );
    }
}
