/*
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
package org.neo4j.kernel.impl.api;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.api.store.StoreStatement;

public class GuardingStatementOperations implements
        EntityWriteOperations,
        EntityReadOperations
{
    private final EntityWriteOperations entityWriteDelegate;
    private final EntityReadOperations entityReadDelegate;
    private final Guard guard;

    public GuardingStatementOperations(
            EntityWriteOperations entityWriteDelegate,
            EntityReadOperations entityReadDelegate,
            Guard guard )
    {
        this.entityWriteDelegate = entityWriteDelegate;
        this.entityReadDelegate = entityReadDelegate;
        this.guard = guard;
    }

    @Override
    public long relationshipCreate( KernelStatement statement,
            int relationshipTypeId,
            long startNodeId,
            long endNodeId )
            throws EntityNotFoundException
    {
        guard.check();
        return entityWriteDelegate.relationshipCreate( statement, relationshipTypeId, startNodeId, endNodeId );
    }

    @Override
    public long nodeCreate( KernelStatement statement )
    {
        guard.check();
        return entityWriteDelegate.nodeCreate( statement );
    }

    @Override
    public void nodeDelete( KernelStatement state, long nodeId ) throws EntityNotFoundException
    {
        guard.check();
        entityWriteDelegate.nodeDelete( state, nodeId );
    }

    @Override
    public void relationshipDelete( KernelStatement state, long relationshipId ) throws EntityNotFoundException
    {
        guard.check();
        entityWriteDelegate.relationshipDelete( state, relationshipId );
    }

    @Override
    public boolean nodeAddLabel( KernelStatement state, long nodeId, int labelId )
            throws ConstraintValidationKernelException, EntityNotFoundException
    {
        guard.check();
        return entityWriteDelegate.nodeAddLabel( state, nodeId, labelId );
    }

    @Override
    public boolean nodeRemoveLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException
    {
        guard.check();
        return entityWriteDelegate.nodeRemoveLabel( state, nodeId, labelId );
    }

    @Override
    public Property nodeSetProperty( KernelStatement state, long nodeId, DefinedProperty property )
            throws ConstraintValidationKernelException, EntityNotFoundException
    {
        guard.check();
        return entityWriteDelegate.nodeSetProperty( state, nodeId, property );
    }

    @Override
    public Property relationshipSetProperty( KernelStatement state,
            long relationshipId,
            DefinedProperty property ) throws EntityNotFoundException
    {
        guard.check();
        return entityWriteDelegate.relationshipSetProperty( state, relationshipId, property );
    }

    @Override
    public Property graphSetProperty( KernelStatement state, DefinedProperty property )
    {
        guard.check();
        return entityWriteDelegate.graphSetProperty( state, property );
    }

    @Override
    public Property nodeRemoveProperty( KernelStatement state, long nodeId, int propertyKeyId )
            throws EntityNotFoundException
    {
        guard.check();
        return entityWriteDelegate.nodeRemoveProperty( state, nodeId, propertyKeyId );
    }

    @Override
    public Property relationshipRemoveProperty( KernelStatement state,
            long relationshipId,
            int propertyKeyId ) throws EntityNotFoundException
    {
        guard.check();
        return entityWriteDelegate.relationshipRemoveProperty( state, relationshipId, propertyKeyId );
    }

    @Override
    public Property graphRemoveProperty( KernelStatement state, int propertyKeyId )
    {
        guard.check();
        return entityWriteDelegate.graphRemoveProperty( state, propertyKeyId );
    }

    @Override
    public PrimitiveLongIterator nodesGetForLabel( KernelStatement state, int labelId )
    {
        guard.check();
        return entityReadDelegate.nodesGetForLabel( state, labelId );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexSeek( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        guard.check();
        return entityReadDelegate.nodesGetFromIndexSeek( state, index, value );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByNumber( KernelStatement state,
            IndexDescriptor index,
            Number lower, boolean includeLower,
            Number upper, boolean includeUpper )
            throws IndexNotFoundKernelException

    {
        guard.check();
        return entityReadDelegate.nodesGetFromIndexRangeSeekByNumber( state, index, lower, includeLower, upper,
                includeUpper );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByString( KernelStatement state,
            IndexDescriptor index,
            String lower, boolean includeLower,
            String upper, boolean includeUpper )
            throws IndexNotFoundKernelException

    {
        guard.check();
        return entityReadDelegate.nodesGetFromIndexRangeSeekByString( state, index, lower, includeLower, upper,
                includeUpper );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByPrefix( KernelStatement state, IndexDescriptor index,
            String prefix ) throws IndexNotFoundKernelException
    {
        guard.check();
        return entityReadDelegate.nodesGetFromIndexRangeSeekByPrefix( state, index, prefix );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexScan( KernelStatement state, IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        guard.check();
        return entityReadDelegate.nodesGetFromIndexScan( state, index );
    }

    @Override
    public long nodeGetFromUniqueIndexSeek( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        guard.check();
        return entityReadDelegate.nodeGetFromUniqueIndexSeek( state, index, value );
    }

    @Override
    public boolean graphHasProperty( KernelStatement state, int propertyKeyId )
    {
        guard.check();
        return entityReadDelegate.graphHasProperty( state, propertyKeyId );
    }

    @Override
    public Object graphGetProperty( KernelStatement state, int propertyKeyId )
    {
        guard.check();
        return entityReadDelegate.graphGetProperty( state, propertyKeyId );
    }

    @Override
    public PrimitiveIntIterator graphGetPropertyKeys( KernelStatement state )
    {
        guard.check();
        return entityReadDelegate.graphGetPropertyKeys( state );
    }

    @Override
    public PrimitiveLongIterator nodesGetAll( KernelStatement state )
    {
        guard.check();
        return entityReadDelegate.nodesGetAll( state );
    }

    @Override
    public PrimitiveLongIterator relationshipsGetAll( KernelStatement state )
    {
        guard.check();
        return entityReadDelegate.relationshipsGetAll( state );
    }

    @Override
    public <EXCEPTION extends Exception> void relationshipVisit( KernelStatement statement, long relId,
            RelationshipVisitor<EXCEPTION> visitor )
            throws EntityNotFoundException, EXCEPTION
    {
        guard.check();
        entityReadDelegate.relationshipVisit( statement, relId, visitor );
    }

    @Override
    public Cursor<NodeItem> nodeCursorById( KernelStatement statement, long nodeId ) throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.nodeCursorById( statement, nodeId );
    }

    @Override
    public Cursor<NodeItem> nodeCursor( KernelStatement statement, long nodeId )
    {
        guard.check();
        return entityReadDelegate.nodeCursor( statement, nodeId );
    }

    @Override
    public Cursor<NodeItem> nodeCursor( TxStateHolder txStateHolder, StoreStatement statement, long nodeId )
    {
        guard.check();
        return entityReadDelegate.nodeCursor( txStateHolder, statement, nodeId );
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursorById( KernelStatement statement, long relId )
            throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.relationshipCursorById( statement, relId );
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursor( KernelStatement statement, long relId )
    {
        guard.check();
        return entityReadDelegate.relationshipCursor( statement, relId );
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursor( TxStateHolder txStateHolder,
            StoreStatement statement,
            long relId )
    {
        guard.check();
        return entityReadDelegate.relationshipCursor( txStateHolder, statement, relId );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetAll( KernelStatement statement )
    {
        guard.check();
        return entityReadDelegate.nodeCursorGetAll( statement );
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursorGetAll( KernelStatement statement )
    {
        guard.check();
        return entityReadDelegate.relationshipCursorGetAll( statement );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetForLabel( KernelStatement statement, int labelId )
    {
        guard.check();
        return entityReadDelegate.nodeCursorGetForLabel( statement, labelId );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexSeek( KernelStatement statement, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        guard.check();
        return entityReadDelegate.nodeCursorGetFromIndexSeek( statement, index, value );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexScan( KernelStatement statement, IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        guard.check();
        return entityReadDelegate.nodeCursorGetFromIndexScan( statement, index );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByNumber( KernelStatement statement,
            IndexDescriptor index,
            Number lower, boolean includeLower,
            Number upper, boolean includeUpper )
            throws IndexNotFoundKernelException
    {
        guard.check();
        return entityReadDelegate.nodeCursorGetFromIndexRangeSeekByNumber( statement, index, lower, includeLower, upper,
                includeUpper );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByString( KernelStatement statement,
            IndexDescriptor index,
            String lower, boolean includeLower,
            String upper, boolean includeUpper )
            throws IndexNotFoundKernelException
    {
        guard.check();
        return entityReadDelegate.nodeCursorGetFromIndexRangeSeekByString( statement, index, lower, includeLower, upper,
                includeUpper );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexSeekByPrefix( KernelStatement statement,
            IndexDescriptor index,
            String prefix ) throws IndexNotFoundKernelException
    {
        guard.check();
        return entityReadDelegate.nodeCursorGetFromIndexSeekByPrefix( statement, index, prefix );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByPrefix( KernelStatement statement,
            IndexDescriptor index,
            String prefix ) throws IndexNotFoundKernelException
    {
        guard.check();
        return entityReadDelegate.nodeCursorGetFromIndexRangeSeekByPrefix( statement, index, prefix );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromUniqueIndexSeek( KernelStatement statement,
            IndexDescriptor index,
            Object value ) throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        guard.check();
        return entityReadDelegate.nodeCursorGetFromUniqueIndexSeek( statement, index, value );
    }

    private static class GuardedRelationshipIterator implements RelationshipIterator
    {
        private final Guard guard;
        private final RelationshipIterator iterator;

        public GuardedRelationshipIterator( Guard guard, RelationshipIterator iterator )
        {
            this.guard = guard;
            this.iterator = iterator;
        }

        @Override
        public <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId,
                RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION
        {
            guard.check();
            return iterator.relationshipVisit( relationshipId, visitor );
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public long next()
        {
            return iterator.next();
        }
    }
}
