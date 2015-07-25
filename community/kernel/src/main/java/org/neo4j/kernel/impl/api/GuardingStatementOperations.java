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
import org.neo4j.graphdb.Direction;
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
    public void nodeDelete( KernelStatement state, NodeItem node )
    {
        guard.check();
        entityWriteDelegate.nodeDelete( state, node );
    }

    @Override
    public void relationshipDelete( KernelStatement state, RelationshipItem relationship )
    {
        guard.check();
        entityWriteDelegate.relationshipDelete( state, relationship );
    }

    @Override
    public boolean nodeAddLabel( KernelStatement state, NodeItem node, int labelId )
            throws ConstraintValidationKernelException
    {
        guard.check();
        return entityWriteDelegate.nodeAddLabel( state, node, labelId );
    }

    @Override
    public boolean nodeRemoveLabel( KernelStatement state, NodeItem node, int labelId )
    {
        guard.check();
        return entityWriteDelegate.nodeRemoveLabel( state, node, labelId );
    }

    @Override
    public Property nodeSetProperty( KernelStatement state, NodeItem node, DefinedProperty property )
            throws ConstraintValidationKernelException
    {
        guard.check();
        return entityWriteDelegate.nodeSetProperty( state, node, property );
    }

    @Override
    public Property relationshipSetProperty( KernelStatement state,
            RelationshipItem relationship,
            DefinedProperty property )
    {
        guard.check();
        return entityWriteDelegate.relationshipSetProperty( state, relationship, property );
    }

    @Override
    public Property graphSetProperty( KernelStatement state, DefinedProperty property )
    {
        guard.check();
        return entityWriteDelegate.graphSetProperty( state, property );
    }

    @Override
    public Property nodeRemoveProperty( KernelStatement state, NodeItem node, int propertyKeyId )
    {
        guard.check();
        return entityWriteDelegate.nodeRemoveProperty( state, node, propertyKeyId );
    }

    @Override
    public Property relationshipRemoveProperty( KernelStatement state,
            RelationshipItem relationship,
            int propertyKeyId )
    {
        guard.check();
        return entityWriteDelegate.relationshipRemoveProperty( state, relationship, propertyKeyId );
    }

    @Override
    public Property graphRemoveProperty( KernelStatement state, int propertyKeyId )
    {
        guard.check();
        return entityWriteDelegate.graphRemoveProperty( state, propertyKeyId );
    }

    @Override
    public boolean nodeExists( KernelStatement state, long nodeId )
    {
        guard.check();
        return entityReadDelegate.nodeExists( state, nodeId );
    }

    @Override
    public boolean relationshipExists( KernelStatement statement, long relId )
    {
        guard.check();
        return entityReadDelegate.relationshipExists( statement, relId );
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
    public boolean nodeHasLabel( KernelStatement state, NodeItem node, int labelId )
    {
        guard.check();
        return entityReadDelegate.nodeHasLabel( state, node, labelId );
    }

    @Override
    public PrimitiveIntIterator nodeGetLabels( KernelStatement state, NodeItem node )
    {
        guard.check();
        return entityReadDelegate.nodeGetLabels( state, node );
    }

    @Override
    public PrimitiveIntIterator nodeGetLabels( TxStateHolder txStateHolder,
            StoreStatement storeStatement,
            NodeItem node )
    {
        guard.check();
        return entityReadDelegate.nodeGetLabels( txStateHolder, storeStatement, node );
    }

    @Override
    public boolean nodeHasProperty( KernelStatement statement, NodeItem node, int propertyKeyId )
    {
        guard.check();
        return entityReadDelegate.nodeHasProperty( statement, node, propertyKeyId );
    }

    @Override
    public boolean nodeHasProperty( TxStateHolder txStateHolder, StoreStatement storeStatement,
            NodeItem node, int propertyKeyId )
    {
        guard.check();
        return entityReadDelegate.nodeHasProperty( txStateHolder, storeStatement, node, propertyKeyId );
    }

    @Override
    public Object nodeGetProperty( KernelStatement state, NodeItem node, int propertyKeyId )
    {
        guard.check();
        return entityReadDelegate.nodeGetProperty( state, node, propertyKeyId );
    }

    @Override
    public boolean relationshipHasProperty( KernelStatement state, RelationshipItem relationship, int propertyKeyId )
    {
        guard.check();
        return entityReadDelegate.relationshipHasProperty( state, relationship, propertyKeyId );
    }

    @Override
    public boolean relationshipHasProperty( TxStateHolder txStateHolder, StoreStatement storeStatement,
            RelationshipItem relationship, int propertyKeyId )
    {
        guard.check();
        return entityReadDelegate.relationshipHasProperty( txStateHolder, storeStatement, relationship, propertyKeyId );
    }

    @Override
    public Object relationshipGetProperty( KernelStatement state, RelationshipItem relationship, int propertyKeyId )
    {
        guard.check();
        return entityReadDelegate.relationshipGetProperty( state, relationship, propertyKeyId );
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
    public PrimitiveIntIterator nodeGetPropertyKeys( KernelStatement state, NodeItem node )
    {
        guard.check();
        return entityReadDelegate.nodeGetPropertyKeys( state, node );
    }

    @Override
    public PrimitiveIntIterator relationshipGetPropertyKeys( KernelStatement state, RelationshipItem relationship )
    {
        guard.check();
        return entityReadDelegate.relationshipGetPropertyKeys( state, relationship );
    }

    @Override
    public PrimitiveIntIterator graphGetPropertyKeys( KernelStatement state )
    {
        guard.check();
        return entityReadDelegate.graphGetPropertyKeys( state );
    }

    @Override
    public RelationshipIterator nodeGetRelationships( KernelStatement statement, NodeItem node, Direction direction,
            int[] relTypes )
    {
        guard.check();
        return new GuardedRelationshipIterator( guard, entityReadDelegate.nodeGetRelationships( statement, node,
                direction, relTypes ) );
    }

    @Override
    public RelationshipIterator nodeGetRelationships( KernelStatement statement, NodeItem node, Direction direction )
    {
        guard.check();
        return new GuardedRelationshipIterator( guard,
                entityReadDelegate.nodeGetRelationships( statement, node, direction ) );
    }

    @Override
    public int nodeGetDegree( KernelStatement statement,
            NodeItem node,
            Direction direction,
            int relType ) throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.nodeGetDegree( statement, node, direction, relType );
    }

    @Override
    public int nodeGetDegree( KernelStatement statement,
            NodeItem node,
            Direction direction ) throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.nodeGetDegree( statement, node, direction );
    }

    @Override
    public PrimitiveIntIterator nodeGetRelationshipTypes( KernelStatement statement,
            NodeItem node ) throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.nodeGetRelationshipTypes( statement, node );
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
    public Cursor<RelationshipItem> relationshipCursor( KernelStatement statement, long relId )
    {
        guard.check();
        return entityReadDelegate.relationshipCursor( statement, relId );
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursor( TxStateHolder txStateHolder, StoreStatement statement, long relId )
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
