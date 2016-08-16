/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;

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
        guard.check( statement );
        return entityWriteDelegate.relationshipCreate( statement, relationshipTypeId, startNodeId, endNodeId );
    }

    @Override
    public long nodeCreate( KernelStatement statement )
    {
        guard.check( statement );
        return entityWriteDelegate.nodeCreate( statement );
    }

    @Override
    public void nodeDelete( KernelStatement statement, long nodeId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        guard.check( statement );
        entityWriteDelegate.nodeDelete( statement, nodeId );
    }

    @Override
    public void relationshipDelete( KernelStatement statement, long relationshipId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        guard.check( statement );
        entityWriteDelegate.relationshipDelete( statement, relationshipId );
    }

    @Override
    public int nodeDetachDelete( KernelStatement statement, long nodeId ) throws KernelException
    {
        guard.check( statement );
        return entityWriteDelegate.nodeDetachDelete( statement, nodeId );
    }

    @Override
    public boolean nodeAddLabel( KernelStatement statement, long nodeId, int labelId )
            throws ConstraintValidationKernelException, EntityNotFoundException
    {
        guard.check( statement );
        return entityWriteDelegate.nodeAddLabel( statement, nodeId, labelId );
    }

    @Override
    public boolean nodeRemoveLabel( KernelStatement statement, long nodeId, int labelId ) throws EntityNotFoundException
    {
        guard.check( statement );
        return entityWriteDelegate.nodeRemoveLabel( statement, nodeId, labelId );
    }

    @Override
    public Property nodeSetProperty( KernelStatement statement, long nodeId, DefinedProperty property )
            throws ConstraintValidationKernelException, EntityNotFoundException, AutoIndexingKernelException,
            InvalidTransactionTypeKernelException
    {
        guard.check( statement );
        return entityWriteDelegate.nodeSetProperty( statement, nodeId, property );
    }

    @Override
    public Property relationshipSetProperty( KernelStatement statement,
            long relationshipId,
            DefinedProperty property ) throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        guard.check( statement );
        return entityWriteDelegate.relationshipSetProperty( statement, relationshipId, property );
    }

    @Override
    public Property graphSetProperty( KernelStatement statement, DefinedProperty property )
    {
        guard.check( statement );
        return entityWriteDelegate.graphSetProperty( statement, property );
    }

    @Override
    public Property nodeRemoveProperty( KernelStatement statement, long nodeId, int propertyKeyId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        guard.check( statement );
        return entityWriteDelegate.nodeRemoveProperty( statement, nodeId, propertyKeyId );
    }

    @Override
    public Property relationshipRemoveProperty( KernelStatement statement,
            long relationshipId,
            int propertyKeyId ) throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        guard.check( statement );
        return entityWriteDelegate.relationshipRemoveProperty( statement, relationshipId, propertyKeyId );
    }

    @Override
    public Property graphRemoveProperty( KernelStatement statement, int propertyKeyId )
    {
        guard.check( statement );
        return entityWriteDelegate.graphRemoveProperty( statement, propertyKeyId );
    }

    @Override
    public PrimitiveLongIterator nodesGetForLabel( KernelStatement statement, int labelId )
    {
        guard.check( statement );
        return entityReadDelegate.nodesGetForLabel( statement, labelId );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexSeek( KernelStatement statement, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        guard.check( statement );
        return entityReadDelegate.nodesGetFromIndexSeek( statement, index, value );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByNumber( KernelStatement statement,
            IndexDescriptor index,
            Number lower, boolean includeLower,
            Number upper, boolean includeUpper )
            throws IndexNotFoundKernelException

    {
        guard.check( statement );
        return entityReadDelegate.nodesGetFromIndexRangeSeekByNumber( statement, index, lower, includeLower, upper,
                includeUpper );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByString( KernelStatement statement,
            IndexDescriptor index,
            String lower, boolean includeLower,
            String upper, boolean includeUpper )
            throws IndexNotFoundKernelException

    {
        guard.check( statement );
        return entityReadDelegate.nodesGetFromIndexRangeSeekByString( statement, index, lower, includeLower, upper,
                includeUpper );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByPrefix( KernelStatement statement, IndexDescriptor index,
            String prefix ) throws IndexNotFoundKernelException
    {
        guard.check( statement );
        return entityReadDelegate.nodesGetFromIndexRangeSeekByPrefix( statement, index, prefix );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexScan( KernelStatement statement, IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        guard.check( statement );
        return entityReadDelegate.nodesGetFromIndexScan( statement, index );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexContainsScan( KernelStatement statement, IndexDescriptor index,
            String term ) throws IndexNotFoundKernelException
    {
        guard.check( statement );
        return entityReadDelegate.nodesGetFromIndexContainsScan( statement, index, term );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexEndsWithScan( KernelStatement statement, IndexDescriptor index,
            String suffix ) throws IndexNotFoundKernelException
    {
        guard.check( statement );
        return entityReadDelegate.nodesGetFromIndexEndsWithScan( statement, index, suffix );
    }

    @Override
    public long nodeGetFromUniqueIndexSeek( KernelStatement statement, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        guard.check( statement );
        return entityReadDelegate.nodeGetFromUniqueIndexSeek( statement, index, value );
    }

    @Override
    public long nodesCountIndexed( KernelStatement statement, IndexDescriptor index, long nodeId, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        guard.check( statement );
        return entityReadDelegate.nodesCountIndexed( statement, index, nodeId, value );
    }

    @Override
    public boolean graphHasProperty( KernelStatement statement, int propertyKeyId )
    {
        guard.check( statement );
        return entityReadDelegate.graphHasProperty( statement, propertyKeyId );
    }

    @Override
    public Object graphGetProperty( KernelStatement statement, int propertyKeyId )
    {
        guard.check( statement );
        return entityReadDelegate.graphGetProperty( statement, propertyKeyId );
    }

    @Override
    public PrimitiveIntIterator graphGetPropertyKeys( KernelStatement statement )
    {
        guard.check( statement );
        return entityReadDelegate.graphGetPropertyKeys( statement );
    }

    @Override
    public PrimitiveLongIterator nodesGetAll( KernelStatement statement )
    {
        guard.check( statement );
        return entityReadDelegate.nodesGetAll( statement );
    }

    @Override
    public PrimitiveLongIterator relationshipsGetAll( KernelStatement statement )
    {
        guard.check( statement );
        return entityReadDelegate.relationshipsGetAll( statement );
    }

    @Override
    public <EXCEPTION extends Exception> void relationshipVisit( KernelStatement statement, long relId,
            RelationshipVisitor<EXCEPTION> visitor )
            throws EntityNotFoundException, EXCEPTION
    {
        guard.check( statement );
        entityReadDelegate.relationshipVisit( statement, relId, visitor );
    }

    @Override
    public Cursor<NodeItem> nodeCursorById( KernelStatement statement, long nodeId ) throws EntityNotFoundException
    {
        guard.check( statement );
        return entityReadDelegate.nodeCursorById( statement, nodeId );
    }

    @Override
    public Cursor<NodeItem> nodeCursor( KernelStatement statement, long nodeId )
    {
        guard.check( statement );
        return entityReadDelegate.nodeCursor( statement, nodeId );
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursorById( KernelStatement statement, long relId )
            throws EntityNotFoundException
    {
        guard.check( statement );
        return entityReadDelegate.relationshipCursorById( statement, relId );
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursor( KernelStatement statement, long relId )
    {
        guard.check( statement );
        return entityReadDelegate.relationshipCursor( statement, relId );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetAll( KernelStatement statement )
    {
        guard.check( statement );
        return entityReadDelegate.nodeCursorGetAll( statement );
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursorGetAll( KernelStatement statement )
    {
        guard.check( statement );
        return entityReadDelegate.relationshipCursorGetAll( statement );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetForLabel( KernelStatement statement, int labelId )
    {
        guard.check( statement );
        return entityReadDelegate.nodeCursorGetForLabel( statement, labelId );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexSeek( KernelStatement statement, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        guard.check( statement );
        return entityReadDelegate.nodeCursorGetFromIndexSeek( statement, index, value );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexScan( KernelStatement statement, IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        guard.check( statement );
        return entityReadDelegate.nodeCursorGetFromIndexScan( statement, index );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByNumber( KernelStatement statement,
            IndexDescriptor index,
            Number lower, boolean includeLower,
            Number upper, boolean includeUpper )
            throws IndexNotFoundKernelException
    {
        guard.check( statement );
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
        guard.check( statement );
        return entityReadDelegate.nodeCursorGetFromIndexRangeSeekByString( statement, index, lower, includeLower, upper,
                includeUpper );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexSeekByPrefix( KernelStatement statement,
            IndexDescriptor index,
            String prefix ) throws IndexNotFoundKernelException
    {
        guard.check( statement );
        return entityReadDelegate.nodeCursorGetFromIndexSeekByPrefix( statement, index, prefix );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByPrefix( KernelStatement statement,
            IndexDescriptor index,
            String prefix ) throws IndexNotFoundKernelException
    {
        guard.check( statement );
        return entityReadDelegate.nodeCursorGetFromIndexRangeSeekByPrefix( statement, index, prefix );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromUniqueIndexSeek( KernelStatement statement,
            IndexDescriptor index,
            Object value ) throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        guard.check( statement );
        return entityReadDelegate.nodeCursorGetFromUniqueIndexSeek( statement, index, value );
    }

    @Override
    public long nodesGetCount( KernelStatement statement )
    {
        guard.check( statement );
        return entityReadDelegate.nodesGetCount( statement );
    }

    @Override
    public long relationshipsGetCount( KernelStatement statement )
    {
        guard.check( statement );
        return entityReadDelegate.relationshipsGetCount( statement );
    }

    @Override
    public boolean nodeExists( KernelStatement statement, long id )
    {
        guard.check( statement );
        return entityReadDelegate.nodeExists( statement, id );
    }
}
