/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.collection.primitive.PrimitiveIntCollection;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.values.storable.Value;

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
            throws ConstraintValidationException, EntityNotFoundException
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
    public Value nodeSetProperty( KernelStatement statement, long nodeId, int propertyKeyId, Value value )
            throws ConstraintValidationException, EntityNotFoundException, AutoIndexingKernelException,
            InvalidTransactionTypeKernelException
    {
        guard.check( statement );
        return entityWriteDelegate.nodeSetProperty( statement, nodeId, propertyKeyId, value );
    }

    @Override
    public Value relationshipSetProperty( KernelStatement statement, long relationshipId, int propertyKeyId,
            Value value ) throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        guard.check( statement );
        return entityWriteDelegate.relationshipSetProperty( statement, relationshipId, propertyKeyId, value );
    }

    @Override
    public Value graphSetProperty( KernelStatement statement, int propertyKeyId, Value value )
    {
        guard.check( statement );
        return entityWriteDelegate.graphSetProperty( statement, propertyKeyId, value );
    }

    @Override
    public Value nodeRemoveProperty( KernelStatement statement, long nodeId, int propertyKeyId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        guard.check( statement );
        return entityWriteDelegate.nodeRemoveProperty( statement, nodeId, propertyKeyId );
    }

    @Override
    public Value relationshipRemoveProperty( KernelStatement statement,
            long relationshipId,
            int propertyKeyId ) throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        guard.check( statement );
        return entityWriteDelegate.relationshipRemoveProperty( statement, relationshipId, propertyKeyId );
    }

    @Override
    public Value graphRemoveProperty( KernelStatement statement, int propertyKeyId )
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
    public PrimitiveLongIterator indexQuery( KernelStatement statement, IndexDescriptor index,
            IndexQuery[] predicates )
            throws IndexNotFoundKernelException, IndexNotApplicableKernelException
    {
        guard.check( statement );
        return entityReadDelegate.indexQuery( statement, index, predicates );
    }

    @Override
    public long nodeGetFromUniqueIndexSeek( KernelStatement statement, IndexDescriptor index, IndexQuery.ExactPredicate... predicates )
            throws IndexNotFoundKernelException, IndexBrokenKernelException, IndexNotApplicableKernelException
    {
        guard.check( statement );
        return entityReadDelegate.nodeGetFromUniqueIndexSeek( statement, index, predicates );
    }

    @Override
    public long nodesCountIndexed( KernelStatement statement, IndexDescriptor index, long nodeId, Value value )
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
    public Value graphGetProperty( KernelStatement statement, int propertyKeyId )
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
    public Cursor<RelationshipItem> relationshipCursorById( KernelStatement statement, long relId )
            throws EntityNotFoundException
    {
        guard.check( statement );
        return entityReadDelegate.relationshipCursorById( statement, relId );
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursorGetAll( KernelStatement statement )
    {
        guard.check( statement );
        return entityReadDelegate.relationshipCursorGetAll( statement );
    }

    @Override
    public Cursor<RelationshipItem> nodeGetRelationships( KernelStatement statement, NodeItem node,
            Direction direction )
    {
        guard.check( statement );
        return entityReadDelegate.nodeGetRelationships( statement, node, direction );
    }

    @Override
    public Cursor<RelationshipItem> nodeGetRelationships( KernelStatement statement, NodeItem node, Direction direction,
            int[] relTypes )
    {
        guard.check( statement );
        return entityReadDelegate.nodeGetRelationships( statement, node, direction, relTypes );
    }

    @Override
    public Cursor<PropertyItem> nodeGetProperties( KernelStatement statement, NodeItem node )
    {
        guard.check( statement );
        return entityReadDelegate.nodeGetProperties( statement, node );
    }

    @Override
    public Value nodeGetProperty( KernelStatement statement, NodeItem node, int propertyKeyId )
    {
        guard.check( statement );
        return entityReadDelegate.nodeGetProperty( statement, node, propertyKeyId );
    }

    @Override
    public boolean nodeHasProperty( KernelStatement statement, NodeItem node, int propertyKeyId )
    {
        guard.check( statement );
        return entityReadDelegate.nodeHasProperty( statement, node, propertyKeyId );
    }

    @Override
    public PrimitiveIntCollection nodeGetPropertyKeys( KernelStatement statement, NodeItem node )
    {
        guard.check( statement );
        return entityReadDelegate.nodeGetPropertyKeys( statement, node );
    }

    @Override
    public Cursor<PropertyItem> relationshipGetProperties( KernelStatement statement, RelationshipItem relationship )
    {
        guard.check( statement );
        return entityReadDelegate.relationshipGetProperties( statement, relationship );
    }

    @Override
    public Value relationshipGetProperty( KernelStatement statement, RelationshipItem relationship, int propertyKeyId )
    {
        guard.check( statement );
        return entityReadDelegate.relationshipGetProperty( statement, relationship, propertyKeyId );
    }

    @Override
    public boolean relationshipHasProperty( KernelStatement statement, RelationshipItem relationship,
            int propertyKeyId )
    {
        guard.check( statement );
        return entityReadDelegate.relationshipHasProperty( statement, relationship, propertyKeyId );
    }

    @Override
    public PrimitiveIntCollection relationshipGetPropertyKeys( KernelStatement statement,
            RelationshipItem relationship )
    {
        guard.check( statement );
        return entityReadDelegate.relationshipGetPropertyKeys( statement, relationship );
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

    @Override
    public PrimitiveIntSet relationshipTypes( KernelStatement statement, NodeItem nodeItem )
    {
        guard.check( statement );
        return entityReadDelegate.relationshipTypes( statement, nodeItem );
    }

    @Override
    public int degree( KernelStatement statement, NodeItem nodeItem, Direction direction )
    {
        guard.check( statement );
        return entityReadDelegate.degree( statement, nodeItem, direction );
    }

    @Override
    public int degree( KernelStatement statement, NodeItem nodeItem, Direction direction, int relType )
    {
        guard.check( statement );
        return entityReadDelegate.degree( statement, nodeItem, direction, relType );
    }
}
