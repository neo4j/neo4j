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

import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.function.Predicate;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.ObjectUtil;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.kernel.api.constraints.MandatoryNodePropertyConstraint;
import org.neo4j.kernel.api.constraints.MandatoryRelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.cursor.LabelItem;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.MandatoryNodePropertyConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.MandatoryRelationshipPropertyConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.UnableToValidateConstraintKernelException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyConstraintViolationKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.operations.EntityOperations;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.locking.Locks;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.impl.locking.ResourceTypes.INDEX_ENTRY;
import static org.neo4j.kernel.impl.locking.ResourceTypes.indexEntryResourceId;

public class ConstraintEnforcingEntityOperations implements EntityOperations, SchemaWriteOperations
{
    private static final Predicate<NodePropertyConstraint> UNIQUENESS_CONSTRAINT =
            Predicates.instanceOf( UniquenessConstraint.class );

    private final EntityWriteOperations entityWriteOperations;
    private final EntityReadOperations entityReadOperations;
    private final SchemaWriteOperations schemaWriteOperations;
    private final SchemaReadOperations schemaReadOperations;

    public ConstraintEnforcingEntityOperations(
            EntityWriteOperations entityWriteOperations,
            EntityReadOperations entityReadOperations,
            SchemaWriteOperations schemaWriteOperations,
            SchemaReadOperations schemaReadOperations )
    {
        this.entityWriteOperations = entityWriteOperations;
        this.entityReadOperations = entityReadOperations;
        this.schemaWriteOperations = schemaWriteOperations;
        this.schemaReadOperations = schemaReadOperations;
    }

    @Override
    public boolean nodeAddLabel( KernelStatement state, NodeItem node, int labelId )
            throws ConstraintValidationKernelException
    {
        Iterator<NodePropertyConstraint> allConstraints = schemaReadOperations.constraintsGetForLabel( state, labelId );
        Iterator<NodePropertyConstraint> constraints = uniquePropertyConstraints( allConstraints );
        while ( constraints.hasNext() )
        {
            PropertyConstraint constraint = constraints.next();
            int propertyKeyId = constraint.propertyKey();

            try ( Cursor<PropertyItem> properties = node.property( propertyKeyId ) )
            {
                if ( properties.next() )
                {
                    validateNoExistingNodeWithLabelAndProperty( state, labelId, propertyKeyId, properties.get().value(), node.id() );
                }
            }
        }
        return entityWriteOperations.nodeAddLabel( state, node, labelId );
    }

    @Override
    public Property nodeSetProperty( KernelStatement state, NodeItem node, DefinedProperty property )
            throws ConstraintValidationKernelException
    {
        try ( Cursor<LabelItem> labels = node.labels() )
        {
            while ( labels.next() )
            {
                int labelId = labels.get().getAsInt();
                int propertyKeyId = property.propertyKeyId();
                Iterator<NodePropertyConstraint> constraintIterator =
                        uniquePropertyConstraints(
                                schemaReadOperations.constraintsGetForLabelAndPropertyKey( state, labelId,
                                        propertyKeyId ) );
                if ( constraintIterator.hasNext() )
                {
                    validateNoExistingNodeWithLabelAndProperty( state, labelId, property.propertyKeyId(),
                            property.value(), node.id() );
                }
            }
        }

        return entityWriteOperations.nodeSetProperty( state, node, property );
    }

    private void validateNoExistingNodeWithLabelAndProperty( KernelStatement state, int labelId,
            int propertyKeyId, Object value, long modifiedNode )
            throws ConstraintValidationKernelException
    {
        try
        {
            IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
            assertIndexOnline( state, indexDescriptor );
            state.locks().acquireExclusive( INDEX_ENTRY,
                    indexEntryResourceId( labelId, propertyKeyId, ObjectUtil.toString( value ) ) );

            long existing = entityReadOperations.nodeGetFromUniqueIndexSeek( state, indexDescriptor, value );
            if ( existing != NO_SUCH_NODE && existing != modifiedNode )
            {
                throw new UniquePropertyConstraintViolationKernelException( labelId, propertyKeyId, value, existing );
            }
        }
        catch ( IndexNotFoundKernelException | IndexBrokenKernelException e )
        {
            throw new UnableToValidateConstraintKernelException( e );
        }
    }

    private void assertIndexOnline( KernelStatement state, IndexDescriptor indexDescriptor )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        switch ( schemaReadOperations.indexGetState( state, indexDescriptor ) )
        {
            case ONLINE:
                return;
            default:
                throw new IndexBrokenKernelException( schemaReadOperations.indexGetFailure( state, indexDescriptor ) );
        }
    }

    private Iterator<NodePropertyConstraint> uniquePropertyConstraints( Iterator<NodePropertyConstraint> constraints )
    {
        return new FilteringIterator<>( constraints, UNIQUENESS_CONSTRAINT );
    }

    // Simply delegate the rest of the invocations

    @Override
    public void nodeDelete( KernelStatement state, NodeItem node )
    {
        entityWriteOperations.nodeDelete( state, node );
    }

    @Override
    public long relationshipCreate( KernelStatement statement,
            int relationshipTypeId,
            long startNodeId,
            long endNodeId )
            throws EntityNotFoundException
    {
        return entityWriteOperations.relationshipCreate( statement, relationshipTypeId, startNodeId, endNodeId );
    }

    @Override
    public void relationshipDelete( KernelStatement state, RelationshipItem relationship )
    {
        entityWriteOperations.relationshipDelete( state, relationship );
    }

    @Override
    public boolean nodeRemoveLabel( KernelStatement state, NodeItem node, int labelId )
    {
        return entityWriteOperations.nodeRemoveLabel( state, node, labelId );
    }

    @Override
    public Property relationshipSetProperty( KernelStatement state,
            RelationshipItem relationship,
            DefinedProperty property )
    {
        return entityWriteOperations.relationshipSetProperty( state, relationship, property );
    }

    @Override
    public Property graphSetProperty( KernelStatement state, DefinedProperty property )
    {
        return entityWriteOperations.graphSetProperty( state, property );
    }

    @Override
    public Property nodeRemoveProperty( KernelStatement state, NodeItem node, int propertyKeyId )
    {
        return entityWriteOperations.nodeRemoveProperty( state, node, propertyKeyId );
    }

    @Override
    public Property relationshipRemoveProperty( KernelStatement state,
            RelationshipItem relationship,
            int propertyKeyId )
    {
        return entityWriteOperations.relationshipRemoveProperty( state, relationship, propertyKeyId );
    }

    @Override
    public Property graphRemoveProperty( KernelStatement state, int propertyKeyId )
    {
        return entityWriteOperations.graphRemoveProperty( state, propertyKeyId );
    }

    @Override
    public PrimitiveLongIterator nodesGetForLabel( KernelStatement state, int labelId )
    {
        return entityReadOperations.nodesGetForLabel( state, labelId );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexSeek( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        return entityReadOperations.nodesGetFromIndexSeek( state, index, value );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByNumber( KernelStatement statement,
                                                                     IndexDescriptor index,
                                                                     Number lower, boolean includeLower,
                                                                     Number upper, boolean includeUpper )
            throws IndexNotFoundKernelException {
        return entityReadOperations.nodesGetFromIndexRangeSeekByNumber( statement, index, lower, includeLower, upper,
                includeUpper );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByString( KernelStatement statement,
                                                                     IndexDescriptor index,
                                                                     String lower, boolean includeLower,
                                                                     String upper, boolean includeUpper )
            throws IndexNotFoundKernelException {
        return entityReadOperations.nodesGetFromIndexRangeSeekByString( statement, index, lower, includeLower, upper,
                includeUpper );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByPrefix( KernelStatement state,
                                                                     IndexDescriptor index, String prefix )
            throws IndexNotFoundKernelException
    {
        return entityReadOperations.nodesGetFromIndexRangeSeekByPrefix( state, index, prefix );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexScan( KernelStatement state, IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        return entityReadOperations.nodesGetFromIndexScan( state, index );
    }

    @Override
    public long nodeGetFromUniqueIndexSeek(
            KernelStatement state,
            IndexDescriptor index,
            Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        assertIndexOnline( state, index );

        int labelId = index.getLabelId();
        int propertyKeyId = index.getPropertyKeyId();
        String stringVal = "";
        if ( null != value )
        {
            DefinedProperty property = Property.property( propertyKeyId, value );
            stringVal = property.valueAsString();
        }

        // If we find the node - hold a shared lock. If we don't find a node - hold an exclusive lock.
        Locks.Client locks = state.locks();
        long indexEntryId = indexEntryResourceId( labelId, propertyKeyId, stringVal );

        locks.acquireShared( INDEX_ENTRY, indexEntryId );

        long nodeId = entityReadOperations.nodeGetFromUniqueIndexSeek( state, index, value );
        if ( NO_SUCH_NODE == nodeId )
        {
            locks.releaseShared( INDEX_ENTRY, indexEntryId );
            locks.acquireExclusive( INDEX_ENTRY, indexEntryId );

            nodeId = entityReadOperations.nodeGetFromUniqueIndexSeek( state, index, value );
            if ( NO_SUCH_NODE != nodeId ) // we found it under the exclusive lock
            {
                // downgrade to a shared lock
                locks.acquireShared( INDEX_ENTRY, indexEntryId );
                locks.releaseExclusive( INDEX_ENTRY, indexEntryId );
            }
        }
        return nodeId;
    }

    @Override
    public boolean nodeExists( KernelStatement state, long nodeId )
    {
        return entityReadOperations.nodeExists( state, nodeId );
    }

    @Override
    public boolean relationshipExists( KernelStatement statement, long relId )
    {
        return entityReadOperations.relationshipExists( statement, relId );
    }

    @Override
    public boolean nodeHasLabel( KernelStatement state, NodeItem node, int labelId )
    {
        return entityReadOperations.nodeHasLabel( state, node, labelId );
    }

    @Override
    public PrimitiveIntIterator nodeGetLabels( KernelStatement state, NodeItem node )
    {
        return entityReadOperations.nodeGetLabels( state, node );
    }

    @Override
    public PrimitiveIntIterator nodeGetLabels( TxStateHolder txStateHolder,
            StoreStatement storeStatement,
            NodeItem node )
    {
        return entityReadOperations.nodeGetLabels( txStateHolder, storeStatement, node );
    }

    @Override
    public boolean nodeHasProperty( KernelStatement statement,
            NodeItem node,
            int propertyKeyId )
    {
        return entityReadOperations.nodeHasProperty( statement, node, propertyKeyId );
    }

    @Override
    public boolean nodeHasProperty( TxStateHolder txStateHolder, StoreStatement storeStatement,
            NodeItem node, int propertyKeyId )
    {
        return entityReadOperations.nodeHasProperty( txStateHolder, storeStatement, node, propertyKeyId );
    }

    @Override
    public Object nodeGetProperty( KernelStatement state, NodeItem node, int propertyKeyId )
    {
        return entityReadOperations.nodeGetProperty( state, node, propertyKeyId );
    }

    @Override
    public boolean relationshipHasProperty( TxStateHolder txStateHolder,
            StoreStatement storeStatement,
            RelationshipItem relationship,
            int propertyKeyId )
    {
        return entityReadOperations.relationshipHasProperty( txStateHolder, storeStatement, relationship,
                propertyKeyId );
    }

    @Override
    public boolean relationshipHasProperty( KernelStatement state, RelationshipItem relationship, int propertyKeyId )
    {
        return entityReadOperations.relationshipHasProperty( state, relationship, propertyKeyId );
    }

    @Override
    public Object relationshipGetProperty( KernelStatement state, RelationshipItem relationship, int propertyKeyId )
    {
        return entityReadOperations.relationshipGetProperty( state, relationship, propertyKeyId );
    }

    @Override
    public boolean graphHasProperty( KernelStatement state, int propertyKeyId )
    {
        return entityReadOperations.graphHasProperty( state, propertyKeyId );
    }

    @Override
    public Object graphGetProperty( KernelStatement state, int propertyKeyId )
    {
        return entityReadOperations.graphGetProperty( state, propertyKeyId );
    }

    @Override
    public PrimitiveIntIterator nodeGetPropertyKeys( KernelStatement state, NodeItem node )
    {
        return entityReadOperations.nodeGetPropertyKeys( state, node );
    }

    @Override
    public PrimitiveIntIterator relationshipGetPropertyKeys( KernelStatement state, RelationshipItem relationship )
    {
        return entityReadOperations.relationshipGetPropertyKeys( state, relationship );
    }

    @Override
    public PrimitiveIntIterator graphGetPropertyKeys( KernelStatement state )
    {
        return entityReadOperations.graphGetPropertyKeys( state );
    }

    @Override
    public RelationshipIterator nodeGetRelationships( KernelStatement statement, NodeItem node, Direction direction,
            int[] relTypes )
    {
        return entityReadOperations.nodeGetRelationships( statement, node, direction, relTypes );
    }

    @Override
    public RelationshipIterator nodeGetRelationships( KernelStatement statement, NodeItem node, Direction direction )
    {
        return entityReadOperations.nodeGetRelationships( statement, node, direction );
    }

    @Override
    public int nodeGetDegree( KernelStatement statement,
            NodeItem node,
            Direction direction,
            int relType ) throws EntityNotFoundException
    {
        return entityReadOperations.nodeGetDegree( statement, node, direction, relType );
    }

    @Override
    public int nodeGetDegree( KernelStatement statement,
            NodeItem node,
            Direction direction ) throws EntityNotFoundException
    {
        return entityReadOperations.nodeGetDegree( statement, node, direction );
    }

    @Override
    public PrimitiveIntIterator nodeGetRelationshipTypes( KernelStatement statement,
            NodeItem node ) throws EntityNotFoundException
    {
        return entityReadOperations.nodeGetRelationshipTypes( statement, node );
    }

    @Override
    public long nodeCreate( KernelStatement statement )
    {
        return entityWriteOperations.nodeCreate( statement );
    }

    @Override
    public PrimitiveLongIterator nodesGetAll( KernelStatement state )
    {
        return entityReadOperations.nodesGetAll( state );
    }

    @Override
    public PrimitiveLongIterator relationshipsGetAll( KernelStatement state )
    {
        return entityReadOperations.relationshipsGetAll( state );
    }

    @Override
    public <EXCEPTION extends Exception> void relationshipVisit( KernelStatement statement,
            long relId, RelationshipVisitor<EXCEPTION> visitor )
            throws EntityNotFoundException, EXCEPTION
    {
        entityReadOperations.relationshipVisit( statement, relId, visitor );
    }

    @Override
    public Cursor<NodeItem> nodeCursor( KernelStatement statement, long nodeId )
    {
        return entityReadOperations.nodeCursor( statement, nodeId );
    }

    @Override
    public Cursor<NodeItem> nodeCursor( TxStateHolder txStateHolder, StoreStatement statement, long nodeId )
    {
        return entityReadOperations.nodeCursor( txStateHolder, statement, nodeId );
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursor( KernelStatement statement, long relId )
    {
        return entityReadOperations.relationshipCursor( statement, relId );
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursor( TxStateHolder txStateHolder, StoreStatement statement, long relId )
    {
        return entityReadOperations.relationshipCursor( txStateHolder, statement, relId );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetAll( KernelStatement statement )
    {
        return entityReadOperations.nodeCursorGetAll( statement );
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursorGetAll( KernelStatement statement )
    {
        return entityReadOperations.relationshipCursorGetAll( statement );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetForLabel( KernelStatement statement, int labelId )
    {
        return entityReadOperations.nodeCursorGetForLabel( statement, labelId );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexSeek( KernelStatement statement, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        return entityReadOperations.nodeCursorGetFromIndexSeek( statement, index, value );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexScan( KernelStatement statement, IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        return entityReadOperations.nodeCursorGetFromIndexScan( statement, index );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexSeekByPrefix( KernelStatement statement,
            IndexDescriptor index,
            String prefix )
            throws IndexNotFoundKernelException
    {
        return entityReadOperations.nodeCursorGetFromIndexSeekByPrefix( statement, index, prefix );
    }
    
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByNumber( KernelStatement statement,
                                                               IndexDescriptor index,
                                                               Number lower, boolean includeLower,
                                                               Number upper, boolean includeUpper )
            throws IndexNotFoundKernelException

    {
        return entityReadOperations.nodeCursorGetFromIndexRangeSeekByNumber( statement, index, lower, includeLower,
                upper, includeUpper );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByString( KernelStatement statement,
                                                               IndexDescriptor index,
                                                               String lower, boolean includeLower,
                                                               String upper, boolean includeUpper )
            throws IndexNotFoundKernelException

    {
        return entityReadOperations.nodeCursorGetFromIndexRangeSeekByString( statement, index, lower, includeLower,
                upper, includeUpper );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByPrefix( KernelStatement statement,
                                                               IndexDescriptor index,
                                                               String prefix ) throws IndexNotFoundKernelException
    {
        return entityReadOperations.nodeCursorGetFromIndexRangeSeekByPrefix( statement, index, prefix );
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromUniqueIndexSeek( KernelStatement statement,
            IndexDescriptor index,
            Object value ) throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        return entityReadOperations.nodeCursorGetFromUniqueIndexSeek( statement, index, value );
    }

    @Override
    public IndexDescriptor indexCreate( KernelStatement state, int labelId, int propertyKeyId )
            throws AlreadyIndexedException, AlreadyConstrainedException
    {
        return schemaWriteOperations.indexCreate( state, labelId, propertyKeyId );
    }

    @Override
    public void indexDrop( KernelStatement state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        schemaWriteOperations.indexDrop( state, descriptor );
    }

    @Override
    public void uniqueIndexDrop( KernelStatement state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        schemaWriteOperations.uniqueIndexDrop( state, descriptor );
    }

    @Override
    public UniquenessConstraint uniquePropertyConstraintCreate( KernelStatement state, int labelId, int propertyKeyId )
            throws AlreadyConstrainedException, CreateConstraintFailureException, AlreadyIndexedException
    {
        return schemaWriteOperations.uniquePropertyConstraintCreate( state, labelId, propertyKeyId );
    }

    @Override
    public MandatoryNodePropertyConstraint mandatoryNodePropertyConstraintCreate( KernelStatement state, int labelId,
            int propertyKeyId ) throws AlreadyConstrainedException, CreateConstraintFailureException
    {
        PrimitiveLongIterator nodes = nodesGetForLabel( state, labelId );
        while ( nodes.hasNext() )
        {
            long nodeId = nodes.next();
            try ( Cursor<NodeItem> node = nodeCursor( state, nodeId ) )
            {
                if ( node.next() )
                {
                    if ( !nodeHasProperty( state, node.get(), propertyKeyId ) )
                    {
                        MandatoryNodePropertyConstraint constraint = new MandatoryNodePropertyConstraint( labelId,
                                propertyKeyId );
                        throw new CreateConstraintFailureException( constraint,
                                new MandatoryNodePropertyConstraintVerificationFailedKernelException( constraint,
                                        nodeId ) );
                    }
                }
            }
        }
                    
        return schemaWriteOperations.mandatoryNodePropertyConstraintCreate( state, labelId, propertyKeyId );
    }

    @Override
    public MandatoryRelationshipPropertyConstraint mandatoryRelationshipPropertyConstraintCreate( KernelStatement state,
            int relTypeId, int propertyKeyId ) throws AlreadyConstrainedException, CreateConstraintFailureException
    {
        PrimitiveLongIterator relationships = relationshipsGetAll( state );
        while ( relationships.hasNext() )
        {
            long relationshipId = relationships.next();
            try ( Cursor<RelationshipItem> relationship = relationshipCursor( state, relationshipId ) )
            {
                if ( relationship.next() )
                {
                    if ( relationship.get().type() == relTypeId && !relationshipHasProperty( state, relationship.get(),
                            propertyKeyId ) )
                    {
                        MandatoryRelationshipPropertyConstraint constraint = new MandatoryRelationshipPropertyConstraint(

                                relTypeId,
                                propertyKeyId );
                        throw new CreateConstraintFailureException( constraint,
                                new MandatoryRelationshipPropertyConstraintVerificationFailedKernelException(
                                        constraint,
                                        relationshipId ) );
                    }
                }
            }
        }

        return schemaWriteOperations.mandatoryRelationshipPropertyConstraintCreate( state, relTypeId, propertyKeyId );
    }

    @Override
    public void constraintDrop( KernelStatement state, NodePropertyConstraint constraint )
            throws DropConstraintFailureException
    {
        schemaWriteOperations.constraintDrop( state, constraint );
    }

    @Override
    public void constraintDrop( KernelStatement state, RelationshipPropertyConstraint constraint )
            throws DropConstraintFailureException
    {
        schemaWriteOperations.constraintDrop( state, constraint );
    }
}
