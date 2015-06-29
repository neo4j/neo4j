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
import org.neo4j.function.Predicate;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.kernel.api.constraints.MandatoryPropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.cursor.NodeCursor;
import org.neo4j.kernel.api.cursor.RelationshipCursor;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException.Evidence;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
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
    public boolean nodeAddLabel( KernelStatement state, long nodeId, int labelId )
            throws EntityNotFoundException, ConstraintValidationKernelException
    {
        Iterator<PropertyConstraint> constraints = uniquePropertyConstraints(
                schemaReadOperations.constraintsGetForLabel( state, labelId ) );
        while ( constraints.hasNext() )
        {
            PropertyConstraint constraint = constraints.next();
            int propertyKeyId = constraint.propertyKeyId();
            Object value = entityReadOperations.nodeGetProperty( state, nodeId, propertyKeyId );
            if ( value != null )
            {
                DefinedProperty property = Property.property( propertyKeyId, value );
                validateNoExistingNodeWithLabelAndProperty( state, labelId, property, nodeId );
            }
        }
        return entityWriteOperations.nodeAddLabel( state, nodeId, labelId );
    }

    @Override
    public Property nodeSetProperty( KernelStatement state, long nodeId, DefinedProperty property )
            throws EntityNotFoundException, ConstraintValidationKernelException
    {
        PrimitiveIntIterator labelIds = entityReadOperations.nodeGetLabels( state, nodeId );
        while ( labelIds.hasNext() )
        {
            int labelId = labelIds.next();
            int propertyKeyId = property.propertyKeyId();
            Iterator<PropertyConstraint> constraintIterator =
                    uniquePropertyConstraints( schemaReadOperations.constraintsGetForLabelAndPropertyKey( state, labelId, propertyKeyId ) );
            if ( constraintIterator.hasNext() )
            {
                validateNoExistingNodeWithLabelAndProperty( state, labelId, property, nodeId );
            }
        }
        return entityWriteOperations.nodeSetProperty( state, nodeId, property );
    }

    private void validateNoExistingNodeWithLabelAndProperty( KernelStatement state, int labelId,
            DefinedProperty property, long modifiedNode )
            throws ConstraintValidationKernelException
    {
        try
        {
            Object value = property.value();
            int propertyKeyId = property.propertyKeyId();
            IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
            assertIndexOnline( state, indexDescriptor );
            state.locks().acquireExclusive( INDEX_ENTRY,
                    indexEntryResourceId( labelId, propertyKeyId, property.valueAsString() ) );

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

    private Iterator<PropertyConstraint> uniquePropertyConstraints( Iterator<PropertyConstraint> propertyConstraintIterator)
    {
        return new FilteringIterator<>( propertyConstraintIterator, new Predicate<PropertyConstraint>()
        {
            @Override
            public boolean test( PropertyConstraint constraint )
            {
                return constraint instanceof UniquenessConstraint;
            }
        } );
    }

    // Simply delegate the rest of the invocations

    @Override
    public void nodeDelete( KernelStatement state, long nodeId ) throws EntityNotFoundException
    {
        entityWriteOperations.nodeDelete( state, nodeId );
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
    public void relationshipDelete( KernelStatement state, long relationshipId ) throws EntityNotFoundException
    {
        entityWriteOperations.relationshipDelete( state, relationshipId );
    }

    @Override
    public boolean nodeRemoveLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException
    {
        return entityWriteOperations.nodeRemoveLabel( state, nodeId, labelId );
    }

    @Override
    public Property relationshipSetProperty( KernelStatement state, long relationshipId, DefinedProperty property )
            throws EntityNotFoundException
    {
        return entityWriteOperations.relationshipSetProperty( state, relationshipId, property );
    }

    @Override
    public Property graphSetProperty( KernelStatement state, DefinedProperty property )
    {
        return entityWriteOperations.graphSetProperty( state, property );
    }

    @Override
    public Property nodeRemoveProperty( KernelStatement state, long nodeId, int propertyKeyId )
            throws EntityNotFoundException
    {
        return entityWriteOperations.nodeRemoveProperty( state, nodeId, propertyKeyId );
    }

    @Override
    public Property relationshipRemoveProperty( KernelStatement state, long relationshipId, int propertyKeyId )
            throws EntityNotFoundException
    {
        return entityWriteOperations.relationshipRemoveProperty( state, relationshipId, propertyKeyId );
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
    public PrimitiveLongIterator nodesGetFromIndexSeekByPrefix( KernelStatement state, IndexDescriptor index,
            String prefix ) throws IndexNotFoundKernelException
    {
        return entityReadOperations.nodesGetFromIndexSeekByPrefix( state, index, prefix );
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
    public boolean nodeHasLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException
    {
        return entityReadOperations.nodeHasLabel( state, nodeId, labelId );
    }

    @Override
    public PrimitiveIntIterator nodeGetLabels( KernelStatement state, long nodeId ) throws EntityNotFoundException
    {
        return entityReadOperations.nodeGetLabels( state, nodeId );
    }

    @Override
    public PrimitiveIntIterator nodeGetLabels( TxStateHolder txStateHolder,
            StoreStatement storeStatement,
            long nodeId ) throws EntityNotFoundException
    {
        return entityReadOperations.nodeGetLabels( txStateHolder, storeStatement, nodeId );
    }

    @Override
    public boolean nodeHasProperty( KernelStatement statement,
            long nodeId,
            int propertyKeyId ) throws EntityNotFoundException
    {
        return entityReadOperations.nodeHasProperty( statement, nodeId, propertyKeyId );
    }

    @Override
    public boolean nodeHasProperty( TxStateHolder txStateHolder,
            StoreStatement storeStatement,
            long nodeId,
            int propertyKeyId ) throws EntityNotFoundException
    {
        return entityReadOperations.nodeHasProperty( txStateHolder, storeStatement, nodeId, propertyKeyId );
    }

    @Override
    public Object nodeGetProperty( KernelStatement state,
            long nodeId,
            int propertyKeyId ) throws EntityNotFoundException
    {
        return entityReadOperations.nodeGetProperty( state, nodeId, propertyKeyId );
    }

    @Override
    public boolean relationshipHasProperty( KernelStatement state,
            long relationshipId,
            int propertyKeyId ) throws EntityNotFoundException
    {
        return entityReadOperations.relationshipHasProperty( state, relationshipId, propertyKeyId );
    }

    @Override
    public Object relationshipGetProperty( KernelStatement state, long relationshipId, int propertyKeyId ) throws
            EntityNotFoundException
    {
        return entityReadOperations.relationshipGetProperty( state, relationshipId, propertyKeyId );
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
    public PrimitiveIntIterator nodeGetPropertyKeys( KernelStatement state, long nodeId ) throws EntityNotFoundException
    {
        return entityReadOperations.nodeGetPropertyKeys( state, nodeId );
    }

    @Override
    public PrimitiveIntIterator relationshipGetPropertyKeys( KernelStatement state, long relationshipId ) throws
            EntityNotFoundException
    {
        return entityReadOperations.relationshipGetPropertyKeys( state, relationshipId );
    }

    @Override
    public PrimitiveIntIterator graphGetPropertyKeys( KernelStatement state )
    {
        return entityReadOperations.graphGetPropertyKeys( state );
    }

    @Override
    public RelationshipIterator nodeGetRelationships( KernelStatement statement, long nodeId, Direction direction,
            int[] relTypes ) throws EntityNotFoundException
    {
        return entityReadOperations.nodeGetRelationships( statement, nodeId, direction, relTypes );
    }

    @Override
    public RelationshipIterator nodeGetRelationships( KernelStatement statement,
            long nodeId,
            Direction direction ) throws EntityNotFoundException
    {
        return entityReadOperations.nodeGetRelationships( statement, nodeId, direction );
    }

    @Override
    public int nodeGetDegree( KernelStatement statement, long nodeId, Direction direction, int relType )
            throws EntityNotFoundException
    {
        return entityReadOperations.nodeGetDegree( statement, nodeId, direction, relType );
    }

    @Override
    public int nodeGetDegree( KernelStatement statement,
            long nodeId,
            Direction direction ) throws EntityNotFoundException
    {
        return entityReadOperations.nodeGetDegree( statement, nodeId, direction );
    }

    @Override
    public PrimitiveIntIterator nodeGetRelationshipTypes( KernelStatement statement, long nodeId )
            throws EntityNotFoundException
    {
        return entityReadOperations.nodeGetRelationshipTypes( statement, nodeId );
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
    public NodeCursor nodeCursor( KernelStatement statement, long nodeId )
    {
        return entityReadOperations.nodeCursor( statement, nodeId );
    }

    @Override
    public RelationshipCursor relationshipCursor( KernelStatement statement, long relId )
    {
        return entityReadOperations.relationshipCursor( statement, relId );
    }

    @Override
    public NodeCursor nodeCursorGetAll( KernelStatement statement )
    {
        return entityReadOperations.nodeCursorGetAll( statement );
    }

    @Override
    public RelationshipCursor relationshipCursorGetAll( KernelStatement statement )
    {
        return entityReadOperations.relationshipCursorGetAll( statement );
    }

    @Override
    public NodeCursor nodeCursorGetForLabel( KernelStatement statement, int labelId )
    {
        return entityReadOperations.nodeCursorGetForLabel( statement, labelId );
    }

    @Override
    public NodeCursor nodeCursorGetFromIndexSeek( KernelStatement statement, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        return entityReadOperations.nodeCursorGetFromIndexSeek( statement, index, value );
    }

    @Override
    public NodeCursor nodeCursorGetFromIndexScan( KernelStatement statement, IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        return entityReadOperations.nodeCursorGetFromIndexScan( statement, index );
    }

    @Override
    public NodeCursor nodeCursorGetFromIndexSeekByPrefix( KernelStatement statement,
            IndexDescriptor index,
            String prefix ) throws IndexNotFoundKernelException
    {
        return entityReadOperations.nodeCursorGetFromIndexSeekByPrefix( statement, index, prefix );
    }

    @Override
    public NodeCursor nodeCursorGetFromUniqueIndexSeek( KernelStatement statement,
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
    public MandatoryPropertyConstraint mandatoryPropertyConstraintCreate( KernelStatement state, int labelId,
            int propertyKeyId ) throws AlreadyConstrainedException, CreateConstraintFailureException
    {
        PrimitiveLongIterator nodes = nodesGetForLabel( state, labelId );
        while ( nodes.hasNext() )
        {
            try
            {
                long nodeId = nodes.next();
                if ( !nodeHasProperty( state, nodeId, propertyKeyId ) )
                {
                    PropertyConstraint constraint = new MandatoryPropertyConstraint( labelId, propertyKeyId );

                    ConstraintVerificationFailedKernelException cause = new ConstraintVerificationFailedKernelException(
                            constraint, Evidence.ofNodeWithNullProperty( nodeId ) );

                    throw new CreateConstraintFailureException( constraint, cause );
                }
            }
            catch ( EntityNotFoundException e )
            {
                PropertyConstraint constraint = new MandatoryPropertyConstraint( labelId, propertyKeyId );
                throw new CreateConstraintFailureException( constraint, e );
            }
        }

        return schemaWriteOperations.mandatoryPropertyConstraintCreate( state, labelId, propertyKeyId );
    }

    @Override
    public void constraintDrop( KernelStatement state, PropertyConstraint constraint )
            throws DropConstraintFailureException
    {
        schemaWriteOperations.constraintDrop( state, constraint );
    }
}
