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

import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.Strings;
import org.neo4j.helpers.collection.CastingIterator;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.UnableToValidateConstraintKernelException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyConstraintViolationKernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.schema.IndexDescriptor;
import org.neo4j.kernel.api.schema.NodePropertyDescriptor;
import org.neo4j.kernel.api.schema.RelationshipPropertyDescriptor;
import org.neo4j.kernel.api.schema_new.index.IndexBoundary;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.impl.api.operations.EntityOperations;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.impl.api.store.EntityLoadingIterator;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.impl.locking.ResourceTypes.INDEX_ENTRY;
import static org.neo4j.kernel.impl.locking.ResourceTypes.indexEntryResourceId;

public class ConstraintEnforcingEntityOperations implements EntityOperations, SchemaWriteOperations
{
    private final EntityWriteOperations entityWriteOperations;
    private final EntityReadOperations entityReadOperations;
    private final SchemaWriteOperations schemaWriteOperations;
    private final SchemaReadOperations schemaReadOperations;
    private final ConstraintSemantics constraintSemantics;

    public ConstraintEnforcingEntityOperations(
            ConstraintSemantics constraintSemantics, EntityWriteOperations entityWriteOperations,
            EntityReadOperations entityReadOperations,
            SchemaWriteOperations schemaWriteOperations,
            SchemaReadOperations schemaReadOperations )
    {
        this.constraintSemantics = constraintSemantics;
        this.entityWriteOperations = entityWriteOperations;
        this.entityReadOperations = entityReadOperations;
        this.schemaWriteOperations = schemaWriteOperations;
        this.schemaReadOperations = schemaReadOperations;
    }

    @Override
    public boolean nodeAddLabel( KernelStatement state, long nodeId, int labelId )
            throws ConstraintValidationKernelException, EntityNotFoundException
    {
        try ( Cursor<NodeItem> cursor = nodeCursorById( state, nodeId ) )
        {
            NodeItem node = cursor.get();
            Iterator<NodePropertyConstraint> allConstraints = schemaReadOperations.constraintsGetForLabel( state, labelId );
            Iterator<UniquenessConstraint> constraints = uniquePropertyConstraints( allConstraints );
            while ( constraints.hasNext() )
            {
                UniquenessConstraint constraint = constraints.next();
                // TODO: Support composite indexes
                Object propertyValue = node.getProperty( constraint.descriptor().getPropertyKeyId() );
                if ( propertyValue != null )
                {
                    // TODO: Support composite indexes
                    validateNoExistingNodeWithLabelAndProperty( state, constraint, propertyValue, node.id() );
                }
            }

        }
        return entityWriteOperations.nodeAddLabel( state, nodeId, labelId );
    }

    @Override
    public Property nodeSetProperty( KernelStatement state, long nodeId, DefinedProperty property )
            throws ConstraintValidationKernelException, EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        try ( Cursor<NodeItem> cursor = nodeCursorById( state, nodeId ) )
        {
            NodeItem node = cursor.get();
            node.labels().visitKeys( labelId ->
            {
                int propertyKeyId = property.propertyKeyId();
                Iterator<UniquenessConstraint> constraintIterator = uniquePropertyConstraints( schemaReadOperations
                        .constraintsGetForLabelAndPropertyKey( state,
                                new NodePropertyDescriptor( labelId, propertyKeyId ) ) );
                if ( constraintIterator.hasNext() )
                {
                    UniquenessConstraint constraint = constraintIterator.next();
                    validateNoExistingNodeWithLabelAndProperty( state, constraint, property.value(), node.id() );
                }
                return false;
            } );
        }

        return entityWriteOperations.nodeSetProperty( state, nodeId, property );
    }

    private void validateNoExistingNodeWithLabelAndProperty( KernelStatement state,  UniquenessConstraint constraint,
            Object value, long modifiedNode ) throws ConstraintValidationKernelException
    {
        try
        {
            // TODO: Support composite constraints
            IndexDescriptor index = constraint.indexDescriptor();
            assertIndexOnline( state, IndexBoundary.map( index ) );
            state.locks().optimistic().acquireExclusive( state.lockTracer(), INDEX_ENTRY,
                    indexEntryResourceId( index.getLabelId(), index.getPropertyKeyId(), Strings.prettyPrint( value
                    ) ) );

            long existing = entityReadOperations.nodeGetFromUniqueIndexSeek( state, index, value );
            if ( existing != NO_SUCH_NODE && existing != modifiedNode )
            {
                throw new UniquePropertyConstraintViolationKernelException( index.getLabelId(),
                        index.getPropertyKeyId(),
                        value, existing );
            }
        }
        catch ( IndexNotFoundKernelException | IndexBrokenKernelException e )
        {
            throw new UnableToValidateConstraintKernelException( e );
        }
    }

    private void assertIndexOnline( KernelStatement state, NewIndexDescriptor indexDescriptor )
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

    private Iterator<UniquenessConstraint> uniquePropertyConstraints( Iterator<NodePropertyConstraint> constraints )
    {
        return new CastingIterator<>( constraints, UniquenessConstraint.class );
    }

    // Simply delegate the rest of the invocations

    @Override
    public void nodeDelete( KernelStatement state, long nodeId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        entityWriteOperations.nodeDelete( state, nodeId );
    }

    @Override
    public int nodeDetachDelete( KernelStatement state, long nodeId ) throws EntityNotFoundException,
            AutoIndexingKernelException, InvalidTransactionTypeKernelException, KernelException
    {
        return entityWriteOperations.nodeDetachDelete( state, nodeId );
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
    public void relationshipDelete( KernelStatement state, long relationshipId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        entityWriteOperations.relationshipDelete( state, relationshipId );
    }

    @Override
    public boolean nodeRemoveLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException
    {
        return entityWriteOperations.nodeRemoveLabel( state, nodeId, labelId );
    }

    @Override
    public Property relationshipSetProperty( KernelStatement state,
            long relationshipId,
            DefinedProperty property ) throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
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
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        return entityWriteOperations.nodeRemoveProperty( state, nodeId, propertyKeyId );
    }

    @Override
    public Property relationshipRemoveProperty( KernelStatement state,
            long relationshipId,
            int propertyKeyId ) throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
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
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByNumber( KernelStatement statement,
            IndexDescriptor index,
            Number lower, boolean includeLower,
            Number upper, boolean includeUpper )
            throws IndexNotFoundKernelException
    {
        return entityReadOperations.nodesGetFromIndexRangeSeekByNumber( statement, index, lower, includeLower, upper,
                includeUpper );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByString( KernelStatement statement,
            IndexDescriptor index,
            String lower, boolean includeLower,
            String upper, boolean includeUpper )
            throws IndexNotFoundKernelException
    {
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
        assertIndexOnline( state, IndexBoundary.map( index ) );

        // TODO: Support composite index, either by allowing value to be an array, or by creating a new method
        int labelId = index.getLabelId();
        int propertyKeyId = index.getPropertyKeyId();
        String stringVal = "";
        if ( null != value )
        {
            DefinedProperty property = Property.property( propertyKeyId, value );
            stringVal = property.valueAsString();
        }

        // If we find the node - hold a shared lock. If we don't find a node - hold an exclusive lock.
        // If locks are deferred than both shared and exclusive locks will be taken only at commit time.
        Locks.Client locks = state.locks().optimistic();
        LockTracer lockTracer = state.lockTracer();
        long indexEntryId = indexEntryResourceId( labelId, propertyKeyId, stringVal );

        locks.acquireShared( lockTracer, INDEX_ENTRY, indexEntryId );

        long nodeId = entityReadOperations.nodeGetFromUniqueIndexSeek( state, index, value );
        if ( NO_SUCH_NODE == nodeId )
        {
            locks.releaseShared( INDEX_ENTRY, indexEntryId );
            locks.acquireExclusive( lockTracer, INDEX_ENTRY, indexEntryId );

            nodeId = entityReadOperations.nodeGetFromUniqueIndexSeek( state, index, value );
            if ( NO_SUCH_NODE != nodeId ) // we found it under the exclusive lock
            {
                // downgrade to a shared lock
                locks.acquireShared( lockTracer, INDEX_ENTRY, indexEntryId );
                locks.releaseExclusive( INDEX_ENTRY, indexEntryId );
            }
        }
        return nodeId;
    }

    @Override
    public long nodesCountIndexed( KernelStatement statement, IndexDescriptor index, long nodeId, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        return entityReadOperations.nodesCountIndexed( statement, index, nodeId, value );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexContainsScan( KernelStatement state, IndexDescriptor index,
            String term ) throws IndexNotFoundKernelException
    {
        return entityReadOperations.nodesGetFromIndexContainsScan( state, index, term );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexEndsWithScan( KernelStatement state, IndexDescriptor index,
            String suffix ) throws IndexNotFoundKernelException
    {
        return entityReadOperations.nodesGetFromIndexEndsWithScan( state, index, suffix );
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
    public PrimitiveIntIterator graphGetPropertyKeys( KernelStatement state )
    {
        return entityReadOperations.graphGetPropertyKeys( state );
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
    public Cursor<NodeItem> nodeCursorById( KernelStatement statement, long nodeId ) throws EntityNotFoundException
    {
        return entityReadOperations.nodeCursorById( statement, nodeId );
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursorById( KernelStatement statement, long relId ) throws EntityNotFoundException
    {
        return entityReadOperations.relationshipCursorById( statement, relId );
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursorGetAll( KernelStatement statement )
    {
        return entityReadOperations.relationshipCursorGetAll( statement );
    }

    @Override
    public NewIndexDescriptor indexCreate( KernelStatement state, NodePropertyDescriptor descriptor )
            throws AlreadyIndexedException, AlreadyConstrainedException
    {
        return schemaWriteOperations.indexCreate( state, descriptor );
    }

    @Override
    public void indexDrop( KernelStatement state, NewIndexDescriptor descriptor ) throws DropIndexFailureException
    {
        schemaWriteOperations.indexDrop( state, descriptor );
    }

    @Override
    public void uniqueIndexDrop( KernelStatement state, NewIndexDescriptor descriptor ) throws DropIndexFailureException
    {
        schemaWriteOperations.uniqueIndexDrop( state, descriptor );
    }

    @Override
    public UniquenessConstraint uniquePropertyConstraintCreate( KernelStatement state, NodePropertyDescriptor descriptor )
            throws AlreadyConstrainedException, CreateConstraintFailureException, AlreadyIndexedException
    {
        return schemaWriteOperations.uniquePropertyConstraintCreate( state, descriptor );
    }

    @Override
    public NodePropertyExistenceConstraint nodePropertyExistenceConstraintCreate( KernelStatement state, NodePropertyDescriptor descriptor ) throws AlreadyConstrainedException, CreateConstraintFailureException
    {
        Iterator<Cursor<NodeItem>> nodes = new EntityLoadingIterator<>( nodesGetForLabel( state, descriptor.getLabelId() ),
                ( id ) -> nodeCursorById( state, id ) );
        constraintSemantics.validateNodePropertyExistenceConstraint( nodes, descriptor );
        return schemaWriteOperations.nodePropertyExistenceConstraintCreate( state, descriptor );
    }

    @Override
    public RelationshipPropertyExistenceConstraint relationshipPropertyExistenceConstraintCreate( KernelStatement state,
            RelationshipPropertyDescriptor descriptor ) throws AlreadyConstrainedException, CreateConstraintFailureException
    {
        try ( Cursor<RelationshipItem> cursor = relationshipCursorGetAll( state ) )
        {
            constraintSemantics.validateRelationshipPropertyExistenceConstraint( cursor, descriptor );
        }
        return schemaWriteOperations.relationshipPropertyExistenceConstraintCreate( state, descriptor );
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

    @Override
    public long nodesGetCount( KernelStatement statement )
    {
        return entityReadOperations.nodesGetCount( statement );
    }

    @Override
    public long relationshipsGetCount( KernelStatement statement )
    {
        return entityReadOperations.relationshipsGetCount( statement );
    }

    @Override
    public boolean nodeExists( KernelStatement statement, long id )
    {
        return entityReadOperations.nodeExists( statement, id );
    }
}
