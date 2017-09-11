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

import org.apache.commons.lang3.ArrayUtils;

import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveIntCollection;
import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.collection.CastingIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedPropertyInCompositeSchemaException;
import org.neo4j.kernel.api.exceptions.schema.UnableToValidateConstraintException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.IndexQuery.ExactPredicate;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.NodeExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.operations.EntityOperations;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.impl.api.schema.NodeSchemaMatcher;
import org.neo4j.kernel.impl.api.store.NodeLoadingIterator;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;
import static org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException.Phase.VALIDATION;
import static org.neo4j.kernel.api.schema.SchemaDescriptorPredicates.hasProperty;
import static org.neo4j.kernel.impl.locking.ResourceTypes.INDEX_ENTRY;
import static org.neo4j.kernel.impl.locking.ResourceTypes.indexEntryResourceId;

public class ConstraintEnforcingEntityOperations implements EntityOperations, SchemaWriteOperations
{
    private final EntityWriteOperations entityWriteOperations;
    private final EntityReadOperations entityReadOperations;
    private final SchemaWriteOperations schemaWriteOperations;
    private final SchemaReadOperations schemaReadOperations;
    private final ConstraintSemantics constraintSemantics;
    private final NodeSchemaMatcher nodeSchemaMatcher;

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
        nodeSchemaMatcher = new NodeSchemaMatcher( entityReadOperations );
    }

    @Override
    public boolean nodeAddLabel( KernelStatement state, long nodeId, int labelId )
            throws EntityNotFoundException, ConstraintValidationException
    {
        try ( Cursor<NodeItem> cursor = nodeCursorById( state, nodeId ) )
        {
            NodeItem node = cursor.get();
            if ( !node.hasLabel( labelId ) )
            {
                // no need to verify if the node already had the label
                Iterator<ConstraintDescriptor> constraints = schemaReadOperations.constraintsGetForLabel( state, labelId );
                while ( constraints.hasNext() )
                {
                    ConstraintDescriptor constraint = constraints.next();
                    if ( constraint.enforcesUniqueness() )
                    {
                        IndexBackedConstraintDescriptor uniqueConstraint = (IndexBackedConstraintDescriptor) constraint;
                        ExactPredicate[] propertyValues = getAllPropertyValues( state, uniqueConstraint.schema(), node );
                        if ( propertyValues != null )
                        {
                            validateNoExistingNodeWithExactValues( state, uniqueConstraint, propertyValues, node.id() );
                        }
                    }
                }
            }
        }
        return entityWriteOperations.nodeAddLabel( state, nodeId, labelId );
    }

    @Override
    public Value nodeSetProperty( KernelStatement state, long nodeId, int propertyKeyId, Value value )
            throws EntityNotFoundException, AutoIndexingKernelException,
                   InvalidTransactionTypeKernelException, ConstraintValidationException
    {
        try ( Cursor<NodeItem> cursor = nodeCursorById( state, nodeId ) )
        {
            NodeItem node = cursor.get();
            state.locks().optimistic().acquireShared( state.lockTracer(), ResourceTypes.LABEL,
                    PrimitiveIntCollections.asLongArray( node.labels() ) );
            Iterator<ConstraintDescriptor> constraints =
                    getConstraintsInvolvingProperty( state, propertyKeyId );
            Iterator<IndexBackedConstraintDescriptor> uniquenessConstraints =
                    new CastingIterator<>( constraints, IndexBackedConstraintDescriptor.class );

            nodeSchemaMatcher.onMatchingSchema( state, uniquenessConstraints, node, propertyKeyId,
                    ( constraint, propertyIds ) ->
                    {
                        if ( propertyIds.contains( propertyKeyId ) )
                        {
                            Value previousValue = nodeGetProperty( state, node, propertyKeyId );
                            if ( value.equals( previousValue ) )
                            {
                                // since we are changing to the same value, there is no need to check
                                return;
                            }
                        }
                        validateNoExistingNodeWithExactValues( state, constraint,
                                getAllPropertyValues( state, constraint.schema(), node, propertyKeyId, value ),
                                node.id() );
                    } );
        }

        return entityWriteOperations.nodeSetProperty( state, nodeId, propertyKeyId, value );
    }

    private Iterator<ConstraintDescriptor> getConstraintsInvolvingProperty( KernelStatement state, int propertyId )
    {
        Iterator<ConstraintDescriptor> allConstraints = schemaReadOperations.constraintsGetAll( state );

        return Iterators.filter( hasProperty( propertyId ), allConstraints );
    }

    private ExactPredicate[] getAllPropertyValues( KernelStatement state, SchemaDescriptor schema, NodeItem node )
    {
        return getAllPropertyValues( state, schema, node, StatementConstants.NO_SUCH_PROPERTY_KEY, Values.NO_VALUE );
    }

    /**
     * Fetch the property values for all properties in schema for a given node. Return these as an exact predicate
     * array.
     *
     * The changedProperty is used to override the store/txState value of the property. This is used when we intend
     * to change a property, and that to verify that the post-change values do not validate some constraint.
     */
    private ExactPredicate[] getAllPropertyValues( KernelStatement state, SchemaDescriptor schema, NodeItem node,
            int changedPropertyKeyId, Value changedValue )
    {
        int[] schemaPropertyIds = schema.getPropertyIds();
        ExactPredicate[] values = new ExactPredicate[schemaPropertyIds.length];

        int nMatched = 0;
        Cursor<PropertyItem> nodePropertyCursor = nodeGetProperties( state, node );
        while ( nodePropertyCursor.next() )
        {
            PropertyItem property = nodePropertyCursor.get();

            int nodePropertyId = property.propertyKeyId();
            int k = ArrayUtils.indexOf( schemaPropertyIds, nodePropertyId );
            if ( k >= 0 )
            {
                if ( nodePropertyId != changedPropertyKeyId )
                {
                    values[k] = IndexQuery.exact( nodePropertyId, property.value() );
                }
                nMatched++;
            }
        }

        if ( changedPropertyKeyId != NO_SUCH_PROPERTY_KEY )
        {
            int k = ArrayUtils.indexOf( schemaPropertyIds, changedPropertyKeyId );
            if ( k >= 0 )
            {
                values[k] = IndexQuery.exact( changedPropertyKeyId, changedValue );
                nMatched++;
            }
        }
        if ( nMatched < values.length )
        {
            return null;
        }
        return values;
    }

    private void validateNoExistingNodeWithExactValues(
            KernelStatement state,
            IndexBackedConstraintDescriptor constraint,
            ExactPredicate[] propertyValues,
            long modifiedNode
    ) throws ConstraintValidationException
    {
        try
        {
            IndexDescriptor index = constraint.ownedIndexDescriptor();
            assertIndexOnline( state, index );

            int labelId = index.schema().getLabelId();
            state.locks().optimistic().acquireExclusive(
                    state.lockTracer(),
                    INDEX_ENTRY,
                    indexEntryResourceId( labelId, propertyValues )
                );

            long existing = entityReadOperations.nodeGetFromUniqueIndexSeek( state, index, propertyValues );
            if ( existing != NO_SUCH_NODE && existing != modifiedNode )
            {
                throw new UniquePropertyValueValidationException( constraint, VALIDATION,
                        new IndexEntryConflictException( existing, NO_SUCH_NODE, IndexQuery.asValueTuple( propertyValues ) ) );
            }
        }
        catch ( IndexNotFoundKernelException | IndexBrokenKernelException | IndexNotApplicableKernelException e )
        {
            throw new UnableToValidateConstraintException( constraint, e );
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

    // Simply delegate the rest of the invocations

    @Override
    public void nodeDelete( KernelStatement state, long nodeId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        entityWriteOperations.nodeDelete( state, nodeId );
    }

    @Override
    public int nodeDetachDelete( KernelStatement state, long nodeId ) throws KernelException
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
    public Value relationshipSetProperty( KernelStatement state, long relationshipId, int propertyKeyId, Value value )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        return entityWriteOperations.relationshipSetProperty( state, relationshipId, propertyKeyId, value );
    }

    @Override
    public Value graphSetProperty( KernelStatement state, int propertyKeyId, Value value )
    {
        return entityWriteOperations.graphSetProperty( state, propertyKeyId, value );
    }

    @Override
    public Value nodeRemoveProperty( KernelStatement state, long nodeId, int propertyKeyId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        return entityWriteOperations.nodeRemoveProperty( state, nodeId, propertyKeyId );
    }

    @Override
    public Value relationshipRemoveProperty( KernelStatement state,
            long relationshipId,
            int propertyKeyId ) throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        return entityWriteOperations.relationshipRemoveProperty( state, relationshipId, propertyKeyId );
    }

    @Override
    public Value graphRemoveProperty( KernelStatement state, int propertyKeyId )
    {
        return entityWriteOperations.graphRemoveProperty( state, propertyKeyId );
    }

    @Override
    public PrimitiveLongIterator nodesGetForLabel( KernelStatement state, int labelId )
    {
        return entityReadOperations.nodesGetForLabel( state, labelId );
    }

    @Override
    public PrimitiveLongIterator indexQuery( KernelStatement statement, IndexDescriptor index,
            IndexQuery[] predicates )
            throws IndexNotFoundKernelException, IndexNotApplicableKernelException
    {
        return entityReadOperations.indexQuery( statement, index, predicates );
    }

    @Override
    public long nodeGetFromUniqueIndexSeek(
            KernelStatement state,
            IndexDescriptor index,
            ExactPredicate... predicates )
            throws IndexNotFoundKernelException, IndexBrokenKernelException, IndexNotApplicableKernelException
    {
        assertIndexOnline( state, index );
        assertPredicatesMatchSchema( index.schema(), predicates );
        int labelId = index.schema().getLabelId();

        // If we find the node - hold a shared lock. If we don't find a node - hold an exclusive lock.
        // If locks are deferred than both shared and exclusive locks will be taken only at commit time.
        Locks.Client locks = state.locks().optimistic();
        LockTracer lockTracer = state.lockTracer();
        long indexEntryId = indexEntryResourceId( labelId, predicates );

        locks.acquireShared( lockTracer, INDEX_ENTRY, indexEntryId );

        long nodeId = entityReadOperations.nodeGetFromUniqueIndexSeek( state, index, predicates );
        if ( NO_SUCH_NODE == nodeId )
        {
            locks.releaseShared( INDEX_ENTRY, indexEntryId );
            locks.acquireExclusive( lockTracer, INDEX_ENTRY, indexEntryId );

            nodeId = entityReadOperations.nodeGetFromUniqueIndexSeek( state, index, predicates );
            if ( NO_SUCH_NODE != nodeId ) // we found it under the exclusive lock
            {
                // downgrade to a shared lock
                locks.acquireShared( lockTracer, INDEX_ENTRY, indexEntryId );
                locks.releaseExclusive( INDEX_ENTRY, indexEntryId );
            }
        }
        return nodeId;
    }

    private void assertPredicatesMatchSchema( LabelSchemaDescriptor schema, ExactPredicate[] predicates )
            throws IndexNotApplicableKernelException
    {
        int[] propertyIds = schema.getPropertyIds();
        if ( propertyIds.length != predicates.length )
        {
            throw new IndexNotApplicableKernelException(
                    format( "The index specifies %d properties, but only %d lookup predicates were given.",
                            propertyIds.length, predicates.length ) );
        }
        for ( int i = 0; i < predicates.length; i++ )
        {
            if ( predicates[i].propertyKeyId() != propertyIds[i] )
            {
                throw new IndexNotApplicableKernelException(
                        format( "The index has the property id %d in position %d, but the lookup property id was %d.",
                                propertyIds[i], i, predicates[i].propertyKeyId() ) );
            }
        }
    }

    @Override
    public long nodesCountIndexed( KernelStatement statement, IndexDescriptor index, long nodeId, Value value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        return entityReadOperations.nodesCountIndexed( statement, index, nodeId, value );
    }

    @Override
    public boolean graphHasProperty( KernelStatement state, int propertyKeyId )
    {
        return entityReadOperations.graphHasProperty( state, propertyKeyId );
    }

    @Override
    public Value graphGetProperty( KernelStatement state, int propertyKeyId )
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
    public Cursor<PropertyItem> nodeGetProperties( KernelStatement statement, NodeItem node )
    {
        return entityReadOperations.nodeGetProperties( statement, node );
    }

    @Override
    public Value nodeGetProperty( KernelStatement statement, NodeItem node, int propertyKeyId )
    {
        return entityReadOperations.nodeGetProperty( statement, node, propertyKeyId );
    }

    @Override
    public boolean nodeHasProperty( KernelStatement statement, NodeItem node, int propertyKeyId )
    {
        return entityReadOperations.nodeHasProperty( statement, node, propertyKeyId );
    }

    @Override
    public PrimitiveIntCollection nodeGetPropertyKeys( KernelStatement statement, NodeItem node )
    {
        return entityReadOperations.nodeGetPropertyKeys( statement, node );
    }

    @Override
    public Cursor<PropertyItem> relationshipGetProperties( KernelStatement statement, RelationshipItem relationship )
    {
        return entityReadOperations.relationshipGetProperties( statement, relationship );
    }

    @Override
    public Value relationshipGetProperty( KernelStatement statement, RelationshipItem relationship, int propertyKeyId )
    {
        return entityReadOperations.relationshipGetProperty( statement, relationship, propertyKeyId );
    }

    @Override
    public boolean relationshipHasProperty( KernelStatement statement, RelationshipItem relationship,
            int propertyKeyId )
    {
        return entityReadOperations.relationshipHasProperty( statement, relationship, propertyKeyId );
    }

    @Override
    public PrimitiveIntCollection relationshipGetPropertyKeys( KernelStatement statement,
            RelationshipItem relationship )
    {
        return entityReadOperations.relationshipGetPropertyKeys( statement, relationship );
    }

    @Override
    public Cursor<RelationshipItem> nodeGetRelationships( KernelStatement statement, NodeItem node,
            Direction direction )
    {
        return entityReadOperations.nodeGetRelationships( statement, node, direction );
    }

    @Override
    public Cursor<RelationshipItem> nodeGetRelationships( KernelStatement statement, NodeItem node, Direction direction,
            int[] relTypes )
    {
        return entityReadOperations.nodeGetRelationships( statement, node, direction, relTypes );
    }

    @Override
    public IndexDescriptor indexCreate( KernelStatement state, LabelSchemaDescriptor descriptor )
            throws AlreadyIndexedException, AlreadyConstrainedException, RepeatedPropertyInCompositeSchemaException
    {
        return schemaWriteOperations.indexCreate( state, descriptor );
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
    public NodeKeyConstraintDescriptor nodeKeyConstraintCreate( KernelStatement state, LabelSchemaDescriptor descriptor )
            throws AlreadyConstrainedException, CreateConstraintFailureException, AlreadyIndexedException,
            RepeatedPropertyInCompositeSchemaException
    {
        Iterator<Cursor<NodeItem>> nodes = new NodeLoadingIterator( nodesGetForLabel( state, descriptor.getLabelId() ),
                id -> nodeCursorById( state, id ) );
        constraintSemantics.validateNodeKeyConstraint( nodes, descriptor,
                ( node, propertyKey ) -> entityReadOperations.nodeHasProperty( state, node, propertyKey ) );
        return schemaWriteOperations.nodeKeyConstraintCreate( state, descriptor );
    }

    @Override
    public UniquenessConstraintDescriptor uniquePropertyConstraintCreate( KernelStatement state, LabelSchemaDescriptor descriptor )
            throws AlreadyConstrainedException, CreateConstraintFailureException, AlreadyIndexedException,
            RepeatedPropertyInCompositeSchemaException
    {
        return schemaWriteOperations.uniquePropertyConstraintCreate( state, descriptor );
    }

    @Override
    public NodeExistenceConstraintDescriptor nodePropertyExistenceConstraintCreate(
                KernelStatement state,
                LabelSchemaDescriptor descriptor
    ) throws AlreadyConstrainedException, CreateConstraintFailureException, RepeatedPropertyInCompositeSchemaException
    {
        Iterator<Cursor<NodeItem>> nodes = new NodeLoadingIterator( nodesGetForLabel( state, descriptor.getLabelId() ),
                id -> nodeCursorById( state, id ) );
        constraintSemantics.validateNodePropertyExistenceConstraint( nodes, descriptor,
                ( node, propertyKey ) -> entityReadOperations.nodeHasProperty( state, node, propertyKey ) );
        return schemaWriteOperations.nodePropertyExistenceConstraintCreate( state, descriptor );
    }

    @Override
    public RelExistenceConstraintDescriptor relationshipPropertyExistenceConstraintCreate(
                KernelStatement state,
                RelationTypeSchemaDescriptor descriptor
    ) throws AlreadyConstrainedException, CreateConstraintFailureException, RepeatedPropertyInCompositeSchemaException
    {
        try ( Cursor<RelationshipItem> cursor = relationshipCursorGetAll( state ) )
        {
            constraintSemantics.validateRelationshipPropertyExistenceConstraint( cursor, descriptor,
                    ( relationship, propertyKey ) -> entityReadOperations
                            .relationshipHasProperty( state, relationship, propertyKey ) );
        }
        return schemaWriteOperations.relationshipPropertyExistenceConstraintCreate( state, descriptor );
    }

    @Override
    public void constraintDrop( KernelStatement state, ConstraintDescriptor constraint )
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

    @Override
    public PrimitiveIntSet relationshipTypes( KernelStatement statement, NodeItem nodeItem )
    {
        return entityReadOperations.relationshipTypes( statement, nodeItem );
    }

    @Override
    public int degree( KernelStatement statement, NodeItem nodeItem, Direction direction )
    {
        return entityReadOperations.degree( statement, nodeItem, direction );
    }

    @Override
    public int degree( KernelStatement statement, NodeItem nodeItem, Direction direction, int relType )
    {
        return entityReadOperations.degree( statement, nodeItem, direction, relType );
    }
}
