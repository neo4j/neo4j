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

import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Iterator;
import java.util.function.Function;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedPropertyInCompositeSchemaException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.NodeExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.kernel.impl.api.operations.LockOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaStateOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.schema.PopulationProgress;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class LockingStatementOperations implements
        EntityWriteOperations,
        SchemaReadOperations,
        SchemaWriteOperations,
        SchemaStateOperations,
        LockOperations
{
    private final EntityReadOperations entityReadDelegate;
    private final EntityWriteOperations entityWriteDelegate;
    private final SchemaReadOperations schemaReadDelegate;
    private final SchemaWriteOperations schemaWriteDelegate;
    private final SchemaStateOperations schemaStateDelegate;

    public LockingStatementOperations(
            EntityReadOperations entityReadDelegate,
            EntityWriteOperations entityWriteDelegate,
            SchemaReadOperations schemaReadDelegate,
            SchemaWriteOperations schemaWriteDelegate,
            SchemaStateOperations schemaStateDelegate )
    {
        this.entityReadDelegate = entityReadDelegate;
        this.entityWriteDelegate = entityWriteDelegate;
        this.schemaReadDelegate = schemaReadDelegate;
        this.schemaWriteDelegate = schemaWriteDelegate;
        this.schemaStateDelegate = schemaStateDelegate;
    }

    @Override
    public boolean nodeAddLabel( KernelStatement state, long nodeId, int labelId )
            throws ConstraintValidationException, EntityNotFoundException
    {
        // TODO (BBC, 22/11/13):
        // In order to enforce constraints we need to check whether this change violates constraints; we therefore need
        // the schema lock to ensure that our view of constraints is consistent.
        //
        // We would like this locking to be done naturally when ConstraintEnforcingEntityOperations calls
        // SchemaReadOperations#constraintsGetForLabel, but the SchemaReadOperations object that
        // ConstraintEnforcingEntityOperations has a reference to does not lock because of the way the cake is
        // constructed.
        //
        // It would be cleaner if the schema and data cakes were separated so that the SchemaReadOperations object used
        // by ConstraintEnforcingEntityOperations included the full cake, with locking included.
        sharedLabelLock( state, labelId );

        acquireExclusiveNodeLock( state, nodeId );
        state.assertOpen();

        return entityWriteDelegate.nodeAddLabel( state, nodeId, labelId );
    }

    @Override
    public boolean nodeRemoveLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException
    {
        acquireExclusiveNodeLock( state, nodeId );
        state.assertOpen();
        return entityWriteDelegate.nodeRemoveLabel( state, nodeId, labelId );
    }

    @Override
    public IndexDescriptor indexCreate( KernelStatement state, LabelSchemaDescriptor descriptor )
            throws AlreadyIndexedException, AlreadyConstrainedException, RepeatedPropertyInCompositeSchemaException
    {
        exclusiveLabelLock( state, descriptor.getLabelId() );
        state.assertOpen();
        return schemaWriteDelegate.indexCreate( state, descriptor );
    }

    @Override
    public void indexDrop( KernelStatement state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        exclusiveLabelLock( state, descriptor.schema().getLabelId() );
        state.assertOpen();
        schemaWriteDelegate.indexDrop( state, descriptor );
    }

    @Override
    public void uniqueIndexDrop( KernelStatement state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        exclusiveLabelLock( state, descriptor.schema().getLabelId() );
        state.assertOpen();
        schemaWriteDelegate.uniqueIndexDrop( state, descriptor );
    }

    @Override
    public <K, V> V schemaStateGetOrCreate( KernelStatement state, K key, Function<K,V> creator )
    {
        state.assertOpen();
        return schemaStateDelegate.schemaStateGetOrCreate( state, key, creator );
    }

    @Override
    public void schemaStateFlush( KernelStatement state )
    {
        state.assertOpen();
        schemaStateDelegate.schemaStateFlush( state );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( KernelStatement state, int labelId )
    {
        sharedLabelLock( state, labelId );
        state.assertOpen();
        return schemaReadDelegate.indexesGetForLabel( state, labelId );
    }

    @Override
    public IndexDescriptor indexGetForSchema( KernelStatement state, LabelSchemaDescriptor descriptor )
    {
        sharedLabelLock( state, descriptor.getLabelId() );
        state.assertOpen();
        return schemaReadDelegate.indexGetForSchema( state, descriptor );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll( KernelStatement state )
    {
        state.assertOpen();
        return Iterators.consumableIterator( schemaReadDelegate.indexesGetAll( state ),
                indexDescriptor -> sharedLabelLock( state, indexDescriptor.schema().getLabelId() ) );
    }

    @Override
    public InternalIndexState indexGetState( KernelStatement state, IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        sharedLabelLock( state, descriptor.schema().getLabelId() );
        state.assertOpen();
        return schemaReadDelegate.indexGetState( state, descriptor );
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( KernelStatement state, IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        sharedLabelLock( state, descriptor.schema().getLabelId() );
        state.assertOpen();
        return schemaReadDelegate.indexGetPopulationProgress( state, descriptor );
    }

    @Override
    public long indexSize( KernelStatement state, IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        sharedLabelLock( state, descriptor.schema().getLabelId() );
        state.assertOpen();
        return schemaReadDelegate.indexSize( state, descriptor );
    }

    @Override
    public double indexUniqueValuesPercentage( KernelStatement state,
            IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        sharedLabelLock( state, descriptor.schema().getLabelId() );
        state.assertOpen();
        return schemaReadDelegate.indexUniqueValuesPercentage( state, descriptor );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( KernelStatement state, IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        sharedLabelLock( state, index.schema().getLabelId() );
        state.assertOpen();
        return schemaReadDelegate.indexGetOwningUniquenessConstraintId( state, index );
    }

    @Override
    public long indexGetCommittedId( KernelStatement state, IndexDescriptor index )
            throws SchemaRuleNotFoundException
    {
        sharedLabelLock( state, index.schema().getLabelId() );
        state.assertOpen();
        return schemaReadDelegate.indexGetCommittedId( state, index );
    }

    @Override
    public void nodeDelete( KernelStatement state, long nodeId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        acquireExclusiveNodeLock( state, nodeId );
        state.assertOpen();
        entityWriteDelegate.nodeDelete( state, nodeId );
    }

    @Override
    public int nodeDetachDelete( final KernelStatement state, final long nodeId ) throws KernelException
    {
        final MutableInt count = new MutableInt();
        TwoPhaseNodeForRelationshipLocking locking = new TwoPhaseNodeForRelationshipLocking( entityReadDelegate,
                relId ->
                {
                    state.assertOpen();
                    try
                    {
                        entityWriteDelegate.relationshipDelete( state, relId );
                        count.increment();
                    }
                    catch ( EntityNotFoundException e )
                    {
                        // it doesn't matter...
                    }
                } );

        locking.lockAllNodesAndConsumeRelationships( nodeId, state );
        state.assertOpen();
        entityWriteDelegate.nodeDetachDelete( state, nodeId );
        return count.intValue();
    }

    @Override
    public long nodeCreate( KernelStatement statement )
    {
        return entityWriteDelegate.nodeCreate( statement );
    }

    @Override
    public long relationshipCreate( KernelStatement state, int relationshipTypeId, long startNodeId, long endNodeId )
            throws EntityNotFoundException
    {
        sharedRelationshipTypeLock( state, relationshipTypeId );
        lockRelationshipNodes( state, startNodeId, endNodeId );
        return entityWriteDelegate.relationshipCreate( state, relationshipTypeId, startNodeId, endNodeId );
    }

    @Override
    public void relationshipDelete( final KernelStatement state, long relationshipId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        entityReadDelegate.relationshipVisit( state, relationshipId,
                ( relId, type, startNode, endNode ) -> lockRelationshipNodes( state, startNode, endNode ) );
        acquireExclusiveRelationshipLock( state, relationshipId );
        state.assertOpen();
        entityWriteDelegate.relationshipDelete(state, relationshipId);
    }

    private void lockRelationshipNodes( KernelStatement state, long startNodeId, long endNodeId )
    {
        // Order the locks to lower the risk of deadlocks with other threads creating/deleting rels concurrently
        acquireExclusiveNodeLock( state, min( startNodeId, endNodeId ) );
        if ( startNodeId != endNodeId )
        {
            acquireExclusiveNodeLock( state, max( startNodeId, endNodeId ) );
        }
    }

    @Override
    public NodeKeyConstraintDescriptor nodeKeyConstraintCreate( KernelStatement state,LabelSchemaDescriptor descriptor )
            throws CreateConstraintFailureException, AlreadyConstrainedException, AlreadyIndexedException,
            RepeatedPropertyInCompositeSchemaException
    {
        exclusiveLabelLock( state, descriptor.getLabelId() );
        state.assertOpen();
        return schemaWriteDelegate.nodeKeyConstraintCreate( state, descriptor );
    }

    @Override
    public UniquenessConstraintDescriptor uniquePropertyConstraintCreate( KernelStatement state,LabelSchemaDescriptor descriptor )
            throws CreateConstraintFailureException, AlreadyConstrainedException, AlreadyIndexedException,
            RepeatedPropertyInCompositeSchemaException
    {
        exclusiveLabelLock( state, descriptor.getLabelId() );
        state.assertOpen();
        return schemaWriteDelegate.uniquePropertyConstraintCreate( state, descriptor );
    }

    @Override
    public NodeExistenceConstraintDescriptor nodePropertyExistenceConstraintCreate( KernelStatement state,
            LabelSchemaDescriptor descriptor ) throws AlreadyConstrainedException, CreateConstraintFailureException,
            RepeatedPropertyInCompositeSchemaException
    {
        exclusiveLabelLock( state, descriptor.getLabelId() );
        state.assertOpen();
        return schemaWriteDelegate.nodePropertyExistenceConstraintCreate( state, descriptor );
    }

    @Override
    public RelExistenceConstraintDescriptor relationshipPropertyExistenceConstraintCreate( KernelStatement state,
            RelationTypeSchemaDescriptor descriptor )
            throws AlreadyConstrainedException, CreateConstraintFailureException,
            RepeatedPropertyInCompositeSchemaException
    {
        exclusiveRelationshipTypeLock( state, descriptor.getRelTypeId() );
        state.assertOpen();
        return schemaWriteDelegate.relationshipPropertyExistenceConstraintCreate( state, descriptor );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForSchema( KernelStatement state, SchemaDescriptor descriptor )
    {

        sharedLock( state, descriptor.resourceType(), descriptor.resourceId() );
        state.assertOpen();
        return schemaReadDelegate.constraintsGetForSchema( state, descriptor );
    }

    @Override
    public boolean constraintExists( KernelStatement state, ConstraintDescriptor descriptor )
    {
        SchemaDescriptor schema = descriptor.schema();
        sharedLock( state, schema.resourceType(), schema.resourceId() );
        state.assertOpen();
        return schemaReadDelegate.constraintExists( state, descriptor );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel( KernelStatement state, int labelId )
    {
        sharedLabelLock( state, labelId );
        state.assertOpen();
        return schemaReadDelegate.constraintsGetForLabel( state, labelId );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( KernelStatement state,
            int typeId )
    {
        sharedRelationshipTypeLock( state, typeId );
        state.assertOpen();
        return schemaReadDelegate.constraintsGetForRelationshipType( state, typeId );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll( KernelStatement state )
    {
        state.assertOpen();
        return Iterators.consumableIterator( schemaReadDelegate.constraintsGetAll( state ), constraintDescriptor ->
        {
            SchemaDescriptor schema = constraintDescriptor.schema();
            acquireShared( state, schema.resourceType(), schema.resourceId() );
        } );
    }

    @Override
    public void constraintDrop( KernelStatement state, ConstraintDescriptor constraint )
            throws DropConstraintFailureException
    {
        SchemaDescriptor schema = constraint.schema();
        exclusiveLock( state, schema.resourceType(), schema.resourceId() );
        state.assertOpen();
        schemaWriteDelegate.constraintDrop( state, constraint );
    }

    @Override
    public Property nodeSetProperty( KernelStatement state, long nodeId, DefinedProperty property )
            throws ConstraintValidationException, EntityNotFoundException, AutoIndexingKernelException,
            InvalidTransactionTypeKernelException
    {
        acquireExclusiveNodeLock( state, nodeId );
        state.assertOpen();
        return entityWriteDelegate.nodeSetProperty( state, nodeId, property );
    }

    @Override
    public Property nodeRemoveProperty( KernelStatement state, long nodeId, int propertyKeyId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        acquireExclusiveNodeLock( state, nodeId );
        state.assertOpen();
        return entityWriteDelegate.nodeRemoveProperty( state, nodeId, propertyKeyId );
    }

    @Override
    public Property relationshipSetProperty( KernelStatement state,
            long relationshipId,
            DefinedProperty property ) throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        acquireExclusiveRelationshipLock( state, relationshipId );
        state.assertOpen();
        return entityWriteDelegate.relationshipSetProperty( state, relationshipId, property );
    }

    @Override
    public Property relationshipRemoveProperty( KernelStatement state,
            long relationshipId,
            int propertyKeyId ) throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        acquireExclusiveRelationshipLock( state, relationshipId );
        state.assertOpen();
        return entityWriteDelegate.relationshipRemoveProperty( state, relationshipId, propertyKeyId );
    }

    @Override
    public Property graphSetProperty( KernelStatement state, DefinedProperty property )
    {
        state.locks().optimistic().acquireExclusive( state.lockTracer(), ResourceTypes.GRAPH_PROPS, ResourceTypes.graphPropertyResource() );
        state.assertOpen();
        return entityWriteDelegate.graphSetProperty( state, property );
    }

    @Override
    public Property graphRemoveProperty( KernelStatement state, int propertyKeyId )
    {
        state.locks().optimistic().acquireExclusive( state.lockTracer(), ResourceTypes.GRAPH_PROPS, ResourceTypes.graphPropertyResource() );
        state.assertOpen();
        return entityWriteDelegate.graphRemoveProperty( state, propertyKeyId );
    }

    @Override
    public void acquireExclusive( KernelStatement state, ResourceType resourceType, long resourceId )
    {
        state.locks().pessimistic().acquireExclusive( state.lockTracer(), resourceType, resourceId );
        state.assertOpen();
    }

    @Override
    public void acquireShared( KernelStatement state, ResourceType resourceType, long resourceId )
    {
        state.locks().pessimistic().acquireShared( state.lockTracer(), resourceType, resourceId );
        state.assertOpen();
    }

    @Override
    public void releaseExclusive( KernelStatement state, ResourceType type, long resourceId )
    {
        state.locks().pessimistic().releaseExclusive( type, resourceId );
        state.assertOpen();
    }

    @Override
    public void releaseShared( KernelStatement state, ResourceType type, long resourceId )
    {
        state.locks().pessimistic().releaseShared( type, resourceId );
        state.assertOpen();
    }

    @Override
    public String indexGetFailure( Statement state, IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        return schemaReadDelegate.indexGetFailure( state, descriptor );
    }

    private void acquireExclusiveNodeLock( KernelStatement state, long nodeId )
    {
        if ( !state.hasTxStateWithChanges() || !state.txState().nodeIsAddedInThisTx( nodeId ) )
        {
            exclusiveLock( state, ResourceTypes.NODE, nodeId );
        }
    }

    private void acquireExclusiveRelationshipLock( KernelStatement state, long relationshipId )
    {
        if ( !state.hasTxStateWithChanges() || !state.txState().relationshipIsAddedInThisTx( relationshipId ) )
        {
            exclusiveLock( state, ResourceTypes.RELATIONSHIP, relationshipId );
        }
    }

    private void exclusiveLabelLock( KernelStatement state, long labelId )
    {
        exclusiveLock( state, ResourceTypes.LABEL, labelId );
    }

    private void sharedLabelLock( KernelStatement state, long labelId )
    {
        sharedLock( state, ResourceTypes.LABEL, labelId );
    }

    private void exclusiveRelationshipTypeLock( KernelStatement state, long typeId )
    {
        exclusiveLock( state, ResourceTypes.RELATIONSHIP_TYPE, typeId );
    }

    private void sharedRelationshipTypeLock( KernelStatement state, long typeId )
    {
        sharedLock( state, ResourceTypes.RELATIONSHIP_TYPE, typeId );
    }

    private void sharedLock( KernelStatement statement, ResourceType resource, long resourceId )
    {
        statement.locks().optimistic().acquireShared( statement.lockTracer(), resource, resourceId );
    }

    private void exclusiveLock( KernelStatement statement, ResourceType resource, long resourceId )
    {
        statement.locks().optimistic().acquireExclusive( statement.lockTracer(), resource, resourceId );
    }
}
