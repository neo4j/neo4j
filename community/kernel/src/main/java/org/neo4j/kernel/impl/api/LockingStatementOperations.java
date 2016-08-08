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

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
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
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.kernel.impl.api.operations.LockOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaStateOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.neo4j.kernel.impl.locking.ResourceTypes.schemaResource;
import static org.neo4j.unsafe.impl.internal.dragons.FeatureToggles.flag;

public class LockingStatementOperations implements
        EntityWriteOperations,
        SchemaReadOperations,
        SchemaWriteOperations,
        SchemaStateOperations,
        LockOperations
{
    private static final boolean SCHEMA_WRITES_DISABLE =
            flag( LockingStatementOperations.class, "schemaWritesDisable", false );

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
            throws ConstraintValidationKernelException, EntityNotFoundException
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
        acquireSharedSchemaLock( state );

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
    public IndexDescriptor indexCreate( KernelStatement state, int labelId, int propertyKey )
            throws AlreadyIndexedException, AlreadyConstrainedException
    {
        acquireExclusiveSchemaLock( state );
        state.assertOpen();
        return schemaWriteDelegate.indexCreate( state, labelId, propertyKey );
    }

    @Override
    public void indexDrop( KernelStatement state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        acquireExclusiveSchemaLock( state );
        state.assertOpen();
        schemaWriteDelegate.indexDrop( state, descriptor );
    }

    @Override
    public void uniqueIndexDrop( KernelStatement state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        acquireExclusiveSchemaLock( state );
        state.assertOpen();
        schemaWriteDelegate.uniqueIndexDrop( state, descriptor );
    }

    @Override
    public <K, V> V schemaStateGetOrCreate( KernelStatement state, K key, Function<K,V> creator )
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaStateDelegate.schemaStateGetOrCreate( state, key, creator );
    }

    @Override
    public <K> boolean schemaStateContains( KernelStatement state, K key )
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaStateDelegate.schemaStateContains( state, key );
    }

    @Override
    public void schemaStateFlush( KernelStatement state )
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        schemaStateDelegate.schemaStateFlush( state );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( KernelStatement state, int labelId )
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaReadDelegate.indexesGetForLabel( state, labelId );
    }

    @Override
    public IndexDescriptor indexGetForLabelAndPropertyKey( KernelStatement state, int labelId, int propertyKey )
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaReadDelegate.indexGetForLabelAndPropertyKey( state, labelId, propertyKey );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll( KernelStatement state )
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaReadDelegate.indexesGetAll( state );
    }

    @Override
    public InternalIndexState indexGetState( KernelStatement state, IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaReadDelegate.indexGetState( state, descriptor );
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( KernelStatement state, IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaReadDelegate.indexGetPopulationProgress( state, descriptor );
    }

    @Override
    public long indexSize( KernelStatement state, IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaReadDelegate.indexSize( state, descriptor );
    }

    @Override
    public double indexUniqueValuesPercentage( KernelStatement state,
            IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaReadDelegate.indexUniqueValuesPercentage( state, descriptor );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( KernelStatement state, IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaReadDelegate.indexGetOwningUniquenessConstraintId( state, index );
    }

    @Override
    public long indexGetCommittedId( KernelStatement state, IndexDescriptor index, Predicate<SchemaRule.Kind> filter )
            throws SchemaRuleNotFoundException
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaReadDelegate.indexGetCommittedId( state, index, filter );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( KernelStatement state, int labelId )
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaReadDelegate.uniqueIndexesGetForLabel( state, labelId );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll( KernelStatement state )
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaReadDelegate.uniqueIndexesGetAll( state );
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
    public int nodeDetachDelete( final KernelStatement state, final long nodeId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException, KernelException
    {
        final AtomicInteger count = new AtomicInteger();
        TwoPhaseNodeForRelationshipLocking locking = new TwoPhaseNodeForRelationshipLocking( entityReadDelegate,
                relId -> {
                    state.assertOpen();
                    try
                    {
                        entityWriteDelegate.relationshipDelete( state, relId );
                        count.incrementAndGet();
                    }
                    catch ( EntityNotFoundException e )
                    {
                        // it doesn't matter...
                    }
                } );

        locking.lockAllNodesAndConsumeRelationships( nodeId, state );
        state.assertOpen();
        entityWriteDelegate.nodeDetachDelete( state, nodeId );
        return count.get();
    }

    @Override
    public long nodeCreate( KernelStatement statement )
    {
        return entityWriteDelegate.nodeCreate( statement );
    }

    @Override
    public long relationshipCreate( KernelStatement state,
            int relationshipTypeId,
            long startNodeId,
            long endNodeId )
            throws EntityNotFoundException
    {
        acquireSharedSchemaLock( state );
        lockRelationshipNodes( state, startNodeId, endNodeId );
        return entityWriteDelegate.relationshipCreate( state, relationshipTypeId, startNodeId, endNodeId );
    }

    @Override
    public void relationshipDelete( final KernelStatement state, long relationshipId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        entityReadDelegate.relationshipVisit(state, relationshipId, new RelationshipVisitor<RuntimeException>() {
            @Override
            public void visit(long relId, int type, long startNode, long endNode) {
                lockRelationshipNodes(state, startNode, endNode);
            }
        });
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
    public UniquenessConstraint uniquePropertyConstraintCreate( KernelStatement state, int labelId, int propertyKeyId )
            throws CreateConstraintFailureException, AlreadyConstrainedException, AlreadyIndexedException
    {
        acquireExclusiveSchemaLock( state );
        state.assertOpen();
        return schemaWriteDelegate.uniquePropertyConstraintCreate( state, labelId, propertyKeyId );
    }

    @Override
    public NodePropertyExistenceConstraint nodePropertyExistenceConstraintCreate( KernelStatement state, int labelId,
            int propertyKeyId ) throws AlreadyConstrainedException, CreateConstraintFailureException
    {
        acquireExclusiveSchemaLock( state );
        state.assertOpen();
        return schemaWriteDelegate.nodePropertyExistenceConstraintCreate( state, labelId, propertyKeyId );
    }

    @Override
    public RelationshipPropertyExistenceConstraint relationshipPropertyExistenceConstraintCreate( KernelStatement state,
            int relTypeId, int propertyKeyId ) throws AlreadyConstrainedException, CreateConstraintFailureException
    {
        acquireExclusiveSchemaLock( state );
        state.assertOpen();
        return schemaWriteDelegate.relationshipPropertyExistenceConstraintCreate( state, relTypeId, propertyKeyId );
    }

    @Override
    public Iterator<NodePropertyConstraint> constraintsGetForLabelAndPropertyKey( KernelStatement state,
            int labelId,
            int propertyKeyId )
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaReadDelegate.constraintsGetForLabelAndPropertyKey( state, labelId, propertyKeyId );
    }

    @Override
    public Iterator<NodePropertyConstraint> constraintsGetForLabel( KernelStatement state, int labelId )
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaReadDelegate.constraintsGetForLabel( state, labelId );
    }

    @Override
    public Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipTypeAndPropertyKey(
            KernelStatement state,
            int relTypeId, int propertyKeyId )
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaReadDelegate.constraintsGetForRelationshipTypeAndPropertyKey( state, relTypeId, propertyKeyId );
    }

    @Override
    public Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipType( KernelStatement state,
            int typeId )
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaReadDelegate.constraintsGetForRelationshipType( state, typeId );
    }

    @Override
    public Iterator<PropertyConstraint> constraintsGetAll( KernelStatement state )
    {
        acquireSharedSchemaLock( state );
        state.assertOpen();
        return schemaReadDelegate.constraintsGetAll( state );
    }

    @Override
    public void constraintDrop( KernelStatement state, NodePropertyConstraint constraint )
            throws DropConstraintFailureException
    {
        acquireExclusiveSchemaLock( state );
        state.assertOpen();
        schemaWriteDelegate.constraintDrop( state, constraint );
    }

    @Override
    public void constraintDrop( KernelStatement state, RelationshipPropertyConstraint constraint )
            throws DropConstraintFailureException
    {
        acquireExclusiveSchemaLock( state );
        state.assertOpen();
        schemaWriteDelegate.constraintDrop( state, constraint );
    }

    @Override
    public Property nodeSetProperty( KernelStatement state, long nodeId, DefinedProperty property )
            throws ConstraintValidationKernelException, EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
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
        acquireSharedSchemaLock( state );

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
        state.locks().optimistic().acquireExclusive( ResourceTypes.GRAPH_PROPS, ResourceTypes.graphPropertyResource() );
        state.assertOpen();
        return entityWriteDelegate.graphSetProperty( state, property );
    }

    @Override
    public Property graphRemoveProperty( KernelStatement state, int propertyKeyId )
    {
        state.locks().optimistic().acquireExclusive( ResourceTypes.GRAPH_PROPS, ResourceTypes.graphPropertyResource() );
        state.assertOpen();
        return entityWriteDelegate.graphRemoveProperty( state, propertyKeyId );
    }

    @Override
    public void acquireExclusive( KernelStatement state, ResourceType resourceType, long resourceId )
    {
        state.locks().pessimistic().acquireExclusive( resourceType, resourceId );
        state.assertOpen();
    }

    @Override
    public void acquireShared( KernelStatement state, ResourceType resourceType, long resourceId )
    {
        state.locks().pessimistic().acquireShared( resourceType, resourceId );
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

    // === TODO Below is unnecessary delegate methods
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
            state.locks().optimistic().acquireExclusive( ResourceTypes.NODE, nodeId );
        }
    }

    private void acquireExclusiveRelationshipLock( KernelStatement state, long relationshipId )
    {
        if ( !state.hasTxStateWithChanges() || !state.txState().relationshipIsAddedInThisTx( relationshipId ) )
        {
            state.locks().optimistic().acquireExclusive( ResourceTypes.RELATIONSHIP, relationshipId );
        }
    }

    private void acquireSharedSchemaLock( KernelStatement state )
    {
        if ( !SCHEMA_WRITES_DISABLE )
        {
            state.locks().optimistic().acquireShared( ResourceTypes.SCHEMA, schemaResource() );
        }
    }

    private void acquireExclusiveSchemaLock( KernelStatement state )
    {
        if ( SCHEMA_WRITES_DISABLE )
        {
            throw new IllegalStateException( "Schema modifications have been disabled via feature toggle" );
        }
        else
        {
            state.locks().optimistic().acquireExclusive( ResourceTypes.SCHEMA, schemaResource() );
        }
    }
}
