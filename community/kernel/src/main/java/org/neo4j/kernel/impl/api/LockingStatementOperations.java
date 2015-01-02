/**
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

import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.AddIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaStateOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.nioneo.store.SchemaStorage;

public class LockingStatementOperations implements
    EntityWriteOperations,
    SchemaReadOperations,
    SchemaWriteOperations,
    SchemaStateOperations
{
    private final EntityWriteOperations entityWriteDelegate;
    private final SchemaReadOperations schemaReadDelegate;
    private final SchemaWriteOperations schemaWriteDelegate;
    private final SchemaStateOperations schemaStateDelegate;

    public LockingStatementOperations(
            EntityWriteOperations entityWriteDelegate,
            SchemaReadOperations schemaReadDelegate,
            SchemaWriteOperations schemaWriteDelegate,
            SchemaStateOperations schemaStateDelegate )
    {
        this.entityWriteDelegate = entityWriteDelegate;
        this.schemaReadDelegate = schemaReadDelegate;
        this.schemaWriteDelegate = schemaWriteDelegate;
        this.schemaStateDelegate = schemaStateDelegate;
    }

    @Override
    public boolean nodeAddLabel( KernelStatement state, long nodeId, int labelId )
            throws EntityNotFoundException, ConstraintValidationKernelException
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
        state.locks().acquireSchemaReadLock();

        state.locks().acquireNodeWriteLock( nodeId );
        return entityWriteDelegate.nodeAddLabel( state, nodeId, labelId );
    }

    @Override
    public boolean nodeRemoveLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException
    {
        state.locks().acquireNodeWriteLock( nodeId );
        return entityWriteDelegate.nodeRemoveLabel( state, nodeId, labelId );
    }

    @Override
    public IndexDescriptor indexCreate( KernelStatement state, int labelId, int propertyKey )
            throws AddIndexFailureException, AlreadyIndexedException, AlreadyConstrainedException
    {
        state.locks().acquireSchemaWriteLock();
        return schemaWriteDelegate.indexCreate( state, labelId, propertyKey );
    }

    @Override
    public void indexDrop( KernelStatement state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        state.locks().acquireSchemaWriteLock();
        schemaWriteDelegate.indexDrop( state, descriptor );
    }

    @Override
    public void uniqueIndexDrop( KernelStatement state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        state.locks().acquireSchemaWriteLock();
        schemaWriteDelegate.uniqueIndexDrop( state, descriptor );
    }

    @Override
    public <K, V> V schemaStateGetOrCreate( KernelStatement state, K key, Function<K, V> creator )
    {
        state.locks().acquireSchemaReadLock();
        return schemaStateDelegate.schemaStateGetOrCreate( state, key, creator );
    }

    @Override
    public <K> boolean schemaStateContains( KernelStatement state, K key )
    {
        state.locks().acquireSchemaReadLock();
        return schemaStateDelegate.schemaStateContains( state, key );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( KernelStatement state, int labelId )
    {
        state.locks().acquireSchemaReadLock();
        return schemaReadDelegate.indexesGetForLabel( state, labelId );
    }

    @Override
    public IndexDescriptor indexesGetForLabelAndPropertyKey( KernelStatement state, int labelId, int propertyKey )
            throws SchemaRuleNotFoundException
    {
        state.locks().acquireSchemaReadLock();
        return schemaReadDelegate.indexesGetForLabelAndPropertyKey( state, labelId, propertyKey );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll( KernelStatement state )
    {
        state.locks().acquireSchemaReadLock();
        return schemaReadDelegate.indexesGetAll( state );
    }

    @Override
    public InternalIndexState indexGetState( KernelStatement state, IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        state.locks().acquireSchemaReadLock();
        return schemaReadDelegate.indexGetState( state, descriptor );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( KernelStatement state, IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        state.locks().acquireSchemaReadLock();
        return schemaReadDelegate.indexGetOwningUniquenessConstraintId( state, index );
    }

    @Override
    public long indexGetCommittedId( KernelStatement state, IndexDescriptor index, SchemaStorage.IndexRuleKind kind )
            throws SchemaRuleNotFoundException
    {
        state.locks().acquireSchemaReadLock();
        return schemaReadDelegate.indexGetCommittedId( state, index, kind );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( KernelStatement state, int labelId )
    {
        state.locks().acquireSchemaReadLock();
        return schemaReadDelegate.uniqueIndexesGetForLabel( state, labelId );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll( KernelStatement state )
    {
        state.locks().acquireSchemaReadLock();
        return schemaReadDelegate.uniqueIndexesGetAll( state );
    }

    @Override
    public void nodeDelete( KernelStatement state, long nodeId )
    {
        state.locks().acquireNodeWriteLock( nodeId );
        entityWriteDelegate.nodeDelete( state, nodeId );
    }

    @Override
    public void relationshipDelete( KernelStatement state, long relationshipId )
    {
        state.locks().acquireRelationshipWriteLock( relationshipId );
        entityWriteDelegate.relationshipDelete( state, relationshipId );
    }

    @Override
    public UniquenessConstraint uniquenessConstraintCreate( KernelStatement state, int labelId, int propertyKeyId )
            throws CreateConstraintFailureException, AlreadyConstrainedException, AlreadyIndexedException
    {
        state.locks().acquireSchemaWriteLock();
        return schemaWriteDelegate.uniquenessConstraintCreate( state, labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( KernelStatement state, int labelId, int propertyKeyId )
    {
        state.locks().acquireSchemaReadLock();
        return schemaReadDelegate.constraintsGetForLabelAndPropertyKey( state, labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabel( KernelStatement state, int labelId )
    {
        state.locks().acquireSchemaReadLock();
        return schemaReadDelegate.constraintsGetForLabel( state, labelId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetAll( KernelStatement state )
    {
        state.locks().acquireSchemaReadLock();
        return schemaReadDelegate.constraintsGetAll( state );
    }

    @Override
    public void constraintDrop( KernelStatement state, UniquenessConstraint constraint )
            throws DropConstraintFailureException
    {
        state.locks().acquireSchemaWriteLock();
        schemaWriteDelegate.constraintDrop( state, constraint );
    }

    @Override
    public Property nodeSetProperty( KernelStatement state, long nodeId, DefinedProperty property )
            throws EntityNotFoundException, ConstraintValidationKernelException
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
        state.locks().acquireSchemaReadLock();

        state.locks().acquireNodeWriteLock( nodeId );
        return entityWriteDelegate.nodeSetProperty( state, nodeId, property );
    }

    @Override
    public Property nodeRemoveProperty( KernelStatement state, long nodeId, int propertyKeyId )
            throws EntityNotFoundException
    {
        state.locks().acquireNodeWriteLock( nodeId );
        return entityWriteDelegate.nodeRemoveProperty( state, nodeId, propertyKeyId );
    }

    @Override
    public Property relationshipSetProperty( KernelStatement state, long relationshipId, DefinedProperty property )
            throws EntityNotFoundException
    {
        state.locks().acquireRelationshipWriteLock( relationshipId );
        return entityWriteDelegate.relationshipSetProperty( state, relationshipId, property );
    }

    @Override
    public Property relationshipRemoveProperty( KernelStatement state, long relationshipId, int propertyKeyId )
            throws EntityNotFoundException
    {
        state.locks().acquireRelationshipWriteLock( relationshipId );
        return entityWriteDelegate.relationshipRemoveProperty( state, relationshipId, propertyKeyId );
    }

    @Override
    public Property graphSetProperty( KernelStatement state, DefinedProperty property )
    {
        state.locks().acquireGraphWriteLock();
        return entityWriteDelegate.graphSetProperty( state, property );
    }

    @Override
    public Property graphRemoveProperty( KernelStatement state, int propertyKeyId )
    {
        state.locks().acquireGraphWriteLock();
        return entityWriteDelegate.graphRemoveProperty( state, propertyKeyId );
    }

    // === TODO Below is unnecessary delegate methods
    @Override
    public String indexGetFailure( Statement state, IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        return schemaReadDelegate.indexGetFailure( state, descriptor );
    }
}
