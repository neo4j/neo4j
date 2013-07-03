/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.EntityWriteOperations;
import org.neo4j.kernel.api.operations.SchemaReadOperations;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

public class LockingStatementContext implements
    EntityWriteOperations,
    SchemaReadOperations,
    SchemaWriteOperations,
    SchemaStateOperations
{
    private final LockHolder lockHolder;
    private final EntityWriteOperations entityWriteDelegate;
    private final SchemaReadOperations schemaReadDelegate;
    private final SchemaWriteOperations schemaWriteDelegate;
    private final SchemaStateOperations schemaStateDelegate;

    public LockingStatementContext(
            EntityWriteOperations entityWriteDelegate,
            SchemaReadOperations schemaReadDelegate,
            SchemaWriteOperations schemaWriteDelegate,
            SchemaStateOperations schemaStateDelegate,
            LockHolder lockHolder )
    {
        this.entityWriteDelegate = entityWriteDelegate;
        this.schemaReadDelegate = schemaReadDelegate;
        this.schemaWriteDelegate = schemaWriteDelegate;
        this.schemaStateDelegate = schemaStateDelegate;
        this.lockHolder = lockHolder;
    }

    @Override
    public boolean nodeAddLabel( long nodeId, long labelId ) throws EntityNotFoundException
    {
        lockHolder.acquireNodeWriteLock( nodeId );
        return entityWriteDelegate.nodeAddLabel( nodeId, labelId );
    }

    @Override
    public boolean nodeRemoveLabel( long nodeId, long labelId ) throws EntityNotFoundException
    {
        lockHolder.acquireNodeWriteLock( nodeId );
        return entityWriteDelegate.nodeRemoveLabel( nodeId, labelId );
    }

    @Override
    public IndexDescriptor indexCreate( long labelId, long propertyKey ) throws SchemaKernelException
    {
        lockHolder.acquireSchemaWriteLock();
        return schemaWriteDelegate.indexCreate( labelId, propertyKey );
    }

    @Override
    public IndexDescriptor uniqueIndexCreate( long labelId, long propertyKey ) throws SchemaKernelException
    {
        lockHolder.acquireSchemaWriteLock();
        return schemaWriteDelegate.uniqueIndexCreate( labelId, propertyKey );
    }

    @Override
    public void indexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        lockHolder.acquireSchemaWriteLock();
        schemaWriteDelegate.indexDrop( descriptor );
    }

    @Override
    public void uniqueIndexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        lockHolder.acquireSchemaWriteLock();
        schemaWriteDelegate.uniqueIndexDrop( descriptor );
    }

    @Override
    public <K, V> V schemaStateGetOrCreate( K key, Function<K, V> creator )
    {
        lockHolder.acquireSchemaReadLock();
        return schemaStateDelegate.schemaStateGetOrCreate( key, creator );
    }

    @Override
    public <K> boolean schemaStateContains( K key )
    {
        lockHolder.acquireSchemaReadLock();
        return schemaStateDelegate.schemaStateContains( key );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( long labelId )
    {
        lockHolder.acquireSchemaReadLock();
        return schemaReadDelegate.indexesGetForLabel( labelId );
    }
    
    @Override
    public IndexDescriptor indexesGetForLabelAndPropertyKey( long labelId, long propertyKey )
            throws SchemaRuleNotFoundException
    {
        lockHolder.acquireSchemaReadLock();
        return schemaReadDelegate.indexesGetForLabelAndPropertyKey( labelId, propertyKey );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        lockHolder.acquireSchemaReadLock();
        return schemaReadDelegate.indexesGetAll();
    }
    
    @Override
    public InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        lockHolder.acquireSchemaReadLock();
        return schemaReadDelegate.indexGetState( descriptor );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        lockHolder.acquireSchemaReadLock();
        return schemaReadDelegate.indexGetOwningUniquenessConstraintId( index );
    }

    @Override
    public long indexGetCommittedId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        lockHolder.acquireSchemaReadLock();
        return schemaReadDelegate.indexGetCommittedId( index );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( long labelId )
    {
        lockHolder.acquireSchemaReadLock();
        return schemaReadDelegate.uniqueIndexesGetForLabel( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll()
    {
        lockHolder.acquireSchemaReadLock();
        return schemaReadDelegate.uniqueIndexesGetAll();
    }

    @Override
    public void nodeDelete( long nodeId )
    {
        lockHolder.acquireNodeWriteLock( nodeId );
        entityWriteDelegate.nodeDelete( nodeId );
    }
    
    @Override
    public void relationshipDelete( long relationshipId )
    {
        lockHolder.acquireRelationshipWriteLock( relationshipId );
        entityWriteDelegate.relationshipDelete( relationshipId );
    }

    @Override
    public UniquenessConstraint uniquenessConstraintCreate( long labelId, long propertyKeyId )
            throws SchemaKernelException
    {
        lockHolder.acquireSchemaWriteLock();
        return schemaWriteDelegate.uniquenessConstraintCreate( labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( long labelId, long propertyKeyId )
    {
        lockHolder.acquireSchemaReadLock();
        return schemaReadDelegate.constraintsGetForLabelAndPropertyKey( labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabel( long labelId )
    {
        lockHolder.acquireSchemaReadLock();
        return schemaReadDelegate.constraintsGetForLabel( labelId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetAll()
    {
        lockHolder.acquireSchemaReadLock();
        return schemaReadDelegate.constraintsGetAll();
    }

    @Override
    public void constraintDrop( UniquenessConstraint constraint )
    {
        lockHolder.acquireSchemaWriteLock();
        schemaWriteDelegate.constraintDrop( constraint );
    }
    
    @Override
    public Property nodeSetProperty( long nodeId, Property property ) throws PropertyKeyIdNotFoundException,
            EntityNotFoundException
    {
        lockHolder.acquireNodeWriteLock( nodeId );
        return entityWriteDelegate.nodeSetProperty( nodeId, property );
    }
    
    @Override
    public Property nodeRemoveProperty( long nodeId, long propertyKeyId ) throws PropertyKeyIdNotFoundException,
            EntityNotFoundException
    {
        lockHolder.acquireNodeWriteLock( nodeId );
        return entityWriteDelegate.nodeRemoveProperty( nodeId, propertyKeyId );
    }
    
    @Override
    public Property relationshipSetProperty( long relationshipId, Property property )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        lockHolder.acquireRelationshipWriteLock( relationshipId );
        return entityWriteDelegate.relationshipSetProperty( relationshipId, property );
    }
    
    @Override
    public Property relationshipRemoveProperty( long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        lockHolder.acquireRelationshipWriteLock( relationshipId );
        return entityWriteDelegate.relationshipRemoveProperty( relationshipId, propertyKeyId );
    }
    
    @Override
    public Property graphSetProperty( Property property ) throws PropertyKeyIdNotFoundException
    {
        lockHolder.acquireGraphWriteLock();
        return entityWriteDelegate.graphSetProperty( property );
    }
    
    @Override
    public Property graphRemoveProperty( long propertyKeyId ) throws PropertyKeyIdNotFoundException
    {
        lockHolder.acquireGraphWriteLock();
        return entityWriteDelegate.graphRemoveProperty( propertyKeyId );
    }
    
    // === TODO Below is unnecessary delegate methods
    @Override
    public String indexGetFailure( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return schemaReadDelegate.indexGetFailure( descriptor );
    }
}
