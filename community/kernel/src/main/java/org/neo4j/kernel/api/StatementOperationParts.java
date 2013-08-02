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
package org.neo4j.kernel.api;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.EntityReadOperations;
import org.neo4j.kernel.api.operations.EntityWriteOperations;
import org.neo4j.kernel.api.operations.KeyReadOperations;
import org.neo4j.kernel.api.operations.KeyWriteOperations;
import org.neo4j.kernel.api.operations.SchemaReadOperations;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.Token;

public class StatementOperationParts
{
    private final KeyReadOperations keyReadOperations;
    private final KeyWriteOperations keyWriteOperations;
    private final EntityReadOperations entityReadOperations;
    private final EntityWriteOperations entityWriteOperations;
    private final SchemaReadOperations schemaReadOperations;
    private final SchemaWriteOperations schemaWriteOperations;
    private final SchemaStateOperations schemaStateOperations;
    
    @SuppressWarnings( "rawtypes" )
    private Map<Class,Object> additionalParts;

    public StatementOperationParts(
            KeyReadOperations keyReadOperations,
            KeyWriteOperations keyWriteOperations,
            EntityReadOperations entityReadOperations,
            EntityWriteOperations entityWriteOperations,
            SchemaReadOperations schemaReadOperations,
            SchemaWriteOperations schemaWriteOperations,
            SchemaStateOperations schemaStateOperations )
    {
        this.keyReadOperations = keyReadOperations;
        this.keyWriteOperations = keyWriteOperations;
        this.entityReadOperations = entityReadOperations;
        this.entityWriteOperations = entityWriteOperations;
        this.schemaReadOperations = schemaReadOperations;
        this.schemaWriteOperations = schemaWriteOperations;
        this.schemaStateOperations = schemaStateOperations;
    }
    
    public <T> StatementOperationParts additionalPart( Class<T> cls, T value )
    {
        if ( additionalParts == null )
        {
            additionalParts = new HashMap<>();
        }
        additionalParts.put( cls, value );
        return this;
    }
    
    @SuppressWarnings( "unchecked" )
    public <T> T resolve( Class<T> cls )
    {
        T part = additionalParts != null ? (T) additionalParts.get( cls ) : null;
        if ( part == null )
        {
            throw new IllegalArgumentException( "No part " + cls.getName() );
        }
        return part;
    }

    public KeyReadOperations keyReadOperations()
    {
        return checkNotNull( keyReadOperations, KeyReadOperations.class );
    }

    public KeyWriteOperations keyWriteOperations()
    {
        return checkNotNull( keyWriteOperations, KeyWriteOperations.class );
    }

    public EntityReadOperations entityReadOperations()
    {
        return checkNotNull( entityReadOperations, EntityReadOperations.class );
    }

    public EntityWriteOperations entityWriteOperations()
    {
        return checkNotNull( entityWriteOperations, EntityWriteOperations.class );
    }

    public SchemaReadOperations schemaReadOperations()
    {
        return checkNotNull( schemaReadOperations, SchemaReadOperations.class );
    }

    public SchemaWriteOperations schemaWriteOperations()
    {
        return checkNotNull( schemaWriteOperations, SchemaWriteOperations.class );
    }

    public SchemaStateOperations schemaStateOperations()
    {
        return checkNotNull( schemaStateOperations, SchemaStateOperations.class );
    }

    @SuppressWarnings( { "unchecked", "rawtypes" } )
    public StatementOperationParts override(
            KeyReadOperations keyReadOperations,
            KeyWriteOperations keyWriteOperations,
            EntityReadOperations entityReadOperations,
            EntityWriteOperations entityWriteOperations,
            SchemaReadOperations schemaReadOperations,
            SchemaWriteOperations schemaWriteOperations,
            SchemaStateOperations schemaStateOperations,
            Object... alternatingAdditionalClassAndObject )
    {
        StatementOperationParts parts = new StatementOperationParts(
                eitherOr( keyReadOperations, this.keyReadOperations, KeyReadOperations.class ),
                eitherOr( keyWriteOperations, this.keyWriteOperations, KeyWriteOperations.class ),
                eitherOr( entityReadOperations, this.entityReadOperations, EntityReadOperations.class ),
                eitherOr( entityWriteOperations, this.entityWriteOperations, EntityWriteOperations.class ),
                eitherOr( schemaReadOperations, this.schemaReadOperations, SchemaReadOperations.class ),
                eitherOr( schemaWriteOperations, this.schemaWriteOperations, SchemaWriteOperations.class ),
                eitherOr( schemaStateOperations, this.schemaStateOperations, SchemaStateOperations.class ));

        if ( additionalParts != null )
        {
            parts.additionalParts = new HashMap<>( additionalParts );
        }
        for ( int i = 0; i < alternatingAdditionalClassAndObject.length; i++ )
        {
            parts.additionalPart( (Class) alternatingAdditionalClassAndObject[i++],
                    alternatingAdditionalClassAndObject[i] );
        }

        return parts;
    }
    
    private <T> T checkNotNull( T object, Class<T> cls )
    {
        if ( object == null )
        {
            throw new IllegalStateException( "No part of type " + cls.getSimpleName() + " assigned" );
        }
        return object;
    }

    private <T> T eitherOr( T first, T other, Class<T> cls )
    {
        return first != null ? first : other;
    }
    
    /**
     * @deprecated Transitional method as long as consumers of the {@link KernelAPI} still always deal with the full
     * {@link StatementOperations} interface. As they will access individual parts instead this should go away.
     * This is just for convenience, but should be removed since it adds one level of delegation.
     *  
     * @return all these parts as a unified {@link StatementOperations} instance.
     */
    @Deprecated
    public StatementOperations asStatementOperations()
    {
        return new StatementOperations()
        {
            @Override
            public long labelGetForName( StatementState state, String labelName ) throws LabelNotFoundKernelException
            {
                return keyReadOperations.labelGetForName( state, labelName );
            }
            @Override
            public String labelGetName( StatementState state, long labelId ) throws LabelNotFoundKernelException
            {
                return keyReadOperations.labelGetName( state, labelId );
            }
            @Override
            public long propertyKeyGetForName( StatementState state, String propertyKeyName ) throws PropertyKeyNotFoundException
            {
                return keyReadOperations.propertyKeyGetForName( state, propertyKeyName );
            }
            @Override
            public String propertyKeyGetName( StatementState state, long propertyKeyId ) throws PropertyKeyIdNotFoundException
            {
                return keyReadOperations.propertyKeyGetName( state, propertyKeyId );
            }
            @Override
            public Iterator<Token> labelsGetAllTokens( StatementState state )
            {
                return keyReadOperations.labelsGetAllTokens(state);
            }
            @Override
            public long labelGetOrCreateForName( StatementState state, String labelName ) throws SchemaKernelException
            {
                return keyWriteOperations.labelGetOrCreateForName( state, labelName );
            }
            @Override
            public long propertyKeyGetOrCreateForName( StatementState state, String propertyKeyName ) throws SchemaKernelException
            {
                return keyWriteOperations.propertyKeyGetOrCreateForName( state, propertyKeyName );
            }
            @Override
            public PrimitiveLongIterator nodesGetForLabel( StatementState state, long labelId )
            {
                return entityReadOperations.nodesGetForLabel( state, labelId );
            }
            @Override
            public PrimitiveLongIterator nodesGetFromIndexLookup( StatementState state, IndexDescriptor index, Object value )
                    throws IndexNotFoundKernelException
            {
                return entityReadOperations.nodesGetFromIndexLookup( state, index, value );
            }
            @Override
            public boolean nodeHasLabel( StatementState state, long nodeId, long labelId ) throws EntityNotFoundException
            {
                return entityReadOperations.nodeHasLabel( state, nodeId, labelId );
            }
            @Override
            public PrimitiveLongIterator nodeGetLabels( StatementState state, long nodeId ) throws EntityNotFoundException
            {
                return entityReadOperations.nodeGetLabels( state, nodeId );
            }
            @Override
            public Property nodeGetProperty( StatementState state, long nodeId, long propertyKeyId ) throws PropertyKeyIdNotFoundException,
                    EntityNotFoundException
            {
                return entityReadOperations.nodeGetProperty( state, nodeId, propertyKeyId );
            }
            @Override
            public Property relationshipGetProperty( StatementState state, long relationshipId, long propertyKeyId )
                    throws PropertyKeyIdNotFoundException, EntityNotFoundException
            {
                return entityReadOperations.relationshipGetProperty( state, relationshipId, propertyKeyId );
            }
            @Override
            public Property graphGetProperty( StatementState state, long propertyKeyId ) throws PropertyKeyIdNotFoundException
            {
                return entityReadOperations.graphGetProperty( state, propertyKeyId );
            }
            @Override
            public boolean nodeHasProperty( StatementState state, long nodeId, long propertyKeyId ) throws PropertyKeyIdNotFoundException,
                    EntityNotFoundException
            {
                return entityReadOperations.nodeHasProperty( state, nodeId, propertyKeyId );
            }
            @Override
            public boolean relationshipHasProperty( StatementState state, long relationshipId, long propertyKeyId )
                    throws PropertyKeyIdNotFoundException, EntityNotFoundException
            {
                return entityReadOperations.relationshipHasProperty( state, relationshipId, propertyKeyId );
            }
            @Override
            public boolean graphHasProperty( StatementState state, long propertyKeyId ) throws PropertyKeyIdNotFoundException
            {
                return entityReadOperations.graphHasProperty( state, propertyKeyId );
            }
            @Override
            public PrimitiveLongIterator nodeGetPropertyKeys( StatementState state, long nodeId ) throws EntityNotFoundException
            {
                return entityReadOperations.nodeGetPropertyKeys( state, nodeId );
            }
            @Override
            public Iterator<Property> nodeGetAllProperties( StatementState state, long nodeId ) throws EntityNotFoundException
            {
                return entityReadOperations.nodeGetAllProperties( state, nodeId );
            }
            @Override
            public PrimitiveLongIterator relationshipGetPropertyKeys( StatementState state, long relationshipId ) throws EntityNotFoundException
            {
                return entityReadOperations.relationshipGetPropertyKeys( state, relationshipId );
            }
            @Override
            public Iterator<Property> relationshipGetAllProperties( StatementState state, long relationshipId )
                    throws EntityNotFoundException
            {
                return entityReadOperations.relationshipGetAllProperties( state, relationshipId );
            }
            @Override
            public PrimitiveLongIterator graphGetPropertyKeys( StatementState state )
            {
                return entityReadOperations.graphGetPropertyKeys(state);
            }
            @Override
            public Iterator<Property> graphGetAllProperties( StatementState state )
            {
                return entityReadOperations.graphGetAllProperties(state);
            }
            @Override
            public void nodeDelete( StatementState state, long nodeId )
            {
                entityWriteOperations.nodeDelete( state, nodeId );
            }
            @Override
            public void relationshipDelete( StatementState state, long relationshipId )
            {
                entityWriteOperations.relationshipDelete( state, relationshipId );
            }
            @Override
            public boolean nodeAddLabel( StatementState state, long nodeId, long labelId ) throws EntityNotFoundException
            {
                return entityWriteOperations.nodeAddLabel( state, nodeId, labelId );
            }
            @Override
            public boolean nodeRemoveLabel( StatementState state, long nodeId, long labelId ) throws EntityNotFoundException
            {
                return entityWriteOperations.nodeRemoveLabel( state, nodeId, labelId );
            }
            @Override
            public Property nodeSetProperty( StatementState state, long nodeId, Property property ) throws PropertyKeyIdNotFoundException,
                    EntityNotFoundException
            {
                return entityWriteOperations.nodeSetProperty( state, nodeId, property );
            }
            @Override
            public Property relationshipSetProperty( StatementState state, long relationshipId, Property property )
                    throws PropertyKeyIdNotFoundException, EntityNotFoundException
            {
                return entityWriteOperations.relationshipSetProperty( state, relationshipId, property );
            }
            @Override
            public Property graphSetProperty( StatementState state, Property property ) throws PropertyKeyIdNotFoundException
            {
                return entityWriteOperations.graphSetProperty( state, property );
            }
            @Override
            public Property nodeRemoveProperty( StatementState state, long nodeId, long propertyKeyId )
                    throws PropertyKeyIdNotFoundException, EntityNotFoundException
            {
                return entityWriteOperations.nodeRemoveProperty( state, nodeId, propertyKeyId );
            }
            @Override
            public Property relationshipRemoveProperty( StatementState state, long relationshipId, long propertyKeyId )
                    throws PropertyKeyIdNotFoundException, EntityNotFoundException
            {
                return entityWriteOperations.relationshipRemoveProperty( state, relationshipId, propertyKeyId );
            }
            @Override
            public Property graphRemoveProperty( StatementState state, long propertyKeyId ) throws PropertyKeyIdNotFoundException
            {
                return entityWriteOperations.graphRemoveProperty( state, propertyKeyId );
            }
            @Override
            public IndexDescriptor indexesGetForLabelAndPropertyKey( StatementState state, long labelId, long propertyKey )
                    throws SchemaRuleNotFoundException
            {
                return schemaReadOperations.indexesGetForLabelAndPropertyKey( state, labelId, propertyKey );
            }
            @Override
            public Iterator<IndexDescriptor> indexesGetForLabel( StatementState state, long labelId )
            {
                return schemaReadOperations.indexesGetForLabel( state, labelId );
            }
            @Override
            public Iterator<IndexDescriptor> indexesGetAll( StatementState state )
            {
                return schemaReadOperations.indexesGetAll(state);
            }
            @Override
            public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( StatementState state, long labelId )
            {
                return schemaReadOperations.uniqueIndexesGetForLabel( state, labelId );
            }
            @Override
            public Iterator<IndexDescriptor> uniqueIndexesGetAll( StatementState state )
            {
                return schemaReadOperations.uniqueIndexesGetAll(state);
            }
            @Override
            public InternalIndexState indexGetState( StatementState state, IndexDescriptor descriptor ) throws IndexNotFoundKernelException
            {
                return schemaReadOperations.indexGetState( state, descriptor );
            }
            @Override
            public String indexGetFailure( StatementState state, IndexDescriptor descriptor ) throws IndexNotFoundKernelException
            {
                return schemaReadOperations.indexGetFailure( state, descriptor );
            }
            @Override
            public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( StatementState state, long labelId, long propertyKeyId )
            {
                return schemaReadOperations.constraintsGetForLabelAndPropertyKey( state, labelId, propertyKeyId );
            }
            @Override
            public Iterator<UniquenessConstraint> constraintsGetForLabel( StatementState state, long labelId )
            {
                return schemaReadOperations.constraintsGetForLabel( state, labelId );
            }
            @Override
            public Iterator<UniquenessConstraint> constraintsGetAll( StatementState state )
            {
                return schemaReadOperations.constraintsGetAll(state);
            }
            @Override
            public Long indexGetOwningUniquenessConstraintId( StatementState state, IndexDescriptor index )
                    throws SchemaRuleNotFoundException
            {
                return schemaReadOperations.indexGetOwningUniquenessConstraintId( state, index );
            }
            @Override
            public long indexGetCommittedId( StatementState state, IndexDescriptor index ) throws SchemaRuleNotFoundException
            {
                return schemaReadOperations.indexGetCommittedId( state, index );
            }
            @Override
            public IndexDescriptor indexCreate( StatementState state, long labelId, long propertyKeyId ) throws SchemaKernelException
            {
                return schemaWriteOperations.indexCreate( state, labelId, propertyKeyId );
            }
            @Override
            public IndexDescriptor uniqueIndexCreate( StatementState state, long labelId, long propertyKey ) throws SchemaKernelException
            {
                return schemaWriteOperations.uniqueIndexCreate( state, labelId, propertyKey );
            }
            @Override
            public void indexDrop( StatementState state, IndexDescriptor descriptor ) throws DropIndexFailureException
            {
                schemaWriteOperations.indexDrop( state, descriptor );
            }
            @Override
            public void uniqueIndexDrop( StatementState state, IndexDescriptor descriptor ) throws DropIndexFailureException
            {
                schemaWriteOperations.uniqueIndexDrop( state, descriptor );
            }
            @Override
            public UniquenessConstraint uniquenessConstraintCreate( StatementState state, long labelId, long propertyKeyId )
                    throws SchemaKernelException
            {
                return schemaWriteOperations.uniquenessConstraintCreate( state, labelId, propertyKeyId );
            }
            @Override
            public void constraintDrop( StatementState state, UniquenessConstraint constraint )
            {
                schemaWriteOperations.constraintDrop( state, constraint );
            }
            @Override
            public <K, V> V schemaStateGetOrCreate( StatementState state, K key, Function<K, V> creator )
            {
                return schemaStateOperations.schemaStateGetOrCreate( state, key, creator );
            }
            @Override
            public <K> boolean schemaStateContains( StatementState state, K key )
            {
                return schemaStateOperations.schemaStateContains( state, key );
            }
        };
    }
}
