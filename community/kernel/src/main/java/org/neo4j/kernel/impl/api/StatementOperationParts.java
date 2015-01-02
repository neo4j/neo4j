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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.api.operations.KeyWriteOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaStateOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;

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

    private <T> T eitherOr( T first, T other, @SuppressWarnings("UnusedParameters"/*used as type flag*/) Class<T> cls )
    {
        return first != null ? first : other;
    }
}
