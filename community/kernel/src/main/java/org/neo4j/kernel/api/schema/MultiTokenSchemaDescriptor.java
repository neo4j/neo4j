/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.api.schema;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.Objects;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.schema.SchemaComputer;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaProcessor;
import org.neo4j.internal.kernel.api.schema.SchemaUtil;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.lock.ResourceType;

import static java.util.stream.Collectors.joining;
import static org.neo4j.internal.kernel.api.schema.SchemaDescriptor.isAnyEntityTokenSchema;

public class MultiTokenSchemaDescriptor implements SchemaDescriptor
{
    private final int[] entityTokens;
    private final EntityType entityType;
    private final int[] propertyIds;

    MultiTokenSchemaDescriptor( int[] entityTokens, EntityType entityType, int[] propertyIds )
    {
        this.entityTokens = entityTokens;
        Arrays.sort( this.entityTokens );
        this.entityType = entityType;
        this.propertyIds = propertyIds;
    }

    @Override
    public boolean isAffected( long[] entityTokenIds )
    {
        if ( isAnyEntityTokenSchema( this ) )
        {
            return true;
        }
        for ( int id : entityTokens )
        {
            if ( ArrayUtils.contains( entityTokenIds, id ) )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public <R> R computeWith( SchemaComputer<R> computer )
    {
        throw new UnsupportedOperationException( "Not an valid operation for multitoken schema" );
    }

    @Override
    public void processWith( SchemaProcessor processor )
    {
        throw new UnsupportedOperationException( "Not an valid operation for multitoken schema" );
    }

    @Override
    public String toString()
    {
        return "MultiTokenSchemaDescriptor[" + userDescription( SchemaUtil.idTokenNameLookup ) + "]";
    }

    @Override
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return String.format( entityType + ":%s(%s)",
                Arrays.stream( tokenNameLookup.entityTokensGetNames( entityType, entityTokens ) )
                      .collect( joining( ", " ) ),
                SchemaUtil.niceProperties( tokenNameLookup, propertyIds ) );
    }

    @Override
    public int[] getPropertyIds()
    {
        return propertyIds;
    }

    @Override
    public int[] getEntityTokenIds()
    {
        return entityTokens;
    }

    @Override
    public int keyId()
    {
        return hashCode();
    }

    @Override
    public ResourceType keyType()
    {
        return ResourceTypes.SCHEMA;
    }

    @Override
    public EntityType entityType()
    {
        return entityType;
    }

    @Override
    public SchemaDescriptor.PropertySchemaType propertySchemaType()
    {
        return PropertySchemaType.PARTIAL_ANY_TOKEN;
    }

    @Override
    public SchemaDescriptor schema()
    {
        return this;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        MultiTokenSchemaDescriptor that = (MultiTokenSchemaDescriptor) o;
        return Arrays.equals( entityTokens, that.entityTokens ) && entityType == that.entityType &&
               Arrays.equals( propertyIds, that.propertyIds );
    }

    @Override
    public int hashCode()
    {

        int result = Objects.hash( entityType );
        result = 31 * result + Arrays.hashCode( entityTokens );
        result = 31 * result + Arrays.hashCode( propertyIds );
        return result;
    }
}
