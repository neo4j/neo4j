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
package org.neo4j.kernel.api.schema;

import java.util.Arrays;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.lock.ResourceType;

public class RelationTypeSchemaDescriptor implements SchemaDescriptor
{
    private final int relTypeId;
    private final int[] propertyIds;

    RelationTypeSchemaDescriptor( int relTypeId, int... propertyIds )
    {
        this.relTypeId = relTypeId;
        this.propertyIds = propertyIds;
    }

    @Override
    public <R> R computeWith( SchemaComputer<R> processor )
    {
        return processor.computeSpecific( this );
    }

    @Override
    public void processWith( SchemaProcessor processor )
    {
        processor.processSpecific( this );
    }

    @Override
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return String.format( "-[:%s(%s)]-", tokenNameLookup.relationshipTypeGetName( relTypeId ),
                SchemaUtil.niceProperties( tokenNameLookup, propertyIds ) );
    }

    public int getRelTypeId()
    {
        return relTypeId;
    }

    @Override
    public int[] getPropertyIds()
    {
        return propertyIds;
    }

    @Override
    public int keyId()
    {
        return getRelTypeId();
    }

    @Override
    public ResourceType keyType()
    {
        return ResourceTypes.RELATIONSHIP_TYPE;
    }

    public int getPropertyId()
    {
        if ( propertyIds.length != 1 )
        {
            throw new IllegalStateException(
                    "Single property schema requires one property but had " + propertyIds.length );
        }
        return propertyIds[0];
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o != null && o instanceof RelationTypeSchemaDescriptor )
        {
            RelationTypeSchemaDescriptor that = (RelationTypeSchemaDescriptor)o;
            return relTypeId == that.getRelTypeId() && Arrays.equals( propertyIds, that.getPropertyIds() );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( propertyIds ) + 31 * relTypeId;
    }
}
