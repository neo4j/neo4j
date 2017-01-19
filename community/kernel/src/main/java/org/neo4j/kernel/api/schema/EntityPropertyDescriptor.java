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

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static java.lang.String.format;

/**
 * Description of an object that contains one entity defined by an int, and one or more properties defined by an int
 * or an int array
 *
 * @see SchemaRule
 */
public abstract class EntityPropertyDescriptor implements Comparable<EntityPropertyDescriptor>
{
    protected final int entityId;
    private final int propertyKeyId;

    public EntityPropertyDescriptor( int entityId, int propertyKeyId )
    {
        this.entityId = entityId;
        this.propertyKeyId = propertyKeyId;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj != null && obj instanceof EntityPropertyDescriptor )
        {
            EntityPropertyDescriptor that = (EntityPropertyDescriptor) obj;
            return this.entityId == that.entityId &&
                    this.propertyKeyId == that.propertyKeyId;
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        int result = entityId;
        result = 31 * result + propertyKeyId;
        return result;
    }

    /**
     * @return label token id this index is for.
     */
    public int getEntityId()
    {
        return entityId;
    }

    /**
     * @return property key token id this index is for.
     */
    public int getPropertyKeyId()
    {
        return propertyKeyId;
    }

    /**
     * @return property key token ids this descriptor is for.
     */
    public int[] getPropertyKeyIds()
    {
        throw new UnsupportedOperationException( "Cannot get multiple property Ids of single property descriptor" );
    }

    @Override
    public String toString()
    {
        return format( ":%s[%d](property[%d])", entityType().getLabelingType(), entityId, propertyKeyId );
    }

    public String propertyIdText()
    {
        return Integer.toString( propertyKeyId );
    }

    public abstract EntityType entityType();

    public abstract String entityNameText( TokenNameLookup tokenNameLookup );

    public String propertyNameText( TokenNameLookup tokenNameLookup )
    {
        return tokenNameLookup.propertyKeyGetName( propertyKeyId );
    }

    /**
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user friendly description of what this index indexes.
     */
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return format( ":%s(%s)", entityNameText( tokenNameLookup ), propertyNameText( tokenNameLookup ) );
    }

    public int compareTo( EntityPropertyDescriptor that )
    {
        int cmp = this.entityId - that.entityId;
        if ( cmp == 0 )
        {
            return this.propertyKeyId - that.propertyKeyId;
        }
        return cmp;
    }
}
