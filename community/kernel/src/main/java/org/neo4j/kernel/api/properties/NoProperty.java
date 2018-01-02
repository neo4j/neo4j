/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.properties;

import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;

final class NoProperty extends Property
{
    private final EntityType entityType;
    private final long entityId;

    NoProperty( int propertyKeyId, EntityType entityType, long entityId )
    {
        super( propertyKeyId );
        this.entityType = entityType;
        this.entityId = entityId;
    }

    @Override
    public String toString()
    {
        StringBuilder string = new StringBuilder( getClass().getSimpleName() );
        string.append( "[" ).append( entityType.name().toLowerCase() );
        if ( entityType == EntityType.GRAPH )
        {
            string.append( "Property" );
        }
        else
        {
            string.append( "Id=" ).append( entityId );
        }
        string.append( ", propertyKeyId=" ).append( propertyKeyId );
        return string.append( "]" ).toString();
    }

    @Override
    public boolean equals( Object o )
    {
        return this == o || o instanceof NoProperty && propertyKeyId == ((NoProperty) o).propertyKeyId;
    }

    @Override
    public boolean isDefined()
    {
        return false;
    }

    @Override
    public int hashCode()
    {
        return propertyKeyId;
    }

    @Override
    public boolean valueEquals( Object value )
    {
        return false;
    }

    @Override
    public Object value( Object defaultValue )
    {
        return defaultValue;
    }

    @Override
    public String valueAsString() throws PropertyNotFoundException
    {
        throw new PropertyNotFoundException( propertyKeyId, entityType, entityId );
    }

    @Override
    public Object value() throws PropertyNotFoundException
    {
        throw new PropertyNotFoundException( propertyKeyId, entityType, entityId );
    }
}
