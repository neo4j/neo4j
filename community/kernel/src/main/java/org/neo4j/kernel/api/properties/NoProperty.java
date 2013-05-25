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
package org.neo4j.kernel.api.properties;

import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;

import static java.lang.String.format;

final class NoProperty extends Property
{
    private final long propertyKeyId;
    private final EntityType entityType;
    private final long entityId;

    NoProperty( long propertyKeyId, EntityType entityType, long entityId )
    {
        this.propertyKeyId = propertyKeyId;
        this.entityType = entityType;
        this.entityId = entityId;
    }

    @Override
    public String toString()
    {
        return format( "%s[propertyKeyId=%s, %sId=%s]", getClass().getSimpleName(),
                propertyKeyId, entityType.name().toLowerCase(), entityId );
    }

    @Override
    public boolean equals( Object o )
    {
        return this == o || o instanceof NoProperty && propertyKeyId == ((NoProperty) o).propertyKeyId;
    }

    @Override
    public boolean isNoProperty()
    {
        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (propertyKeyId ^ (propertyKeyId >>> 32));
    }

    @Override
    public long propertyKeyId()
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
    public String stringValue( String defaultValue )
    {
        return defaultValue;
    }

    @Override
    public Number numberValue( Number defaultValue )
    {
        return defaultValue;
    }

    @Override
    public int intValue( int defaultValue )
    {
        return defaultValue;
    }

    @Override
    public long longValue( long defaultValue )
    {
        return defaultValue;
    }

    @Override
    public boolean booleanValue( boolean defaultValue )
    {
        return defaultValue;
    }

    @Override
    public Object value() throws PropertyNotFoundException
    {
        throw new PropertyNotFoundException( propertyKeyId );
    }

    @Override
    public String stringValue() throws PropertyNotFoundException
    {
        throw new PropertyNotFoundException( propertyKeyId );
    }

    @Override
    public boolean booleanValue() throws PropertyNotFoundException
    {
        throw new PropertyNotFoundException( propertyKeyId );
    }

    @Override
    public Number numberValue() throws PropertyNotFoundException
    {
        throw new PropertyNotFoundException( propertyKeyId );
    }

    @Override
    public int intValue() throws PropertyNotFoundException
    {
        throw new PropertyNotFoundException( propertyKeyId );
    }

    @Override
    public long longValue() throws PropertyNotFoundException
    {
        throw new PropertyNotFoundException( propertyKeyId );
    }
}
