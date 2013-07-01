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

import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.PropertyDatas;

/**
 * This does not extend AbstractProperty since the JVM can take advantage of the 4 byte initial field alignment if
 * we don't extend a class that has fields.
 */
final class BooleanProperty extends PropertyWithValue
{
    private final boolean value;
    private final long propertyKeyId;

    BooleanProperty( long propertyKeyId, boolean value )
    {
        this.propertyKeyId = propertyKeyId;
        this.value = value;
    }

    @Override
    public long propertyKeyId()
    {
        return propertyKeyId;
    }

    @Override
    public boolean valueEquals( Object other )
    {
        return other instanceof Boolean && (Boolean) value == other;
    }

    @Override
    public Boolean value()
    {
        return value;
    }

    @Override
    public boolean booleanValue()
    {
        return value;
    }

    @Override
    public boolean booleanValue( boolean defaultValue )
    {
        return value;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o instanceof BooleanProperty )
        {
            BooleanProperty that = (BooleanProperty) o;
            return propertyKeyId == that.propertyKeyId && value == that.value;
        }
        return false;
    }

    @Override
    public boolean isNoProperty()
    {
        return false;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (propertyKeyId ^ (propertyKeyId >>> 32));
        return value ? result : -result;
    }

    @Override
    @Deprecated
    public PropertyData asPropertyDataJustForIntegration()
    {
        return PropertyDatas.forBoolean( (int) propertyKeyId, -1, value );
    }
}
