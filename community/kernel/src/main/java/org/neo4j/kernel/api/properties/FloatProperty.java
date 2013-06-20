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

import static java.lang.Float.floatToIntBits;

final class FloatProperty extends NumberPropertyWithin4Bytes
{
    private final float value;
    private final long propertyKeyId;

    FloatProperty( long propertyKeyId, float value )
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
        if ( other instanceof Float )
        {
            boolean b = value == (float)other;
            return b;
        }

        return valueCompare( value, other );
    }

    @Override
    boolean hasEqualValue( NumberPropertyWithin4Bytes that )
    {
        return value == ((FloatProperty) that).value;
    }

    @Override
    int valueBits()
    {
        return floatToIntBits( value );
    }

    @Override
    public Number value()
    {
        return value;
    }

    @Override
    @Deprecated
    public PropertyData asPropertyDataJustForIntegration()
    {
        return PropertyDatas.forFloat( (int) propertyKeyId, -1, value );
    }
}
