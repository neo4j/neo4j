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

import java.util.Arrays;

class IntArrayProperty extends FullSizeProperty
{
    private final int[] value;

    IntArrayProperty( long propertyKeyId, int[] value )
    {
        super( propertyKeyId );
        assert value != null;
        this.value = value;
    }

    @Override
    public int[] value()
    {
        return value;
    }

    @Override
    public boolean valueEquals( Object value )
    {
        if ( value instanceof int[])
        {
            return Arrays.equals(this.value, (int[])value);
        }
        if ( value instanceof Integer[] )
        {
            Integer[] otherValue = (Integer[]) value;
            if ( this.value.length != otherValue.length )
            {
                return false;
            }
            for (int i = 0; i < otherValue.length; i++ )
            {
                if ( otherValue[i] == null || this.value[i] != otherValue[i] )
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    int valueHash()
    {
        return Arrays.hashCode( value );
    }

    @Override
    boolean hasEqualValue( FullSizeProperty that )
    {
        return Arrays.equals( this.value, ((IntArrayProperty)that).value );
    }
}
