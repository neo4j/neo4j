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

import java.util.Arrays;

class BooleanArrayProperty extends DefinedProperty
{
    private final boolean[] value;

    BooleanArrayProperty( int propertyKeyId, boolean[] value )
    {
        super( propertyKeyId );
        assert value != null;
        this.value = value;
    }

    @Override
    public boolean[] value()
    {
        return value.clone();
    }

    @Override
    public boolean valueEquals( Object other )
    {
        return valueEquals( this.value, other );
    }

    static boolean valueEquals( boolean[] value, Object other )
    {
        if ( other instanceof boolean[] )
        {
            return Arrays.equals( value, (boolean[]) other );
        }
        if ( other instanceof Boolean[] )
        {
            Boolean[] that = (Boolean[]) other;
            int length = value.length;
            if ( length == that.length )
            {
                for ( int i = 0; i < length; i++ )
                {
                    Boolean bool = that[i];
                    if ( bool == null || bool != value[i] )
                    {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    int valueHash()
    {
        return hash( value );
    }

    static int hash( boolean[] value )
    {
        return Arrays.hashCode( value );
    }

    @Override
    boolean hasEqualValue( DefinedProperty that )
    {
        return that instanceof BooleanArrayProperty && Arrays.equals( this.value, ((BooleanArrayProperty) that).value );
    }
}
