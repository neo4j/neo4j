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

class StringArrayProperty extends DefinedProperty
{
    final String[] value;

    StringArrayProperty( int propertyKeyId, String[] value )
    {
        super( propertyKeyId );
        assert value != null;
        this.value = value;
    }

    @Override
    public String[] value()
    {
        return value.clone();
    }

    @Override
    public boolean valueEquals( Object other )
    {
        String[] value = this.value;
        return valueEquals( value, other );
    }

    static boolean valueEquals( String[] value, Object other )
    {
        if ( other instanceof String[] )
        {
            return Arrays.equals( value, (String[]) other );
        }
        if ( other instanceof char[] )
        {
            return CharArrayProperty.eq( value, (char[]) other );
        }
        else if ( other instanceof Character[] )
        {
            Character[] that = (Character[]) other;
            if ( value.length == that.length )
            {
                for ( int i = 0; i < that.length; i++ )
                {
                    String str = value[i];
                    Character character = that[i];
                    if ( character == null || str.length() != 1 || str.charAt( 0 ) != character )
                    {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }
        return false;
    }

    @Override
    int valueHash()
    {
        return hash( value );
    }

    static int hash( String[] value )
    {
        return Arrays.hashCode( value );
    }

    @Override
    boolean hasEqualValue( DefinedProperty other )
    {
        if ( other instanceof StringArrayProperty )
        {
            StringArrayProperty that = (StringArrayProperty) other;
            return Arrays.equals( this.value, that.value );
        }
        if ( other instanceof CharArrayProperty )
        {
            CharArrayProperty that = (CharArrayProperty) other;
            return CharArrayProperty.eq( this.value, that.value );
        }
        return false;
    }
}
