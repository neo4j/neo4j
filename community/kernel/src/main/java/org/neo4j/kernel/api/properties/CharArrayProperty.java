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
package org.neo4j.kernel.api.properties;

import java.util.Arrays;

class CharArrayProperty extends DefinedProperty
{
    final char[] value;

    CharArrayProperty( int propertyKeyId, char[] value )
    {
        super( propertyKeyId );
        assert value != null;
        this.value = value;
    }

    @Override
    public char[] value()
    {
        return value.clone();
    }

    @Override
    public boolean valueEquals( Object other )
    {
        return valueEquals( this.value, other );
    }

    static boolean valueEquals( char[] value, Object other )
    {
        if ( other instanceof char[] )
        {
            return Arrays.equals( value, (char[]) other );
        }
        if ( other instanceof Character[] )
        {
            Character[] that = (Character[]) other;
            if ( value.length == that.length )
            {
                for ( int i = 0; i < that.length; i++ )
                {
                    Character character = that[i];
                    if ( character == null || character != value[i] )
                    {
                        return false;
                    }
                }
                return true;
            }
        }
        else if ( other instanceof String[] )
        {
            return eq( (String[]) other, value );
        }
        // else if ( other instanceof String ) // should we perhaps support this?
        return false;
    }

    @Override
    public int valueHash()
    {
        return hash( value );
    }

    static int hash( char[] value )
    {
        return Arrays.hashCode( value );
    }

    @Override
    public boolean hasEqualValue( DefinedProperty other )
    {
        if ( other instanceof CharArrayProperty )
        {
            CharArrayProperty that = (CharArrayProperty) other;
            return Arrays.equals( this.value, that.value );
        }
        if ( other instanceof StringArrayProperty )
        {
            StringArrayProperty that = (StringArrayProperty) other;
            return eq( that.value, this.value );
        }
        return false;
    }

    static boolean eq( String[] strings, char[] chars )
    {
        if ( strings.length == chars.length )
        {
            for ( int i = 0; i < strings.length; i++ )
            {
                String str = strings[i];
                if ( str == null || str.length() != 1 || str.charAt( 0 ) != chars[i] )
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
