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

final class  StringProperty extends DefinedProperty implements DefinedProperty.WithStringValue
{
    private final String value;

    StringProperty( int propertyKeyId, String value )
    {
        super( propertyKeyId );
        assert value != null;
        this.value = value;
    }

    @Override
    public boolean valueEquals( Object other )
    {
        return valueEquals( value, other );
    }

    static boolean valueEquals( String value, Object other )
    {
        if ( other instanceof String )
        {
            return value.equals( other );
        }
        if ( other instanceof Character )
        {
            Character that = (Character) other;
            return value.length() == 1 && value.charAt( 0 ) == that;
        }
        return false;
    }

    @Override
    public String value()
    {
        return value;
    }

    @Override
    int valueHash()
    {
        return value.hashCode();
    }

    @Override
    boolean hasEqualValue( DefinedProperty other )
    {
        if ( other instanceof StringProperty )
        {
            StringProperty that = (StringProperty) other;
            return value.equals( that.value );
        }
        if ( other instanceof CharProperty )
        {
            CharProperty that = (CharProperty) other;
            return value.length() == 1 && that.value == value.charAt( 0 );
        }
        return false;
    }

    @Override
    public String stringValue()
    {
        return value;
    }
}
