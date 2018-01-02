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

/**
 * This does not extend AbstractProperty since the JVM can take advantage of the 4 byte initial field alignment if
 * we don't extend a class that has fields.
 */
final class CharProperty extends DefinedProperty implements DefinedProperty.WithStringValue
{
    final char value;

    CharProperty( int propertyKeyId, char value )
    {
        super( propertyKeyId );
        this.value = value;
    }

    @Override
    @SuppressWarnings("UnnecessaryUnboxing")
    public boolean valueEquals( Object other )
    {
        if ( other instanceof Character )
        {
            return value == ((Character) other).charValue();
        }
        if ( other instanceof String )
        {
            String that = (String) other;
            return that.length() == 1 && that.charAt( 0 ) == value;
        }
        return false;
    }

    @Override
    public Character value()
    {
        return value;
    }

    @Override
    int valueHash()
    {
        return value;
    }

    @Override
    boolean hasEqualValue( DefinedProperty that )
    {
        if ( that instanceof CharProperty )
        {
            return value == ((CharProperty) that).value;
        }
        else if ( that instanceof StringProperty )
        {
            String str = ((StringProperty) that).value();
            return str.length() == 1 && value == str.charAt( 0 );
        }
        else
        {
            return false;
        }
    }

    @Override
    public String stringValue()
    {
        return Character.toString( value );
    }
}
