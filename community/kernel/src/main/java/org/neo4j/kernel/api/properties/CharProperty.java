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

import static org.neo4j.kernel.impl.cache.SizeOfs.withObjectOverhead;

/**
 * This does not extend AbstractProperty since the JVM can take advantage of the 4 byte initial field alignment if
 * we don't extend a class that has fields.
 */
final class CharProperty extends DefinedProperty
{
    private final char value;
    private final int propertyKeyId;

    CharProperty( int propertyKeyId, char value )
    {
        this.propertyKeyId = propertyKeyId;
        this.value = value;
    }

    @Override
    public int propertyKeyId()
    {
        return propertyKeyId;
    }

    @Override
    @SuppressWarnings("UnnecessaryUnboxing")
    public boolean valueEquals( Object other )
    {
        if ( other instanceof Character )
        {
            return value == ((Character)other).charValue();
        }
        return valueCompare( value, other );
    }

    @Override
    public Character value()
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
        if ( o instanceof CharProperty )
        {
            CharProperty that = (CharProperty) o;
            return propertyKeyId == that.propertyKeyId && value == that.value;
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        int result = value;
        result = 31 * result + propertyKeyId;
        return result;
    }

    @Override
    public int sizeOfObjectInBytesIncludingOverhead()
    {
        return withObjectOverhead( 8 );
    }
}
