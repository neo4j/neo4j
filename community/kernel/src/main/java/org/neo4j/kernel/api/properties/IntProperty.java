/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
final class IntProperty extends DefinedProperty
{
    private final int value;

    IntProperty( int propertyKeyId, int value )
    {
        super(propertyKeyId);
        this.value = value;
    }

    @Override
    @SuppressWarnings("UnnecessaryUnboxing")
    public boolean valueEquals( Object other )
    {
        if ( other instanceof Integer )
        {
            return value == ((Integer)other).intValue();
        }
        return valueCompare( value, other );
    }

    @Override
    boolean hasEqualValue( DefinedProperty that )
    {
        return value == ((IntProperty) that).value;
    }

    @Override
    public Integer value()
    {
        return value;
    }

    @Override
    int valueHash()
    {
        return value;
    }

    @Override
    public int intValue()
    {
        return value;
    }

    @Override
    public long longValue()
    {
        return value;
    }

    @Override
    public int sizeOfObjectInBytesIncludingOverhead()
    {
        return withObjectOverhead( 8 );
    }
}
