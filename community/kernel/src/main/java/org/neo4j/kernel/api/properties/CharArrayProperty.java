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

import java.util.Arrays;

import static org.neo4j.kernel.impl.cache.SizeOfs.sizeOfArray;
import static org.neo4j.kernel.impl.cache.SizeOfs.withObjectOverhead;
import static org.neo4j.kernel.impl.cache.SizeOfs.withReference;

class CharArrayProperty extends DefinedProperty
{
    private final char[] value;

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
    public boolean valueEquals( Object value )
    {
        if ( value instanceof char[] )
        {
            return Arrays.equals( this.value, (char[]) value );
        }
        return valueCompare( this.value, value );
    }

    @Override
    int valueHash()
    {
        return Arrays.hashCode( value );
    }

    @Override
    boolean hasEqualValue( DefinedProperty that )
    {
        return Arrays.equals( this.value, ((CharArrayProperty)that).value );
    }

    @Override
    public int sizeOfObjectInBytesIncludingOverhead()
    {
        return withObjectOverhead( withReference( sizeOfArray( value ) ) );
    }
}
