/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.values.storable;

import java.util.Arrays;

import org.neo4j.hashing.HashFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;

import static java.lang.String.format;

public class StringArray extends TextArray
{
    private final String[] value;

    StringArray( String[] value )
    {
        assert value != null;
        this.value = value;
    }

    @Override
    public int length()
    {
        return value.length;
    }

    @Override
    public String stringValue( int offset )
    {
        return value[offset];
    }

    @Override
    public boolean equals( Value other )
    {
        return other.equals( value );
    }

    @Override
    public boolean equals( char[] x )
    {
        return PrimitiveArrayValues.equals( x, value );
    }

    @Override
    public boolean equals( String[] x )
    {
        return Arrays.equals( value, x );
    }

    @Override
    public int computeHash()
    {
        return Arrays.hashCode( value );
    }

    @Override
    public long updateHash( HashFunction hashFunction, long hash )
    {
        hash = hashFunction.update( hash, value.length );
        for ( String s : value )
        {
            hash = StringWrappingStringValue.updateHash( hashFunction, hash, s );
        }
        return hash;
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        PrimitiveArrayWriting.writeTo( writer, value );
    }

    @Override
    public String[] asObjectCopy()
    {
        return value.clone();
    }

    @Override
    @Deprecated
    public String[] asObject()
    {
        return value;
    }

    @Override
    public String prettyPrint()
    {
        return Arrays.toString( value );
    }

    @Override
    public AnyValue value( int offset )
    {
        return Values.stringOrNoValue( stringValue( offset ) );
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapStringArray( this );
    }

    @Override
    public String toString()
    {
        return format( "%s%s", getTypeName(), Arrays.toString( value ) );
    }

    @Override
    public String getTypeName()
    {
        return "StringArray";
    }
}
