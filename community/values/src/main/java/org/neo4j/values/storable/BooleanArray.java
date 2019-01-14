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

public class BooleanArray extends ArrayValue
{
    private final boolean[] value;

    BooleanArray( boolean[] value )
    {
        assert value != null;
        this.value = value;
    }

    @Override
    public int length()
    {
        return value.length;
    }

    public boolean booleanValue( int offset )
    {
        return value[offset];
    }

    @Override
    public String getTypeName()
    {
        return "BooleanArray";
    }

    @Override
    public boolean equals( Value other )
    {
        return other.equals( this.value );
    }

    @Override
    public boolean equals( boolean[] x )
    {
        return Arrays.equals( value, x );
    }

    @Override
    public int computeHash()
    {
        return NumberValues.hash( value );
    }

    @Override
    public long updateHash( HashFunction hashFunction, long hash )
    {
        hash = hashFunction.update( hash, value.length );
        hash = hashFunction.update( hash, hashCode() );
        return hash;
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapBooleanArray( this );
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        PrimitiveArrayWriting.writeTo( writer, value );
    }

    @Override
    public boolean[] asObjectCopy()
    {
        return value.clone();
    }

    @Override
    @Deprecated
    public boolean[] asObject()
    {
        return value;
    }

    @Override
    int unsafeCompareTo( Value otherValue )
    {
        return NumberValues.compareBooleanArrays( this, (BooleanArray) otherValue );
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.BOOLEAN_ARRAY;
    }

    @Override
    public NumberType numberType()
    {
        return NumberType.NO_NUMBER;
    }

    @Override
    public String prettyPrint()
    {
        return Arrays.toString( value );
    }

    @Override
    public AnyValue value( int position )
    {
        return Values.booleanValue( booleanValue( position ) );
    }

    @Override
    public String toString()
    {
        return format( "%s%s", getTypeName(), Arrays.toString( value ) );
    }
}
