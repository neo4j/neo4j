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

import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;

public class DurationArray extends NonPrimitiveArray<DurationValue>
{
    private final DurationValue[] value;

    DurationArray( DurationValue[] value )
    {
        assert value != null;
        this.value = value;
    }

    @Override
    protected DurationValue[] value()
    {
        return value;
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapDurationArray( this );
    }

    @Override
    public boolean equals( Value other )
    {
        return other.equals( value );
    }

    @Override
    public boolean equals( DurationValue[] x )
    {
        return Arrays.equals( value, x);
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.beginArray( value.length, ValueWriter.ArrayType.DURATION );
        for ( DurationValue x : value )
        {
            x.writeTo( writer );
        }
        writer.endArray();
    }

    @Override
    public AnyValue value( int offset )
    {
        return Values.durationValue( value[offset] );
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.DURATION_ARRAY;
    }

    @Override
    int unsafeCompareTo( Value otherValue )
    {
        return compareToNonPrimitiveArray( (DurationArray) otherValue );
    }

    @Override
    public String getTypeName()
    {
        return "DurationArray";
    }
}
