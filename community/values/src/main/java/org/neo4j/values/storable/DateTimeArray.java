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

import java.time.ZonedDateTime;
import java.util.Arrays;

import org.neo4j.values.ValueMapper;

public class DateTimeArray extends TemporalArray<ZonedDateTime,DateTimeValue>
{
    private final ZonedDateTime[] value;

    DateTimeArray( ZonedDateTime[] value )
    {
        assert value != null;
        this.value = value;
    }

    @Override
    protected ZonedDateTime[] value()
    {
        return value;
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapDateTimeArray( this );
    }

    @Override
    public boolean equals( Value other )
    {
        return other.equals( value );
    }

    @Override
    public boolean equals( ZonedDateTime[] x )
    {
        return Arrays.equals( value, x );
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writeTo( writer, ValueWriter.ArrayType.ZONED_DATE_TIME, value );
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.ZONED_DATE_TIME_ARRAY;
    }

    @Override
    int unsafeCompareTo( Value otherValue )
    {
        return compareToNonPrimitiveArray( (DateTimeArray) otherValue );
    }

    @Override
    public String getTypeName()
    {
        return "DateTimeArray";
    }
}
