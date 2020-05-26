/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.sizeOfObjectArray;

public final class PointArray extends NonPrimitiveArray<PointValue>
{
    private static final long SHALLOW_SIZE = shallowSizeOfInstance( PointArray.class );

    private final PointValue[] value;

    PointArray( PointValue[] value )
    {
        assert value != null;
        this.value = value;
    }

    @Override
    protected PointValue[] value()
    {
        return value;
    }

    public PointValue pointValue( int offset )
    {
        return value()[offset];
    }

    @Override
    public boolean equals( Geometry[] x )
    {
        return Arrays.equals( value(), x );
    }

    @Override
    public boolean equals( Value other )
    {
        return other instanceof PointArray && Arrays.equals( this.value(), ((PointArray) other).value() );
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.GEOMETRY_ARRAY;
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapPointArray( this );
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        PrimitiveArrayWriting.writeTo( writer, value() );
    }

    @Override
    public AnyValue value( int offset )
    {
        return Values.point( value[offset] );
    }

    @Override
    int unsafeCompareTo( Value otherValue )
    {
        return compareToNonPrimitiveArray( (PointArray) otherValue );
    }

    @Override
    public String getTypeName()
    {
        return "PointArray";
    }

    @Override
    public long estimatedHeapUsage()
    {
        int length = value.length;
        return SHALLOW_SIZE + (length == 0 ? 0 : sizeOfObjectArray( value[0].estimatedHeapUsage(), length ));
    }
}
