/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.values;

import java.util.Arrays;
import java.util.concurrent.Callable;

public class LazyLongArray extends LazyIntegralArray<long[]>
{
    LazyLongArray( Callable<long[]> producer )
    {
        super( producer );
    }

    @Override
    public int hashCode()
    {
        return NumberValues.hash( getOrLoad() );
    }

    @Override
    public boolean equals( Object other )
    {
        return other != null && other instanceof Value && equals( (Value) other );
    }

    @Override
    boolean equals( Value other )
    {
        return other.equals( getOrLoad() );
    }

    @Override
    boolean equals( byte[] x )
    {
        return PrimitiveArrayValues.equals( x, getOrLoad() );
    }

    @Override
    boolean equals( short[] x )
    {
        return PrimitiveArrayValues.equals( x, getOrLoad() );
    }

    @Override
    boolean equals( int[] x )
    {
        return PrimitiveArrayValues.equals( x, getOrLoad() );
    }

    @Override
    boolean equals( long[] x )
    {
        return Arrays.equals( getOrLoad(), x );
    }

    @Override
    boolean equals( float[] x )
    {
        return PrimitiveArrayValues.equals( getOrLoad(), x );
    }

    @Override
    boolean equals( double[] x )
    {
        return PrimitiveArrayValues.equals( getOrLoad(), x );
    }

    @Override
    void writeTo( ValueWriter writer )
    {
        PrimitiveArrayWriting.writeTo( writer, getOrLoad() );
    }

    @Override
    public int length()
    {
        return getOrLoad().length;
    }

    @Override
    public long longValue( int offset )
    {
        return getOrLoad()[offset];
    }
}
