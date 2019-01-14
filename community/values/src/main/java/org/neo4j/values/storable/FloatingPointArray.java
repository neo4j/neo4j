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

import org.neo4j.hashing.HashFunction;

public abstract class FloatingPointArray extends NumberArray
{
    public abstract double doubleValue( int offset );

    @Override
    public int compareTo( IntegralArray other )
    {
        return -NumberValues.compareIntegerVsFloatArrays( other, this );
    }

    @Override
    public int compareTo( FloatingPointArray other )
    {
        return NumberValues.compareFloatArrays( this, other );
    }

    @Override
    public NumberType numberType()
    {
        return NumberType.FLOATING_POINT;
    }

    @Override
    public long updateHash( HashFunction hashFunction, long hash )
    {
        int len = length();
        hash = hashFunction.update( hash, len );
        for ( int i = 0; i < len; i++ )
        {
            hash = hashFunction.update( hash, Double.doubleToLongBits( doubleValue( i ) ) );
        }
        return hash;
    }
}
