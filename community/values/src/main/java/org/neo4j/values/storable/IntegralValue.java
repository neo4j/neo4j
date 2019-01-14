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
import org.neo4j.values.AnyValue;
import org.neo4j.values.utils.ValueMath;

public abstract class IntegralValue extends NumberValue
{
    public static long safeCastIntegral( String name, AnyValue value, long defaultValue )
    {
        if ( value == null || value == Values.NO_VALUE )
        {
            return defaultValue;
        }
        if ( value instanceof IntegralValue )
        {
            return ((IntegralValue) value).longValue();
        }
        throw new IllegalArgumentException(
                name + " must be an integer value, but was a " + value.getClass().getSimpleName() );
    }

    @Override
    public boolean equals( long x )
    {
        return longValue() == x;
    }

    @Override
    public boolean equals( double x )
    {
        return NumberValues.numbersEqual( x, longValue() );
    }

    @Override
    public final int computeHash()
    {
        return NumberValues.hash( longValue() );
    }

    @Override
    public long updateHash( HashFunction hashFunction, long hash )
    {
        return hashFunction.update( hash, longValue() );
    }

    @Override
    public boolean eq( Object other )
    {
        return other instanceof Value && equals( (Value) other );
    }

    @Override
    public final boolean equals( Value other )
    {
        if ( other instanceof IntegralValue )
        {
            IntegralValue that = (IntegralValue) other;
            return this.longValue() == that.longValue();
        }
        else if ( other instanceof FloatingPointValue )
        {
            FloatingPointValue that = (FloatingPointValue) other;
            return NumberValues.numbersEqual( that.doubleValue(), this.longValue() );
        }
        else
        {
            return false;
        }
    }

    @Override
    public int compareTo( IntegralValue other )
    {
        return Long.compare( longValue(), other.longValue() );
    }

    @Override
    public int compareTo( FloatingPointValue other )
    {
        return NumberValues.compareLongAgainstDouble( longValue(), other.doubleValue() );
    }

    @Override
    public NumberType numberType()
    {
        return NumberType.INTEGRAL;
    }

    @Override
    public double doubleValue()
    {
        return longValue();
    }

    @Override
    public LongValue minus( long b )
    {
        return ValueMath.subtract( longValue(), b );
    }

    @Override
    public DoubleValue minus( double b )
    {
        return ValueMath.subtract( longValue(), b );
    }

    @Override
    public LongValue plus( long b )
    {
        return ValueMath.add( longValue(), b );
    }

    @Override
    public DoubleValue plus( double b )
    {
        return ValueMath.add( longValue(), b );
    }

    @Override
    public LongValue times( long b )
    {
        return ValueMath.multiply( longValue(), b );
    }

    @Override
    public DoubleValue times( double b )
    {
        return ValueMath.multiply( longValue(), b );
    }

    @Override
    public LongValue dividedBy( long b )
    {
        return Values.longValue( longValue() / b );
    }

    @Override
    public DoubleValue dividedBy( double b )
    {
        return Values.doubleValue( doubleValue() / b );
    }
}
