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

abstract class IntegralValue extends NumberValue
{
    abstract long longValue();

    @Override
    public final int hashCode()
    {
        return NumberValues.hash( longValue() );
    }

    @Override
    public boolean equals( Object other )
    {
        return other != null && other instanceof Value && equals( (Value) other );
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

    public int compareTo( IntegralValue other )
    {
        return Long.compare( longValue(), other.longValue() );
    }

    public int compareTo( FloatingPointValue other )
    {
        return NumberValues.compareLongAgainstDouble( longValue(), other.doubleValue() );
    }

    @Override
    public NumberType numberType()
    {
        return NumberType.INTEGRAL;
    }
}
