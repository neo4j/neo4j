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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.NumberValues;
import org.neo4j.values.storable.Values;

/**
 * Useful to compare values stored as raw bits and value type without having to box them as {@link NumberValue number values}.
 */
class RawBits
{
    static final byte BYTE = 0;
    static final byte SHORT = 1;
    static final byte INT = 2;
    static final byte LONG = 3;
    static final byte FLOAT = 4;
    static final byte DOUBLE = 5;

    /**
     * Convert value represented by type and raw bits to corresponding {@link NumberValue}. If type is not {@link #BYTE}, {@link #SHORT},
     * {@link #INT}, {@link #LONG}, {@link #FLOAT} or {@link #DOUBLE}, the raw bits will be interpreted as a long.
     *
     * @param rawBits Raw bits of value
     * @param type Type of value
     * @return {@link NumberValue} with type and value given by provided raw bits and type.
     */
    static NumberValue asNumberValue( long rawBits, byte type )
    {
        switch ( type )
        {
        case BYTE:
            return Values.byteValue( (byte) rawBits );
        case SHORT:
            return Values.shortValue( (short) rawBits );
        case INT:
            return Values.intValue( (int) rawBits );
        case LONG:
            return Values.longValue( rawBits );
        case FLOAT:
            return Values.floatValue( Float.intBitsToFloat( (int) rawBits ) );
        case DOUBLE:
            return Values.doubleValue( Double.longBitsToDouble( rawBits ) );
        default:
            // If type is not recognized, interpret as long.
            return Values.longValue( rawBits );
        }
    }

    /**
     * Compare number values represented by type and raw bits. If type is not {@link #BYTE}, {@link #SHORT}, {@link #INT}, {@link #LONG},
     * {@link #FLOAT} or {@link #DOUBLE}, the raw bits will be compared as long.
     *
     * @param lhsRawBits Raw bits of left hand side value
     * @param lhsType Type of left hand side value
     * @param rhsRawBits Raw bits of right hand side value
     * @param rhsType Type of right hand side value
     * @return An int less that 0 if lhs value is numerically less than rhs value. An int equal to 0 if lhs and rhs value are
     * numerically equal (independent of type) and an int greater than 0 if lhs value is greater than rhs value.
     */
    static int compare( long lhsRawBits, byte lhsType, long rhsRawBits, byte rhsType )
    {
        // case integral - integral
        if ( lhsType == BYTE ||
                lhsType == SHORT ||
                lhsType == INT ||
                lhsType == LONG )
        {
            return compareLongAgainstRawType( lhsRawBits, rhsRawBits, rhsType );
        }
        else if ( lhsType == FLOAT )
        {
            double lhsFloat = Float.intBitsToFloat( (int) lhsRawBits );
            return compareDoubleAgainstRawType( lhsFloat, rhsRawBits, rhsType );
        }
        else if ( lhsType == DOUBLE )
        {
            double lhsDouble = Double.longBitsToDouble( lhsRawBits );
            return compareDoubleAgainstRawType( lhsDouble, rhsRawBits, rhsType );
        }
        // We can not throw here because we will visit this method inside a pageCursor.shouldRetry() block.
        // Just return a comparison that at least will be commutative.
        return Long.compare( lhsRawBits, rhsRawBits );
    }

    private static int compareLongAgainstRawType( long lhs, long rhsRawBits, byte rhsType )
    {
        if ( rhsType == BYTE ||
                rhsType == SHORT ||
                rhsType == INT ||
                rhsType == LONG )
        {
            return Long.compare( lhs, rhsRawBits );
        }
        else if ( rhsType == FLOAT )
        {
            return NumberValues.compareLongAgainstDouble( lhs, Float.intBitsToFloat( (int) rhsRawBits ) );
        }
        else if ( rhsType == DOUBLE )
        {
            return NumberValues.compareLongAgainstDouble( lhs, Double.longBitsToDouble( rhsRawBits ) );
        }
        // We can not throw here because we will visit this method inside a pageCursor.shouldRetry() block.
        // Just return a comparison that at least will be commutative.
        return Long.compare( lhs, rhsRawBits );
    }

    private static int compareDoubleAgainstRawType( double lhsDouble, long rhsRawBits, byte rhsType )
    {
        if ( rhsType == BYTE ||
                rhsType == SHORT ||
                rhsType == INT ||
                rhsType == LONG )
        {
            return NumberValues.compareDoubleAgainstLong( lhsDouble, rhsRawBits );
        }
        else if ( rhsType == FLOAT )
        {
            return Double.compare( lhsDouble, Float.intBitsToFloat( (int) rhsRawBits ) );
        }
        else if ( rhsType == DOUBLE )
        {
            return Double.compare( lhsDouble, Double.longBitsToDouble( rhsRawBits ) );
        }
        // We can not throw here because we will visit this method inside a pageCursor.shouldRetry() block.
        // Just return a comparison that at least will be commutative.
        return Long.compare( Double.doubleToLongBits( lhsDouble ), rhsRawBits );
    }
}
