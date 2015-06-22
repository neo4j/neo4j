/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.helpers;

public abstract class MathUtil
{
    private static final long NON_DOUBLE_LONG = 0xFFE0_0000_0000_0000L; // doubles are exact integers up to 53 bits

    public static boolean numbersEqual( double fpn, long in )
    {
        if ( in < 0 )
        {
            if ( fpn < 0.0 )
            {
                if ( (NON_DOUBLE_LONG & in) == NON_DOUBLE_LONG ) // the high order bits are only sign bits
                { // no loss of precision if converting the long to a double, so it's safe to compare as double
                    return fpn == in;
                }
                else if ( fpn < Long.MIN_VALUE )
                { // the double is too big to fit in a long, they cannot be equal
                    return false;
                }
                else if ( (fpn == Math.floor( fpn )) && !Double.isInfinite( fpn ) ) // no decimals
                { // safe to compare as long
                    return in == (long) fpn;
                }
            }
        }
        else
        {
            if ( !(fpn < 0.0) )
            {
                if ( (NON_DOUBLE_LONG & in) == 0 ) // the high order bits are only sign bits
                { // no loss of precision if converting the long to a double, so it's safe to compare as double
                    return fpn == in;
                }
                else if ( fpn > Long.MAX_VALUE )
                { // the double is too big to fit in a long, they cannot be equal
                    return false;
                }
                else if ( (fpn == Math.floor( fpn )) && !Double.isInfinite( fpn ) )  // no decimals
                { // safe to compare as long
                    return in == (long) fpn;
                }
            }
        }
        return false;
    }

}
