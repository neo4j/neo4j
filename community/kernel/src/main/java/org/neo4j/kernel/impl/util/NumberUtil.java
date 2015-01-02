/**
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
package org.neo4j.kernel.impl.util;

public class NumberUtil
{
    private static final long LONG_SIGN_MASK = 0x80000000_00000000L;

    private NumberUtil()
    {
        throw new AssertionError( "Disallow instances" );
    }

    /**
     * @return whether or not {@code value1} and {@code value2} have the same {@link #signOf(long) sign}.
     */
    public static boolean haveSameSign( long value1, long value2 )
    {
        return signOf( value1 ) == signOf( value2 );
    }

    /**
     * @return the sign of a long value, {@code true} for positive, {@code false} for negative.
     */
    public static boolean signOf( long value )
    {
        return (value & LONG_SIGN_MASK) == 0;
    }
}
