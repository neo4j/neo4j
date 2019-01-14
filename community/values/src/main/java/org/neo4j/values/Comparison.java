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
package org.neo4j.values;

/**
 * Defines the result of a ternary comparison.
 * <p>
 * In a ternary comparison the result may not only be greater than, equal or smaller than but the
 * result can also be undefined.
 */
public enum Comparison
{
    GREATER_THAN
            {
                @Override
                public int value()
                {
                    return 1;
                }
            },
    EQUAL
            {
                @Override
                public int value()
                {
                    return 0;
                }
            },
    SMALLER_THAN
            {
                @Override
                public int value()
                {
                    return -1;
                }
            },
    GREATER_THAN_AND_EQUAL,
    SMALLER_THAN_AND_EQUAL,
    UNDEFINED;

    /**
     * Integer representation of comparison
     * <p>
     * Returns a positive integer if {@link Comparison#GREATER_THAN} than, negative integer for
     * {@link Comparison#SMALLER_THAN},
     * and zero for {@link Comparison#EQUAL}
     *
     * @return a positive number if result is greater than, a negative number if the result is smaller than or zero
     * if equal.
     * @throws IllegalStateException if the result is undefined.
     */
    public int value()
    {
        throw new IllegalStateException( "This value is undefined and can't handle primitive comparisons" );
    }

    /**
     * Maps an integer value to comparison result.
     *
     * @param i the integer to be mapped to a Comparison
     * @return {@link Comparison#GREATER_THAN} than if positive, {@link Comparison#SMALLER_THAN} if negative or
     * {@link Comparison#EQUAL} if zero
     */
    public static Comparison from( int i )
    {
        if ( i > 0 )
        {
            return GREATER_THAN;
        }
        else if ( i < 0 )
        {
            return SMALLER_THAN;
        }
        else
        {
            return EQUAL;
        }
    }
}
