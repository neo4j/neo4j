/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.Comparator;

/**
 * @author Anton Persson
 */
public class NoneStrictMath
{
    public static double EPSILON = 1.0E-8;

    /**
     * Compares two numbers given some amount of allowed error.
     */
    public static int compare( double x, double y, double eps )
    {
        return equals( x, y, eps ) ? 0 : x < y ? -1 : 1;
    }

    /**
     * Compares two numbers given some amount of allowed error.
     * Error given by {@link NoneStrictMath#EPSILON}
     */
    public static int compare( double x, double y )
    {
        return compare( x, y, EPSILON );
    }

    /**
     * Returns true if both arguments are equal or within the range of allowed error (inclusive)
     */
    public static boolean equals( double x, double y, double eps )
    {
        return Math.abs( x - y ) <= eps;
    }

    /**
     * Returns true if both arguments are equal or within the range of allowed error (inclusive)
     * Error given by {@link NoneStrictMath#EPSILON}
     */
    public static boolean equals( double x, double y )
    {
        return equals( x, y, EPSILON );
    }

    public static class CommonToleranceComparator implements Comparator<Double>
    {
        private final double epsilon;

        public CommonToleranceComparator( double epsilon )
        {
            this.epsilon = epsilon;
        }

        @Override
        public int compare( Double x, Double y )
        {
            return NoneStrictMath.compare( x, y, epsilon );
        }
    }
}
