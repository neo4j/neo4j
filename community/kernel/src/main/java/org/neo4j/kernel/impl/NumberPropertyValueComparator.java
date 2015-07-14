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
package org.neo4j.kernel.impl;

import java.util.Comparator;

public class NumberPropertyValueComparator implements Comparator<Number>
{
    public final static NumberPropertyValueComparator INSTANCE = new NumberPropertyValueComparator();

    private NumberPropertyValueComparator()
    {
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compare( Number left, Number right )
    {
        if ( left.getClass() == right.getClass() )
        {
            return ((Comparable<Number>) left).compareTo( right );
        }
        else
        {
            if ((left instanceof Double) || (right instanceof Double))
            {
                return Double.compare( left.doubleValue(), right.doubleValue() );
            }
            else if ((left instanceof Float) || (right instanceof Float))
            {
                return Float.compare( left.floatValue(), right.floatValue() );
            }
            else
            {
                return Long.compare( left.longValue(), right.longValue() );
            }
        }
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals( Object obj )
    {
        return INSTANCE == obj;
    }
}
