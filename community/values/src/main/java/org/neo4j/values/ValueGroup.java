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

public class ValueGroup
{
    private ValueGroup()
    {
    }

    interface VNumber
    {
        int compareTo( VInteger other );

        int compareTo( VFloatingPoint other );
    }

    interface VInteger extends VNumber
    {
        long longValue();
    }

    interface VFloatingPoint extends VNumber
    {
        double doubleValue();
    }

    interface VBoolean
    {
        boolean booleanValue();

        int compareTo( VBoolean other );
    }

    interface VText
    {
        String stringValue();

        int compareTo( VText other );
    }

    interface VNumberArray
    {
        int length();

        int compareTo( VIntegerArray other );

        int compareTo( VFloatingPointArray other );
    }

    interface VIntegerArray extends VNumberArray
    {
        long longValue( int offset );
    }

    interface VFloatingPointArray extends VNumberArray
    {
        double doubleValue( int offset );
    }

    interface VBooleanArray
    {
        int length();

        boolean booleanValue( int offset );

        int compareTo( VBooleanArray other );
    }

    interface VTextArray
    {
        int length();

        String stringValue( int offset );

        int compareTo( VTextArray other );
    }
}
