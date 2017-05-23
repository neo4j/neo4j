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

    enum Id
    {
        NO_VALUE( -1 ),
        TEXT( 0 ),
        BOOLEAN( 1 ),
        INTEGER( 2 ),
        FLOAT( 2 ),
        INTEGER_ARRAY( 3 ),
        FLOAT_ARRAY( 3 ),
        TEXT_ARRAY( 4 ),
        BOOLEAN_ARRAY( 5 );

        private final int comparabilityGroup;

        Id( int comparabilityGroup )
        {
            this.comparabilityGroup = comparabilityGroup;
        }

        public int comparabilityGroup()
        {
            return comparabilityGroup;
        }
    }

    interface WithId
    {
        Id valueGroupId();
    }

    interface VNumber extends WithId
    {
        int compareTo( VInteger other );

        int compareTo( VFloatingPoint other );
    }

    interface VInteger extends VNumber
    {
        long longValue();

        default Id valueGroupId()
        {
            return Id.INTEGER;
        }
    }

    interface VFloatingPoint extends VNumber
    {
        double doubleValue();

        default Id valueGroupId()
        {
            return Id.FLOAT;
        }
    }

    interface VBoolean extends WithId
    {
        boolean booleanValue();

        default Id valueGroupId()
        {
            return Id.BOOLEAN;
        }

        int compareTo( VBoolean other );
    }

    interface VText extends WithId
    {
        String stringValue();

        default Id valueGroupId()
        {
            return Id.TEXT;
        }

        int compareTo( VText other );
    }

    interface VNumberArray extends WithId
    {
        int length();

        int compareTo( VIntegerArray other );

        int compareTo( VFloatingPointArray other );
    }

    interface VIntegerArray extends VNumberArray
    {
        long longValue( int offset );

        default Id valueGroupId()
        {
            return Id.INTEGER_ARRAY;
        }
    }

    interface VFloatingPointArray extends VNumberArray
    {
        double doubleValue( int offset );

        default Id valueGroupId()
        {
            return Id.FLOAT_ARRAY;
        }
    }

    interface VBooleanArray extends WithId
    {
        int length();

        boolean booleanValue( int offset );

        default Id valueGroupId()
        {
            return Id.BOOLEAN_ARRAY;
        }

        int compareTo( VBooleanArray other );
    }

    interface VTextArray extends WithId
    {
        int length();

        String stringValue( int offset );

        default Id valueGroupId()
        {
            return Id.TEXT_ARRAY;
        }

        int compareTo( VTextArray other );
    }
}
