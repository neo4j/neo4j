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

import java.util.Comparator;

import static java.lang.String.format;

class ValueComparator implements Comparator<Value>
{
    private final Comparator<ValueGroup.Id> valueGroupComparator;

    ValueComparator( Comparator<ValueGroup.Id> valueGroupComparator )
    {
        this.valueGroupComparator = valueGroupComparator;
    }

    @Override
    public int compare( Value v1, Value v2 )
    {
        assert v1 != null && v2 != null : "null values are not supported, use NoValue.NO_VALUE instead";

        ValueGroup.Id id1 = v1.valueGroupId();
        ValueGroup.Id id2 = v2.valueGroupId();

        int x = valueGroupComparator.compare( id1, id2 );

        if ( x == 0 )
        {
            switch ( id1 )
            {
            case NO_VALUE:
                return x;

            case INTEGER:
                return compareNumberScalar( (ValueGroup.VInteger)v1, v2 );

            case FLOAT:
                return compareNumberScalar( (ValueGroup.VFloatingPoint)v1, v2 );

            case TEXT:
                return ((ValueGroup.VText) v1).compareTo( (ValueGroup.VText) v2 );

            case BOOLEAN:
                return ((ValueGroup.VBoolean) v1).compareTo( (ValueGroup.VBoolean) v2 );

            case INTEGER_ARRAY:
                return compareNumberArray( (ValueGroup.VIntegerArray)v1, v2 );

            case FLOAT_ARRAY:
                return compareNumberArray( (ValueGroup.VFloatingPointArray)v1, v2 );

            case TEXT_ARRAY:
                return ((ValueGroup.VTextArray) v1).compareTo( (ValueGroup.VTextArray) v2 );

            case BOOLEAN_ARRAY:
                return ((ValueGroup.VBooleanArray) v1).compareTo( (ValueGroup.VBooleanArray) v2 );

            default:
                throw new UnsupportedOperationException( format( "Unknown ValueGroup id '%s'", id1 ) );
            }
        }
        return x;
    }

    private int compareNumberScalar( ValueGroup.VInteger v1, Value v2 )
    {
        switch ( v2.valueGroupId() )
        {
        case INTEGER:
            return v1.compareTo( (ValueGroup.VInteger)v2 );

        case FLOAT:
            return v1.compareTo( (ValueGroup.VFloatingPoint)v2 );

        default:
            throw new UnsupportedOperationException( format(
                    "Cannot compare values of type %s with type %s", ValueGroup.Id.INTEGER, v2.valueGroupId() ) );
        }
    }

    private int compareNumberScalar( ValueGroup.VFloatingPoint v1, Value v2 )
    {
        switch ( v2.valueGroupId() )
        {
        case INTEGER:
            return v1.compareTo( (ValueGroup.VInteger)v2 );

        case FLOAT:
            return v1.compareTo( (ValueGroup.VFloatingPoint)v2 );

        default:
            throw new UnsupportedOperationException( format(
                    "Cannot compare values of type %s with type %s", ValueGroup.Id.FLOAT, v2.valueGroupId() ) );
        }
    }

    private int compareNumberArray( ValueGroup.VIntegerArray v1, Value v2 )
    {
        switch ( v2.valueGroupId() )
        {
        case INTEGER:
            return v1.compareTo( (ValueGroup.VIntegerArray)v2 );

        case FLOAT:
            return v1.compareTo( (ValueGroup.VFloatingPointArray)v2 );

        default:
            throw new UnsupportedOperationException( format(
                    "Cannot compare values of type %s with type %s", ValueGroup.Id.INTEGER, v2.valueGroupId() ) );
        }
    }

    private int compareNumberArray( ValueGroup.VFloatingPointArray v1, Value v2 )
    {
        switch ( v2.valueGroupId() )
        {
        case INTEGER:
            return v1.compareTo( (ValueGroup.VIntegerArray)v2 );

        case FLOAT:
            return v1.compareTo( (ValueGroup.VFloatingPointArray)v2 );

        default:
            throw new UnsupportedOperationException( format(
                    "Cannot compare values of type %s with type %s", ValueGroup.Id.FLOAT, v2.valueGroupId() ) );
        }
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj != null && obj instanceof ValueComparator;
    }

    @Override
    public int hashCode()
    {
        return 1;
    }
}
