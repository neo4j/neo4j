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

/**
 * Comparator for values. Usable for sorting values, for example during index range scans.
 */
class ValueComparator implements Comparator<Value>
{
    private final Comparator<ValueGroup> valueGroupComparator;
    private final Comparator<VirtualValue> virtualValueComparator;

    ValueComparator(
            Comparator<ValueGroup> valueGroupComparator,
            Comparator<VirtualValue> virtualValueComparator )
    {
        this.valueGroupComparator = valueGroupComparator;
        this.virtualValueComparator = virtualValueComparator;
    }

    @Override
    public int compare( Value v1, Value v2 )
    {
        assert v1 != null && v2 != null : "null values are not supported, use NoValue.NO_VALUE instead";

        ValueGroup id1 = v1.valueGroup();
        ValueGroup id2 = v2.valueGroup();

        int x = valueGroupComparator.compare( id1, id2 );

        if ( x == 0 )
        {
            switch ( id1 )
            {
            case NO_VALUE:
                return x;

            case INTEGER:
                return compareNumberScalar( (IntegralValue)v1, v2 );

            case FLOAT:
                return compareNumberScalar( (FloatingPointValue)v1, v2 );

            case TEXT:
                return ((TextValue) v1).compareTo( (TextValue) v2 );

            case BOOLEAN:
                return ((BooleanValue) v1).compareTo( (BooleanValue) v2 );

            case INTEGER_ARRAY:
                return compareNumberArray( (IntegralArray)v1, v2 );

            case FLOAT_ARRAY:
                return compareNumberArray( (FloatingPointArray)v1, v2 );

            case TEXT_ARRAY:
                return ((TextArray) v1).compareTo( (TextArray) v2 );

            case BOOLEAN_ARRAY:
                return ((BooleanArray) v1).compareTo( (BooleanArray) v2 );

            case VIRTUAL:
                return virtualValueComparator.compare( (VirtualValue)v1, (VirtualValue)v2 );

            default:
                throw new UnsupportedOperationException( format( "Unknown ValueGroup id '%s'", id1 ) );
            }
        }
        return x;
    }

    private int compareNumberScalar( IntegralValue v1, Value v2 )
    {
        switch ( v2.valueGroup() )
        {
        case INTEGER:
            return v1.compareTo( (IntegralValue)v2 );

        case FLOAT:
            return v1.compareTo( (FloatingPointValue)v2 );

        default:
            throw new UnsupportedOperationException( format(
                    "Cannot compare values of type %s with type %s", ValueGroup.INTEGER, v2.valueGroup() ) );
        }
    }

    private int compareNumberScalar( FloatingPointValue v1, Value v2 )
    {
        switch ( v2.valueGroup() )
        {
        case INTEGER:
            return v1.compareTo( (IntegralValue)v2 );

        case FLOAT:
            return v1.compareTo( (FloatingPointValue)v2 );

        default:
            throw new UnsupportedOperationException( format(
                    "Cannot compare values of type %s with type %s", ValueGroup.FLOAT, v2.valueGroup() ) );
        }
    }

    private int compareNumberArray( IntegralArray v1, Value v2 )
    {
        switch ( v2.valueGroup() )
        {
        case INTEGER_ARRAY:
            return v1.compareTo( (IntegralArray)v2 );

        case FLOAT_ARRAY:
            return v1.compareTo( (FloatingPointArray)v2 );

        default:
            throw new UnsupportedOperationException( format(
                    "Cannot compare values of type %s with type %s", ValueGroup.INTEGER_ARRAY, v2.valueGroup() ) );
        }
    }

    private int compareNumberArray( FloatingPointArray v1, Value v2 )
    {
        switch ( v2.valueGroup() )
        {
        case INTEGER_ARRAY:
            return v1.compareTo( (IntegralArray)v2 );

        case FLOAT_ARRAY:
            return v1.compareTo( (FloatingPointArray)v2 );

        default:
            throw new UnsupportedOperationException( format(
                    "Cannot compare values of type %s with type %s", ValueGroup.FLOAT_ARRAY, v2.valueGroup() ) );
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
