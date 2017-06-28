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
package org.neo4j.values.storable;

import java.util.Comparator;

import static java.lang.String.format;

/**
 * Comparator for values. Usable for sorting values, for example during index range scans.
 */
public class ValueComparator implements Comparator<Value>
{
    private final Comparator<ValueGroup> valueGroupComparator;

    ValueComparator(
            Comparator<ValueGroup> valueGroupComparator )
    {
        this.valueGroupComparator = valueGroupComparator;
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

            case NUMBER:
                switch ( v1.numberType() )
                {
                case INTEGRAL:
                    return compareNumberScalar( (IntegralValue)v1, v2 );

                case FLOATING_POINT:
                    return compareNumberScalar( (FloatingPointValue)v1, v2 );

                default:
                    throw new UnsupportedOperationException(
                        format( "Cannot handle ValueGroup id '%s' that is not Integral of FloatingPoint", id1 ) );
                }

            case TEXT:
                return ((TextValue) v1).compareTo( (TextValue) v2 );

            case BOOLEAN:
                return ((BooleanValue) v1).compareTo( (BooleanValue) v2 );

            case NUMBER_ARRAY:
                switch ( v1.numberType() )
                {
                case INTEGRAL:
                    return compareNumberArray( (IntegralArray)v1, v2 );

                case FLOATING_POINT:
                    return compareNumberArray( (FloatingPointArray)v1, v2 );

                default:
                    throw new UnsupportedOperationException(
                        format( "Cannot handle ValueGroup id '%s' that is not Integral of FloatingPoint", id1 ) );
                }

            case TEXT_ARRAY:
                return ((TextArray) v1).compareTo( (TextArray) v2 );

            case BOOLEAN_ARRAY:
                return ((BooleanArray) v1).compareTo( (BooleanArray) v2 );

            default:
                throw new UnsupportedOperationException( format(
                        "Cannot compare ValueGroup id '%s' using ValueComparator", id1
                ) );
            }
        }
        return x;
    }

    private int compareNumberScalar( IntegralValue v1, Value v2 )
    {
        switch ( v2.valueGroup() )
        {
        case NUMBER:
            switch ( v2.numberType() )
            {
            case INTEGRAL:
                return v1.compareTo( (IntegralValue)v2 );

            case FLOATING_POINT:
                return v1.compareTo( (FloatingPointValue)v2 );

            default:
                throw new UnsupportedOperationException( format(
                        "Cannot compare values of type %s with type %s", NumberType.INTEGRAL, v2.valueGroup() ) );
            }

        default:
            throw new UnsupportedOperationException( format(
                    "Cannot compare values of type %s with type %s", ValueGroup.NUMBER, v2.valueGroup() ) );
        }
    }

    private int compareNumberScalar( FloatingPointValue v1, Value v2 )
    {
        switch ( v2.valueGroup() )
        {
        case NUMBER:
            switch ( v2.numberType() )
            {
            case INTEGRAL:
                return v1.compareTo( (IntegralValue)v2 );

            case FLOATING_POINT:
                return v1.compareTo( (FloatingPointValue)v2 );

            default:
                throw new UnsupportedOperationException( format(
                        "Cannot compare values of type %s with type %s", NumberType.FLOATING_POINT, v2.valueGroup() ) );
            }

        default:
            throw new UnsupportedOperationException( format(
                    "Cannot compare values of type %s with type %s", ValueGroup.NUMBER, v2.valueGroup() ) );
        }
    }

    private int compareNumberArray( IntegralArray v1, Value v2 )
    {
        switch ( v2.valueGroup() )
        {
        case NUMBER_ARRAY:
            switch ( v2.numberType() )
            {
            case INTEGRAL:
                return v1.compareTo( (IntegralArray)v2 );

            case FLOATING_POINT:
                return v1.compareTo( (FloatingPointArray)v2 );

            default:
                throw new UnsupportedOperationException( format(
                        "Cannot compare values of type %s with type %s", NumberType.FLOATING_POINT, v2.valueGroup() ) );
            }

        default:
            throw new UnsupportedOperationException( format(
                    "Cannot compare values of type %s with type %s", ValueGroup.NUMBER, v2.valueGroup() ) );
        }
    }

    private int compareNumberArray( FloatingPointArray v1, Value v2 )
    {
        switch ( v2.valueGroup() )
        {
        case NUMBER_ARRAY:
            switch ( v2.numberType() )
            {
            case INTEGRAL:
                return v1.compareTo( (IntegralArray)v2 );

            case FLOATING_POINT:
                return v1.compareTo( (FloatingPointArray)v2 );

            default:
                throw new UnsupportedOperationException( format(
                        "Cannot compare values of type %s with type %s", NumberType.FLOATING_POINT, v2.valueGroup() ) );
            }

        default:
            throw new UnsupportedOperationException( format(
                    "Cannot compare values of type %s with type %s", ValueGroup.NUMBER, v2.valueGroup() ) );
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
