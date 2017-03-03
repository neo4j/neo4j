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
package org.neo4j.kernel.api.schema_new;

import java.util.Arrays;
import java.util.Comparator;

import static org.neo4j.kernel.impl.api.PropertyValueComparison.COMPARE_VALUES;

import static java.lang.String.format;
import static java.lang.String.valueOf;

/**
 * Holder for n property values, ordered according to a schema descriptor property id order
 */
public class OrderedPropertyValues
{
    // FACTORY METHODS

    public static OrderedPropertyValues of( Object... values )
    {
        return new OrderedPropertyValues( values );
    }

    public static OrderedPropertyValues of( IndexQuery.ExactPredicate[] exactPreds )
    {
        Object[] values = new Object[exactPreds.length];
        for ( int i = 0; i < exactPreds.length; i++ )
        {
            values[i] = exactPreds[i].value();
        }
        return new OrderedPropertyValues( values );
    }

    // ACTUAL CLASS

    private final Object[] values;

    private OrderedPropertyValues( Object[] values )
    {
        this.values = values;
    }

    public Object[] values()
    {
        return values;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        OrderedPropertyValues that = (OrderedPropertyValues) o;

        return Arrays.deepEquals( values, that.values );
    }

    @Override
    public int hashCode()
    {
        return Arrays.deepHashCode( values );
    }

    public int size()
    {
        return values.length;
    }

    public Object getSinglePropertyValue()
    {
        assert values.length == 1 : "Assumed single property but had " + values.length;
        return values[0];
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        String sep = "( ";
        for ( Object value : values )
        {
            sb.append( sep );
            sep = ", ";
            sb.append( quote( value ) );
        }
        sb.append( " )" );
        return sb.toString();
    }

    // STATIC HELPERS

    public static String quote( Object propertyValue )
    {
        if ( propertyValue instanceof String )
        {
            return format( "'%s'", propertyValue );
        }
        else if ( propertyValue.getClass().isArray() )
        {
            Class<?> type = propertyValue.getClass().getComponentType();
            if ( type == Boolean.TYPE )
            {
                return Arrays.toString( (boolean[]) propertyValue );
            } else if ( type == Byte.TYPE )
            {
                return Arrays.toString( (byte[]) propertyValue );
            } else if ( type == Short.TYPE )
            {
                return Arrays.toString( (short[]) propertyValue );
            } else if ( type == Character.TYPE )
            {
                return Arrays.toString( (char[]) propertyValue );
            } else if ( type == Integer.TYPE )
            {
                return Arrays.toString( (int[]) propertyValue );
            } else if ( type == Long.TYPE )
            {
                return Arrays.toString( (long[]) propertyValue );
            } else if ( type == Float.TYPE )
            {
                return Arrays.toString( (float[]) propertyValue );
            } else if ( type == Double.TYPE )
            {
                return Arrays.toString( (double[]) propertyValue );
            }
            return Arrays.toString( (Object[]) propertyValue );
        }
        return valueOf( propertyValue );
    }

    public static final Comparator<OrderedPropertyValues> COMPARATOR = ( left, right ) ->
    {
        if ( left.values.length != right.values.length )
        {
            throw new IllegalStateException( "Comparing two OrderedPropertyValues of different lengths!" );
        }

        int compare = 0;
        for ( int i = 0; i < left.values.length; i++ )
        {
            compare = COMPARE_VALUES.compare( left.values[i], right.values[i] );
            if ( compare != 0 )
            {
                return compare;
            }
        }
        return compare;
    };
}
