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
package org.neo4j.kernel.api.schema;

import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;
import static org.neo4j.kernel.impl.api.PropertyValueComparison.COMPARE_VALUES;

/**
 * Holder for n property values, ordered according to a schema descriptor property id order
 *
 * This implementation uses the Property class hierarchy to achieve correct equality between property values of
 * different types. However, the propertyKeyId is really not needed and we could consider reimplementing the class if
 * we make static methods for comparing property values.
 */
public class OrderedPropertyValues
{
    // FACTORY METHODS

    public static OrderedPropertyValues ofUndefined( Object... values )
    {
        return new OrderedPropertyValues(
                Arrays.stream( values )
                        .map( value -> Property.property( NO_SUCH_PROPERTY_KEY, value ) )
                        .toArray( DefinedProperty[]::new )
                );
    }

    public static OrderedPropertyValues of( DefinedProperty[] values )
    {
        return new OrderedPropertyValues( values );
    }

    public static OrderedPropertyValues of( IndexQuery.ExactPredicate[] exactPreds )
    {
        DefinedProperty[] values = new DefinedProperty[exactPreds.length];
        for ( int i = 0; i < exactPreds.length; i++ )
        {
            values[i] = Property.property( exactPreds[i].propertyKeyId(), exactPreds[i].value() );
        }
        return new OrderedPropertyValues( values );
    }

    // ACTUAL CLASS

    private final DefinedProperty[] properties;

    private OrderedPropertyValues( DefinedProperty[] properties )
    {
        this.properties = properties;
    }

    public Object valueAt( int position )
    {
        return properties[position].value();
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

        if ( that.properties.length != properties.length )
        {
            return false;
        }

        for ( int i = 0; i < properties.length; i++ )
        {
            if ( !properties[i].valueEquals( that.properties[i].value() ) )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        int result = 0;
        for ( DefinedProperty property : properties )
        {
            result = 31 * ( result + property.valueHash() );
        }
        return result;
    }

    public int size()
    {
        return properties.length;
    }

    public Object getSinglePropertyValue()
    {
        assert properties.length == 1 : "Assumed single property but had " + properties.length;
        return properties[0].value();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        String sep = "( ";
        for ( DefinedProperty property : properties )
        {
            sb.append( sep );
            sep = ", ";
            sb.append( quote( property.value() ) );
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
            }
            else if ( type == Byte.TYPE )
            {
                return Arrays.toString( (byte[]) propertyValue );
            }
            else if ( type == Short.TYPE )
            {
                return Arrays.toString( (short[]) propertyValue );
            }
            else if ( type == Character.TYPE )
            {
                return Arrays.toString( (char[]) propertyValue );
            }
            else if ( type == Integer.TYPE )
            {
                return Arrays.toString( (int[]) propertyValue );
            }
            else if ( type == Long.TYPE )
            {
                return Arrays.toString( (long[]) propertyValue );
            }
            else if ( type == Float.TYPE )
            {
                return Arrays.toString( (float[]) propertyValue );
            }
            else if ( type == Double.TYPE )
            {
                return Arrays.toString( (double[]) propertyValue );
            }
            return Arrays.toString( (Object[]) propertyValue );
        }
        return valueOf( propertyValue );
    }

    public static final Comparator<OrderedPropertyValues> COMPARATOR = ( left, right ) ->
    {
        if ( left.properties.length != right.properties.length )
        {
            throw new IllegalStateException( "Comparing two OrderedPropertyValues of different lengths!" );
        }

        int compare = 0;
        for ( int i = 0; i < left.properties.length; i++ )
        {
            compare = COMPARE_VALUES.compare( left.properties[i].value(), right.properties[i].value() );
            if ( compare != 0 )
            {
                return compare;
            }
        }
        return compare;
    };
}
