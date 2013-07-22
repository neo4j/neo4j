/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.properties;

import java.lang.reflect.Array;

import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.PropertyDatas;

/**
 * Base class for properties that have a value.
 */
abstract class PropertyWithValue extends Property
{
    @Override
    public abstract Object value();

    @Override
    public Object value( Object defaultValue )
    {
        return value();
    }

    public String toString()
    {
        return getClass().getSimpleName() + "[propertyKeyId=" + propertyKeyId() + ", value=" + valueToString() + "]";
    }

    String valueToString()
    {
        return value().toString();
    }

    @Override
    public String stringValue()
    {
        Object value = value();
        throw new ClassCastException(
                String.format( "[%s:%s] is not a String", value, value.getClass().getSimpleName() ) );
    }

    @Override
    public String stringValue( String defaultValue )
    {
        return stringValue();
    }

    @Override
    public Number numberValue()
    {
        Object value = value();
        throw new ClassCastException(
                String.format( "[%s:%s] is not a Number", value, value.getClass().getSimpleName() ) );
    }

    @Override
    public Number numberValue( Number defaultValue )
    {
        return numberValue();
    }

    @Override
    public int intValue()
    {
        Object value = value();
        throw new ClassCastException(
                String.format( "[%s:%s] is not an int", value, value.getClass().getSimpleName() ) );
    }

    @Override
    public int intValue( int defaultValue )
    {
        return intValue();
    }

    @Override
    public long longValue()
    {
        Object value = value();
        throw new ClassCastException(
                String.format( "[%s:%s] is not a long", value, value.getClass().getSimpleName() ) );
    }

    @Override
    public long longValue( long defaultValue )
    {
        return longValue();
    }

    @Override
    public boolean booleanValue()
    {
        Object value = value();
        throw new ClassCastException(
                String.format( "[%s:%s] is not a boolean", value, value.getClass().getSimpleName() ) );
    }

    @Override
    public boolean booleanValue( boolean defaultValue )
    {
        return booleanValue();
    }

    @Override
    @Deprecated
    public PropertyData asPropertyDataJustForIntegration()
    {
        return PropertyDatas.forStringOrArray( (int) propertyKeyId(), -1, value() );
    }

    protected boolean valueCompare( Object a, Object b )
    {
        // COMPARE NUMBERS
        if ( a instanceof Number && b instanceof Number )
        {
            return compareNumbers( (Number) a, (Number) b );
        }

        // COMPARE STRINGS
        if ( (a instanceof String || a instanceof Character) &&
                (b instanceof String || b instanceof Character) )
        {
            return a.toString().equals( b.toString() );
        }

        // COMPARE ARRAYS
        if ( a.getClass().isArray() && b.getClass().isArray() )
        {
            return compareArrays( a, b );
        }

        return false;
    }

    private boolean compareArrays( Object a, Object b )
    {
        int aLength = Array.getLength( a );
        int bLength = Array.getLength( b );
        if ( aLength != bLength )
        {
            return false;
        }

        for ( int i = 0; i < aLength; i++ )
        {
            Object aObj = Array.get( a, i );
            Object bObj = Array.get( b, i );
            if ( !valueCompare( aObj, bObj ) )
            {
                return false;
            }
        }
        return true;
    }

    private boolean compareNumbers( Number aNumber, Number bNumber )
    {
        // If any of the two are non-integers
        if ( aNumber instanceof Float
                || bNumber instanceof Float
                || aNumber instanceof Double
                || bNumber instanceof Double )
        {
            double b = bNumber.doubleValue();
            double a = aNumber.doubleValue();
            return a == b;
        }

        return aNumber.longValue() == bNumber.longValue();
    }
}
