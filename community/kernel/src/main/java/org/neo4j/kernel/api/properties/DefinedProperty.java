/**
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
package org.neo4j.kernel.api.properties;

import org.neo4j.helpers.ArrayUtil;
import org.neo4j.helpers.ArrayUtil.ArrayEquality;
import org.neo4j.kernel.impl.cache.SizeOfObject;

/**
 * Base class for properties that have a value.
 *
 * About {@link #sizeOfObjectInBytesIncludingOverhead() size} of property objects.
 * Java Object layout:
 *
 * |----4b----|----4b----|
 * |  header  |   ref    |
 * | hashCode | (unused) |
 * | (members of object) |
 * |   ...    |   ...    |
 * |   ...    |   ...    |
 * |---------------------|
 *
 * The property key, being an int and the first member of {@link Property} objects will be squeezed into
 * the (unused) area. Added members after that will be appended after that. The total space of the object
 * will be aligned to whole 8 bytes.
 */
public abstract class DefinedProperty extends Property implements SizeOfObject
{
    @Override
    public boolean isDefined()
    {
        return true;
    }

    @Override
    public abstract Object value();

    @Override
    public Object value( Object defaultValue )
    {
        return value();
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[propertyKeyId=" + propertyKeyId() + ", value=" + valueAsString() + "]";
    }

    @Override
    public final boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o != null && getClass() == o.getClass() )
        {
            DefinedProperty that = (DefinedProperty) o;
            return this.propertyKeyId == that.propertyKeyId && hasEqualValue( that );
        }
        return false;
    }

    @Override
    public final int hashCode()
    {
        return propertyKeyId ^ valueHash();
    }

    abstract int valueHash();

    abstract boolean hasEqualValue( DefinedProperty that );

    @Override
    public String valueAsString()
    {
        Object value = value();
        if ( value.getClass().isArray() )
        {
            return ArrayUtil.toString( value );
        }
        return value.toString();
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

    DefinedProperty( int propertyKeyId )
    {
        super( propertyKeyId );
    }

    protected boolean valueCompare( Object lhs, Object rhs )
    {
        return compareValues( lhs, rhs );
    }

    private static boolean compareValues( Object lhs, Object rhs )
    {
        // COMPARE NUMBERS
        if ( lhs instanceof Number && rhs instanceof Number )
        {
            return compareNumbers( (Number) lhs, (Number) rhs );
        }

        // COMPARE STRINGS
        if ( (lhs instanceof String || lhs instanceof Character) &&
                (rhs instanceof String || rhs instanceof Character) )
        {
            return lhs.toString().equals( rhs.toString() );
        }

        // COMPARE BOOLEANS
        if ( lhs instanceof Boolean && rhs instanceof Boolean )
        {
            return compareBooleans( (Boolean) lhs, (Boolean) rhs );
        }

        // COMPARE ARRAYS
        if ( lhs.getClass().isArray() && rhs.getClass().isArray() )
        {
            return ArrayUtil.equals( lhs, rhs, PROPERTY_EQUALITY );
        }

        return false;
    }

    private static boolean compareBooleans( Boolean lhs, Boolean rhs )
    {
        return lhs.equals( rhs );
    }

    private static boolean compareNumbers( Number aNumber, Number bNumber )
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

    private static final ArrayEquality PROPERTY_EQUALITY = new ArrayEquality()
    {
        @Override
        public boolean typeEquals( Class<?> firstType, Class<?> otherType )
        {   // Not always true, but we won't let type differences affect the outcome at this stage,
            // since many types are compatible in this property comparison.
            return true;
        }

        @Override
        public boolean itemEquals( Object lhs, Object rhs )
        {
            return compareValues( lhs, rhs );
        }
    };
}
