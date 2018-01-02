/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.helpers.MathUtil;
import org.neo4j.helpers.ObjectUtil;

import java.util.Comparator;

import static org.neo4j.kernel.impl.api.PropertyValueComparison.COMPARE_VALUES;


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
public abstract class DefinedProperty extends Property
{
    public static Comparator<DefinedProperty> COMPARATOR = new Comparator<DefinedProperty>()
    {
        @Override
        public int compare( DefinedProperty left, DefinedProperty right )
        {
            int cmp = left.propertyKeyId - right.propertyKeyId;
            if ( cmp == 0 )
            {
                return COMPARE_VALUES.compare( left.value(), right.value() );
            }

            // else
            return cmp;
        }
    };

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
        if ( o instanceof DefinedProperty )
        {
            DefinedProperty that = (DefinedProperty) o;
            if ( this.propertyKeyId == that.propertyKeyId )
            {
                if ( o instanceof LazyProperty )
                { // the cost of boxing is small compared to what LazyProperty does
                    return that.valueEquals( value() );
                }
                else
                {
                    return hasEqualValue( that );
                }
            }
        }
        return false;
    }

    @Override
    public final int hashCode()
    {
        return propertyKeyId ^ valueHash();
    }

    abstract int valueHash();

    /** We never pass {@link LazyProperty} to this method, since we check for it in {@link #equals(Object)}. */
    abstract boolean hasEqualValue( DefinedProperty that );

    @Override
    public String valueAsString()
    {
        return ObjectUtil.toString( value() );
    }

    DefinedProperty( int propertyKeyId )
    {
        super( propertyKeyId );
    }

    static boolean numbersEqual( ArrayValue.IntegralArray lhs, ArrayValue.IntegralArray rhs )
    {
        int length = lhs.length();
        if ( length != rhs.length() )
        {
            return false;
        }
        for ( int i = 0; i < length; i++ )
        {
            if ( lhs.longValue( i ) != rhs.longValue( i ) )
            {
                return false;
            }
        }
        return true;
    }

    static boolean numbersEqual( ArrayValue.FloatingPointArray lhs, ArrayValue.FloatingPointArray rhs )
    {
        int length = lhs.length();
        if ( length != rhs.length() )
        {
            return false;
        }
        for ( int i = 0; i < length; i++ )
        {
            if ( lhs.doubleValue( i ) != rhs.doubleValue( i ) )
            {
                return false;
            }
        }
        return true;
    }

    static boolean numbersEqual( ArrayValue.FloatingPointArray fps, ArrayValue.IntegralArray ins )
    {
        int length = ins.length();
        if ( length != fps.length() )
        {
            return false;
        }
        for ( int i = 0; i < length; i++ )
        {
            if ( !MathUtil.numbersEqual( fps.doubleValue( i ), ins.longValue( i ) ) )
            {
                return false;
            }
        }
        return true;
    }

    public interface WithStringValue
    {
        String stringValue();
    }

    public interface WithDoubleValue
    {
        double doubleValue();
    }

    public interface WithLongValue
    {
        long longValue();
    }
}
