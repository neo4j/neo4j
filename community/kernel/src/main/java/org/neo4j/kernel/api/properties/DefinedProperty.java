/*
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

import org.neo4j.helpers.ObjectUtil;
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

    private static final long NON_DOUBLE_LONG = 0xFFE0_0000_0000_0000L; // doubles are exact integers up to 53 bits

    static boolean numbersEqual( double fpn, long in )
    {
        if ( in < 0 )
        {
            if ( fpn < 0.0 )
            {
                if ( (NON_DOUBLE_LONG & in) == NON_DOUBLE_LONG ) // the high order bits are only sign bits
                { // no loss of precision if converting the long to a double, so it's safe to compare as double
                    return fpn == (double) in;
                }
                else if ( fpn < (double) Long.MIN_VALUE )
                { // the double is too big to fit in a long, they cannot be equal
                    return false;
                }
                else if ( (fpn == Math.floor( fpn )) && !Double.isInfinite( fpn ) ) // no decimals
                { // safe to compare as long
                    return in == (long) fpn;
                }
            }
        }
        else
        {
            if ( !(fpn < 0.0) )
            {
                if ( (NON_DOUBLE_LONG & in) == 0 ) // the high order bits are only sign bits
                { // no loss of precision if converting the long to a double, so it's safe to compare as double
                    return fpn == (double) in;
                }
                else if ( fpn > (double) Long.MAX_VALUE )
                { // the double is too big to fit in a long, they cannot be equal
                    return false;
                }
                else if ( (fpn == Math.floor( fpn )) && !Double.isInfinite( fpn ) )  // no decimals
                { // safe to compare as long
                    return in == (long) fpn;
                }
            }
        }
        return false;
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
            if ( !numbersEqual( fps.doubleValue( i ), ins.longValue( i ) ) )
            {
                return false;
            }
        }
        return true;
    }
}
