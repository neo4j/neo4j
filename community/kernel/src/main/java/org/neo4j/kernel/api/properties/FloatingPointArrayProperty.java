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

abstract class FloatingPointArrayProperty extends DefinedProperty implements ArrayValue.FloatingPointArray
{
    FloatingPointArrayProperty( int propertyKeyId )
    {
        super( propertyKeyId );
    }

    public abstract int length();

    public abstract double doubleValue( int index );

    @Override
    final int valueHash()
    {
        return hash( this );
    }

    static int hash( FloatingPointArray value )
    {
        int result = 1;
        for ( int i = 0, len = value.length(); i < len; i++ )
        {
            long element = (long) value.doubleValue( i );
            int elementHash = (int) (element ^ (element >>> 32));
            result = 31 * result + elementHash;
        }
        return result;
    }

    @Override
    public final boolean valueEquals( Object other )
    {
        return valueEquals( this, other );
    }

    static boolean valueEquals( FloatingPointArray value, Object other )
    {
        if ( other instanceof double[] )
        {
            return numbersEqual( value, new DoubleArray( (double[]) other ) );
        }
        else if ( other instanceof float[] )
        {
            return numbersEqual( value, new FloatArray( (float[]) other ) );
        }
        else if ( other instanceof Number[] )
        {
            Number[] that = (Number[]) other;
            if ( that.length == value.length() )
            {
                if ( other instanceof Double[] || other instanceof Float[] )
                {
                    return numbersEqual( value, NumberArray.asFloatingPoint( that ) );
                }
                else
                {
                    return numbersEqual( value, NumberArray.asIntegral( that ) );
                }
            }
        }
        else if ( other instanceof long[] )
        {
            return numbersEqual( value, new LongArray( (long[]) other ) );
        }
        else if ( other instanceof int[] )
        {
            return numbersEqual( value, new IntArray( (int[]) other ) );
        }
        else if ( other instanceof short[] )
        {
            return numbersEqual( value, new ShortArray( (short[]) other ) );
        }
        else if ( other instanceof byte[] )
        {
            return numbersEqual( value, new ByteArray( (byte[]) other ) );
        }
        return false;
    }

    @Override
    final boolean hasEqualValue( DefinedProperty other )
    {
        if ( other instanceof FloatingPointArrayProperty )
        {
            FloatingPointArrayProperty that = (FloatingPointArrayProperty) other;
            return numbersEqual( this, that );
        }
        else if ( other instanceof IntegralArrayProperty )
        {
            IntegralArrayProperty that = (IntegralArrayProperty) other;
            return numbersEqual( this, that );
        }
        return false;
    }
}
