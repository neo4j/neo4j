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

interface ArrayValue
{
    int length();

    interface IntegralArray extends ArrayValue
    {
        long longValue( int index );
    }

    interface FloatingPointArray extends ArrayValue
    {
        double doubleValue( int index );
    }

    // <pre>
    final class ByteArray implements IntegralArray
    {
        private final byte[] value; ByteArray( byte[] value ) { this.value = value; }
        @Override public int length()                    { return value.length; }
        @Override public long longValue( int index )     { return value[index]; }
    }
    final class ShortArray implements IntegralArray
    {
        private final short[] value; ShortArray( short[] value ) { this.value = value; }
        @Override public int length()                    { return value.length; }
        @Override public long longValue( int index )     { return value[index]; }
    }
    final class IntArray implements IntegralArray
    {
        private final int[] value; IntArray( int[] value ) { this.value = value; }
        @Override public int length()                    { return value.length; }
        @Override public long longValue( int index )     { return value[index]; }
    }
    final class LongArray implements IntegralArray
    {
        private final long[] value; LongArray( long[] value ) { this.value = value; }
        @Override public int length()                    { return value.length; }
        @Override public long longValue( int index )     { return value[index]; }
    }
    final class FloatArray implements FloatingPointArray
    {
        private final float[] value; FloatArray( float[] value ) { this.value = value; }
        @Override public int length()                    { return value.length; }
        @Override public double doubleValue( int index ) { return value[index]; }
    }
    final class DoubleArray implements FloatingPointArray
    {
        private final double[] value; DoubleArray( double[] value ) { this.value = value; }
        @Override public int length()                    { return value.length; }
        @Override public double doubleValue( int index ) { return value[index]; }
    }
    final class NumberArray implements IntegralArray, FloatingPointArray
    {
        static IntegralArray asIntegral( Number[] value )           { return new NumberArray( value ); }
        static FloatingPointArray asFloatingPoint( Number[] value ) { return new NumberArray( value ); }
        private final Number[] value; private NumberArray( Number[] value ) { this.value = value; }
        @Override public int length()                    { return value.length; }
        @Override public long longValue( int index )     { return value[index].longValue(); }
        @Override public double doubleValue( int index ) { return value[index].doubleValue(); }
    }
    //</pre>
}
