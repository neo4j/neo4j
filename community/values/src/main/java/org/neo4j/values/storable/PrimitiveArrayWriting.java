/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.graphdb.spatial.Point;

/**
 * Static methods for writing primitive arrays to a ValueWriter.
 */
public final class PrimitiveArrayWriting
{
    public static <E extends Exception> void writeTo( ValueWriter<E> writer, byte[] values ) throws E
    {
        writer.beginArray( values.length, ValueWriter.ArrayType.BYTE );
        for ( byte x : values )
        {
            writer.writeInteger( x );
        }
        writer.endArray();
    }

    public static <E extends Exception> void writeTo( ValueWriter<E> writer, short[] values ) throws E
    {
        writer.beginArray( values.length, ValueWriter.ArrayType.SHORT );
        for ( short x : values )
        {
            writer.writeInteger( x );
        }
        writer.endArray();
    }

    public static <E extends Exception> void writeTo( ValueWriter<E> writer, int[] values ) throws E
    {
        writer.beginArray( values.length, ValueWriter.ArrayType.INT );
        for ( int x : values )
        {
            writer.writeInteger( x );
        }
        writer.endArray();
    }

    public static <E extends Exception> void writeTo( ValueWriter<E> writer, long[] values ) throws E
    {
        writer.beginArray( values.length, ValueWriter.ArrayType.LONG );
        for ( long x : values )
        {
            writer.writeInteger( x );
        }
        writer.endArray();
    }

    public static <E extends Exception> void writeTo( ValueWriter<E> writer, float[] values ) throws E
    {
        writer.beginArray( values.length, ValueWriter.ArrayType.FLOAT );
        for ( float x : values )
        {
            writer.writeFloatingPoint( x );
        }
        writer.endArray();
    }

    public static <E extends Exception> void writeTo( ValueWriter<E> writer, double[] values ) throws E
    {
        writer.beginArray( values.length, ValueWriter.ArrayType.DOUBLE );
        for ( double x : values )
        {
            writer.writeFloatingPoint( x );
        }
        writer.endArray();
    }

    public static <E extends Exception> void writeTo( ValueWriter<E> writer, boolean[] values ) throws E
    {
        writer.beginArray( values.length, ValueWriter.ArrayType.BOOLEAN );
        for ( boolean x : values )
        {
            writer.writeBoolean( x );
        }
        writer.endArray();
    }

    public static <E extends Exception> void writeTo( ValueWriter<E> writer, char[] values ) throws E
    {
        writer.beginArray( values.length, ValueWriter.ArrayType.CHAR );
        for ( char x : values )
        {
            writer.writeString( x );
        }
        writer.endArray();
    }

    public static <E extends Exception> void writeTo( ValueWriter<E> writer, String[] values ) throws E
    {
        writer.beginArray( values.length, ValueWriter.ArrayType.STRING );
        for ( String x : values )
        {
            writer.writeString( x );
        }
        writer.endArray();
    }

    public static <E extends Exception> void writeTo( ValueWriter<E> writer, Point[] values ) throws E
    {
        writer.beginArray( values.length, ValueWriter.ArrayType.POINT );
        for ( Point x : values )
        {
            PointValue value = Values.point( x );
            writer.writePoint( value.getCoordinateReferenceSystem(), value.coordinate() );
        }
        writer.endArray();
    }
}
