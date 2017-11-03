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
package org.neo4j.kernel.impl.store;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.impl.store.format.standard.StandardFormatSettings;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public enum GeometryType
{
    GEOMETRY_POINT( 0, "Point" )
            {
                @Override
                public Value decode( CoordinateReferenceSystem crs, int dimension, long[] valueBlocks, int offset )
                {
                    double[] coordinate = new double[dimension];
                    for ( int i = 0; i < dimension; i++ )
                    {
                        coordinate[i] = Double.longBitsToDouble( valueBlocks[i + 1 + offset] );
                    }
                    return Values.pointValue( crs, coordinate );
                }

                @Override
                public int calculateNumberOfBlocksUsedForGeometry( long firstBlock )
                {
                    int dimension = getDimension( firstBlock );
                    if ( dimension > 3 )
                    {
                        return PropertyType.BLOCKS_USED_FOR_BAD_TYPE_OR_ENCODING;
                    }
                    return 1 + dimension;
                }
            };

    public final int gtype;
    public final String name;

    GeometryType( int gtype, String name )
    {
        this.gtype = gtype;
        this.name = name;
    }

    public abstract Value decode( CoordinateReferenceSystem crs, int dimension, long[] valueBlocks, int offset );

    // TODO this might not work this way for Polygons or other geometries that do not
    // know their length just from the firstBlock. Since we will probably use dynamic property
    // stores, it might be that the answer here is just 1.
    public abstract int calculateNumberOfBlocksUsedForGeometry( long firstBlock );

    private static final GeometryType[] TYPES = GeometryType.values();
    private static final Map<String, GeometryType> all = new HashMap<>( TYPES.length );

    static
    {
        for ( GeometryType geometryType : TYPES )
        {
            all.put( geometryType.name, geometryType );
        }
    }

    private static int getGeometryType( long firstBlock )
    {
        return (int) ((firstBlock & 0x00000000F0000000L) >> 28);
    }

    private static int getDimension( long firstBlock )
    {
        return (int) ((firstBlock & 0x0000000F00000000L) >> 32);
    }

    private static int getCRSTable( long firstBlock )
    {
        return (int) ((firstBlock & 0x000000F000000000L) >> 36);
    }

    private static int getCRSCode( long firstBlock )
    {
        return (int) ((firstBlock & 0x00FFFF0000000000L) >> 40);
    }

    private static boolean isFloatPrecision( long firstBlock )
    {
        return ((firstBlock & 0x0100000000000000L) >> 56) == 1;
    }

    public static int calculateNumberOfBlocksUsed( long firstBlock )
    {
        GeometryType geometryType = find( getGeometryType( firstBlock ) );
        if ( geometryType == null )
        {
            return PropertyType.BLOCKS_USED_FOR_BAD_TYPE_OR_ENCODING;
        }
        else
        {
            return geometryType.calculateNumberOfBlocksUsedForGeometry( firstBlock );
        }
    }

    public static GeometryType find( int gtype )
    {
        if ( gtype < TYPES.length )
        {
            return TYPES[gtype];
        }
        else
        {
            // Kernel code requires no exceptions in deeper PropertyChain processing of corrupt/invalid data
            return null;
        }
    }

    public static GeometryType find( String name )
    {
        GeometryType table = all.get( name );
        if ( table != null )
        {
            return table;
        }
        else
        {
            throw new IllegalArgumentException( "No known Geometry Type: " + name );
        }
    }

    public static Value decode( PropertyBlock block )
    {
        return decode( block.getValueBlocks(), 0 );
    }

    public static Value decode( long[] valueBlocks, int offset )
    {
        long firstBlock = valueBlocks[offset];
        int gtype = getGeometryType( firstBlock );
        int dimension = getDimension( firstBlock );

        if ( isFloatPrecision( firstBlock ) )
        {
            throw new UnsupportedOperationException( "Float precision is unsupported in Geometry properties" );
        }
        if ( dimension > 3 )
        {
            throw new UnsupportedOperationException( "Points with more than 3 dimensions are not supported" );
        }
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( getCRSTable( firstBlock ), getCRSCode( firstBlock ) );
        return find( gtype ).decode( crs, dimension, valueBlocks, offset );
    }

    public static long[] encodePoint( int keyId, CoordinateReferenceSystem crs, double[] coordinate )
    {
        if ( coordinate.length > 3 )
        {
            // One property block can only contains at most 4x8 byte parts, one for header and 3 for coordinates
            throw new UnsupportedOperationException( "Points with more than 3 dimensions are not supported in the PropertyStore" );
        }

        long keyAndType = keyId | (((long) (PropertyType.GEOMETRY.intValue()) << StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS));
        long gtype_bits = GeometryType.GEOMETRY_POINT.gtype << (StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS + 4);
        long dimension_bits = ((long) coordinate.length) << (StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS + 8);
        long crsTableId_bits = ((long) crs.table.tableId) << (StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS + 12);
        long crsCode_bits = ((long) crs.code) << (StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS + 16);

        long[] data = new long[1 + coordinate.length];
        data[0] = keyAndType | gtype_bits | dimension_bits | crsTableId_bits | crsCode_bits;
        for ( int i = 0; i < coordinate.length; i++ )
        {
            data[1 + i] = Double.doubleToLongBits( coordinate[i] );
        }
        return data;
    }

    public static byte[] encodePointArray( PointValue[] points )
    {
        int dimension = points[0].coordinate().length;
        double[] data = new double[points.length * dimension];
        for ( int i = 0; i < data.length; i++ )
        {
            data[i] = points[i / dimension].coordinate()[i % dimension];
        }
        int code = points[0].getCoordinateReferenceSystem().code;
        GeometryHeader geometryHeader = new GeometryHeader( GeometryType.GEOMETRY_POINT.gtype, dimension,
                points[0].getCoordinateReferenceSystem() );
        byte[] bytes = DynamicArrayStore.encodeFromNumbers( data, DynamicArrayStore.GEOMETRY_HEADER_SIZE );
        geometryHeader.writeArrayHeaderTo( bytes );
        return bytes;
    }

    /**
     * Handler for header information for Geometry objects and arrays of Geometry objects
     */
    public static class GeometryHeader
    {
        public final int geometryType;
        private final int dimension;
        private final CoordinateReferenceSystem crs;

        private GeometryHeader( int geometryType, int dimension, CoordinateReferenceSystem crs )
        {
            this.geometryType = geometryType;
            this.dimension = dimension;
            this.crs = crs;
        }

        private GeometryHeader( int geometryType, int dimension, int crsTableId, int crsCode )
        {
            this( geometryType, dimension, CoordinateReferenceSystem.get( crsTableId, crsCode ) );
        }

        private void writeArrayHeaderTo( byte[] bytes )
        {
            bytes[0] = (byte) PropertyType.GEOMETRY.intValue();
            bytes[1] = (byte) geometryType;
            bytes[2] = (byte) dimension;
            bytes[3] = (byte) crs.table.tableId;
            bytes[4] = (byte) (crs.code & 0xFFL);
            bytes[5] = (byte) (crs.code >> 8 & 0xFFL);
        }

        static GeometryHeader fromArrayHeaderBytes( byte[] header )
        {
            int geometryType = header[1];
            int dimension = header[2];
            int crsTableId = header[3];
            int crsCode = header[4] + (header[5] << 8);
            return new GeometryHeader( geometryType, dimension, crsTableId, crsCode );
        }

        public static GeometryHeader fromArrayHeaderByteBuffer( ByteBuffer buffer )
        {
            int geometryType = buffer.get();
            int dimension = buffer.get();
            int crsTableId = buffer.get();
            int crsCode = buffer.get() + (buffer.get() << 8);
            return new GeometryHeader( geometryType, dimension, crsTableId, crsCode );
        }
    }

    public static ArrayValue decodePointArray( GeometryHeader header, byte[] data )
    {
        byte[] dataHeader = PropertyType.ARRAY.readDynamicRecordHeader( data );
        byte[] dataBody = new byte[data.length - dataHeader.length];
        System.arraycopy( data, dataHeader.length, dataBody, 0, dataBody.length );
        Value dataValue = DynamicArrayStore.getRightArray( Pair.of( dataHeader, dataBody ) );
        if ( dataValue instanceof FloatingPointArray )
        {
            FloatingPointArray numbers = (FloatingPointArray) dataValue;
            PointValue[] points = new PointValue[numbers.length() / header.dimension];
            for ( int i = 0; i < points.length; i++ )
            {
                double[] coords = new double[header.dimension];
                for ( int d = 0; d < header.dimension; d++ )
                {
                    coords[d] = numbers.doubleValue( i * header.dimension + d );
                }
                points[i] = Values.pointValue( header.crs, coords );
            }
            return Values.pointArray( points );
        }
        else
        {
            //TODO: Perhaps throw an exception
            return Values.EMPTY_POINT_ARRAY;
        }
    }
}
