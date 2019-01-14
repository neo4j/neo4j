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

/**
 * For the PropertyStore format, check {@link PropertyStore}.
 * For the array format, check {@link DynamicArrayStore}.
 */
public enum GeometryType
{
    GEOMETRY_INVALID( 0, "Invalid" )
            {
                @Override
                public Value decode( CoordinateReferenceSystem crs, int dimension, long[] valueBlocks, int offset )
                {
                    throw new UnsupportedOperationException( "Cannot decode invalid geometry" );
                }

                @Override
                public int calculateNumberOfBlocksUsedForGeometry( long firstBlock )
                {
                    return PropertyType.BLOCKS_USED_FOR_BAD_TYPE_OR_ENCODING;
                }

                @Override
                public ArrayValue decodeArray( GeometryHeader header, byte[] data )
                {
                    throw new UnsupportedOperationException( "Cannot decode invalid geometry array" );
                }
            },
    GEOMETRY_POINT( 1, "Point" )
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
                    if ( dimension > GeometryType.getMaxSupportedDimensions() )
                    {
                        return PropertyType.BLOCKS_USED_FOR_BAD_TYPE_OR_ENCODING;
                    }
                    return 1 + dimension;
                }

                @Override
                public ArrayValue decodeArray( GeometryHeader header, byte[] data )
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
                        throw new InvalidRecordException(
                                "Point array with unexpected type. Actual:" + dataValue.getClass().getSimpleName() + ". Expected: FloatingPointArray." );
                    }
                }
            };

    /**
     * Handler for header information for Geometry objects and arrays of Geometry objects
     */
    public static class GeometryHeader
    {
        private final int geometryType;
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
            bytes[3] = (byte) crs.getTable().getTableId();
            bytes[4] = (byte) (crs.getCode() >> 8 & 0xFFL);
            bytes[5] = (byte) (crs.getCode() & 0xFFL);
        }

        static GeometryHeader fromArrayHeaderBytes( byte[] header )
        {
            int geometryType = Byte.toUnsignedInt( header[1] );
            int dimension = Byte.toUnsignedInt( header[2] );
            int crsTableId = Byte.toUnsignedInt( header[3] );
            int crsCode = (Byte.toUnsignedInt( header[4] ) << 8) + Byte.toUnsignedInt( header[5] );
            return new GeometryHeader( geometryType, dimension, crsTableId, crsCode );
        }

        public static GeometryHeader fromArrayHeaderByteBuffer( ByteBuffer buffer )
        {
            int geometryType = Byte.toUnsignedInt( buffer.get() );
            int dimension = Byte.toUnsignedInt( buffer.get() );
            int crsTableId = Byte.toUnsignedInt( buffer.get() );
            int crsCode = (Byte.toUnsignedInt( buffer.get() ) << 8) + Byte.toUnsignedInt( buffer.get() );
            return new GeometryHeader( geometryType, dimension, crsTableId, crsCode );
        }
    }

    private static final GeometryType[] TYPES = GeometryType.values();
    private static final Map<String, GeometryType> all = new HashMap<>( TYPES.length );

    static
    {
        for ( GeometryType geometryType : TYPES )
        {
            all.put( geometryType.name, geometryType );
        }
    }

    private static final long GEOMETRY_TYPE_MASK =  0x00000000F0000000L;
    private static final long DIMENSION_MASK =      0x0000000F00000000L;
    private static final long CRS_TABLE_MASK =      0x000000F000000000L;
    private static final long CRS_CODE_MASK =       0x00FFFF0000000000L;
    private static final long PRECISION_MASK =      0x0100000000000000L;

    private static int getGeometryType( long firstBlock )
    {
        return (int) ((firstBlock & GEOMETRY_TYPE_MASK) >> 28);
    }

    private static int getDimension( long firstBlock )
    {
        return (int) ((firstBlock & DIMENSION_MASK) >> 32);
    }

    private static int getCRSTable( long firstBlock )
    {
        return (int) ((firstBlock & CRS_TABLE_MASK) >> 36);
    }

    private static int getCRSCode( long firstBlock )
    {
        return (int) ((firstBlock & CRS_CODE_MASK) >> 40);
    }

    private static boolean isFloatPrecision( long firstBlock )
    {
        return ((firstBlock & PRECISION_MASK) >> 56) == 1;
    }

    private static int getMaxSupportedDimensions()
    {
        return PropertyType.getPayloadSizeLongs() - 1;
    }

    public static int calculateNumberOfBlocksUsed( long firstBlock )
    {
        GeometryType geometryType = find( getGeometryType( firstBlock ) );
        return geometryType.calculateNumberOfBlocksUsedForGeometry( firstBlock );
    }

    private static GeometryType find( int gtype )
    {
        if ( gtype < TYPES.length && gtype >= 0 )
        {
            return TYPES[gtype];
        }
        else
        {
            // Kernel code requires no exceptions in deeper PropertyChain processing of corrupt/invalid data
            return GEOMETRY_INVALID;
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
        if ( dimension > GeometryType.getMaxSupportedDimensions() )
        {
            throw new UnsupportedOperationException(
                    "Points with more than " + GeometryType.getMaxSupportedDimensions() +
                    " dimensions are not supported" );
        }
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( getCRSTable( firstBlock ), getCRSCode( firstBlock ) );
        return find( gtype ).decode( crs, dimension, valueBlocks, offset );
    }

    public static long[] encodePoint( int keyId, CoordinateReferenceSystem crs, double[] coordinate )
    {
        if ( coordinate.length > GeometryType.getMaxSupportedDimensions() )
        {
            // One property block can only contains at most 4x8 byte parts, one for header and 3 for coordinates
            throw new UnsupportedOperationException(
                    "Points with more than " + GeometryType.getMaxSupportedDimensions() +
                    " dimensions are not supported" );
        }

        int idBits = StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS;

        long keyAndType = keyId | (((long) (PropertyType.GEOMETRY.intValue()) << idBits));
        long gtypeBits = GeometryType.GEOMETRY_POINT.gtype << (idBits + 4);
        long dimensionBits = ((long) coordinate.length) << (idBits + 8);
        long crsTableIdBits = ((long) crs.getTable().getTableId()) << (idBits + 12);
        long crsCodeBits = ((long) crs.getCode()) << (idBits + 16);

        long[] data = new long[1 + coordinate.length];
        data[0] = keyAndType | gtypeBits | dimensionBits | crsTableIdBits | crsCodeBits;
        for ( int i = 0; i < coordinate.length; i++ )
        {
            data[1 + i] = Double.doubleToLongBits( coordinate[i] );
        }
        return data;
    }

    public static byte[] encodePointArray( PointValue[] points )
    {
        int dimension = points[0].coordinate().length;
        CoordinateReferenceSystem crs = points[0].getCoordinateReferenceSystem();
        for ( int i = 1; i < points.length; i++ )
        {
            if ( dimension != points[i].coordinate().length )
            {
                throw new IllegalArgumentException(
                        "Attempting to store array of points with inconsistent dimension. Point " + i + " has a different dimension." );
            }
            if ( !crs.equals( points[i].getCoordinateReferenceSystem() ) )
            {
                throw new IllegalArgumentException( "Attempting to store array of points with inconsistent CRS. Point " + i + " has a different CRS." );
            }
        }

        double[] data = new double[points.length * dimension];
        for ( int i = 0; i < data.length; i++ )
        {
            data[i] = points[i / dimension].coordinate()[i % dimension];
        }
        GeometryHeader geometryHeader = new GeometryHeader( GeometryType.GEOMETRY_POINT.gtype, dimension, crs );
        byte[] bytes = DynamicArrayStore.encodeFromNumbers( data, DynamicArrayStore.GEOMETRY_HEADER_SIZE );
        geometryHeader.writeArrayHeaderTo( bytes );
        return bytes;
    }

    public static ArrayValue decodeGeometryArray( GeometryHeader header, byte[] data )
    {
        return find( header.geometryType ).decodeArray( header, data );
    }

    private final int gtype;
    private final String name;

    GeometryType( int gtype, String name )
    {
        this.gtype = gtype;
        this.name = name;
    }

    public abstract Value decode( CoordinateReferenceSystem crs, int dimension, long[] valueBlocks, int offset );

    public abstract int calculateNumberOfBlocksUsedForGeometry( long firstBlock );

    public abstract ArrayValue decodeArray( GeometryHeader header, byte[] data );

    public int getGtype()
    {
        return gtype;
    }

    public String getName()
    {
        return name;
    }
}
