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

import org.neo4j.kernel.impl.store.format.standard.StandardFormatSettings;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public enum GeometryType
{
    GEOMETRY_POINT( 0 )
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

    GeometryType( int gtype )
    {
        this.gtype = gtype;
    }

    public abstract Value decode( CoordinateReferenceSystem crs, int dimension, long[] valueBlocks, int offset );

    // TODO this might not work this way for Polygons or other geometries that do not
    // know their length just from the firstBlock. Since we will probably use dynamic property
    // stores, it might be that the answer here is just 1.
    public abstract int calculateNumberOfBlocksUsedForGeometry( long firstBlock );

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

    private static GeometryType find( int gtype )
    {
        for ( GeometryType type : GeometryType.values() )
        {
            if ( type.gtype == gtype )
            {
                return type;
            }
        }
        return null;
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
            throw new UnsupportedOperationException( "Points with more than 3 dimensions are not supported in the PropertyStore" );
        }
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( getCRSTable( firstBlock ), getCRSCode( firstBlock ) );
        return find( gtype ).decode( crs, dimension, valueBlocks, offset );
    }

    public static long[] encodePoint( int keyId, CoordinateReferenceSystem crs, double[] coordinate ) throws IllegalArgumentException
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
}
