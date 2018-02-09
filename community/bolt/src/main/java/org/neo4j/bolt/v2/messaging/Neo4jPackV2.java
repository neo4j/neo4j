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
package org.neo4j.bolt.v2.messaging;

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.packstream.PackInput;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.bolt.v1.packstream.PackStream;
import org.neo4j.bolt.v1.packstream.PackType;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;

import static org.neo4j.values.storable.Values.pointValue;

public class Neo4jPackV2 extends Neo4jPackV1
{
    public static final int VERSION = 2;

    public static final byte POINT_2D = 'X';
    public static final byte POINT_3D = 'Y';

    @Override
    public Neo4jPack.Packer newPacker( PackOutput output )
    {
        return new PackerV2( output );
    }

    @Override
    public Neo4jPack.Unpacker newUnpacker( PackInput input )
    {
        return new UnpackerV2( input );
    }

    @Override
    public int version()
    {
        return VERSION;
    }

    private static class PackerV2 extends Neo4jPackV1.PackerV1
    {
        PackerV2( PackOutput output )
        {
            super( output );
        }

        @Override
        public void writePoint( CoordinateReferenceSystem crs, double[] coordinate ) throws IOException
        {
            if ( coordinate.length == 2 )
            {
                packStructHeader( 2, POINT_2D );
                pack( crs.getCode() );
                pack( coordinate[0], coordinate[1] );
            }
            else if ( coordinate.length == 3 )
            {
                packStructHeader( 2, POINT_3D );
                pack( crs.getCode() );
                pack( coordinate[0], coordinate[1], coordinate[2] );
            }
            else
            {
                throw new IllegalArgumentException( "Point with 2D or 3D coordinate expected, " +
                                                    "got crs=" + crs + ", coordinate=" + Arrays.toString( coordinate ) );
            }
        }

        @Override
        public void pack( double value1, double value2 ) throws IOException
        {
            out.writeByte( PackStream.FLOAT_64_PAIR )
                    .writeDouble( value1 )
                    .writeDouble( value2 );
        }

        @Override
        public void pack( double value1, double value2, double value3 ) throws IOException
        {
            out.writeByte( PackStream.FLOAT_64_TRIPLE )
                    .writeDouble( value1 )
                    .writeDouble( value2 )
                    .writeDouble( value3 );
        }
    }

    private static class UnpackerV2 extends Neo4jPackV1.UnpackerV1
    {
        UnpackerV2( PackInput input )
        {
            super( input );
        }

        @Override
        protected AnyValue unpackStruct( char signature ) throws IOException
        {
            if ( signature == POINT_2D )
            {
                return unpackPoint2D();
            }
            else if ( signature == POINT_3D )
            {
                return unpackPoint3D();
            }
            else
            {
                return super.unpackStruct( signature );
            }
        }

        @Override
        public double[] unpackDoublePair() throws IOException
        {
            byte markerByte = in.readByte();
            if ( markerByte == PackStream.FLOAT_64_PAIR )
            {
                return new double[]{in.readDouble(), in.readDouble()};
            }
            throw new PackStream.Unexpected( PackType.FLOAT_PAIR, markerByte );
        }

        @Override
        public double[] unpackDoubleTriple() throws IOException
        {
            byte markerByte = in.readByte();
            if ( markerByte == PackStream.FLOAT_64_TRIPLE )
            {
                return new double[]{in.readDouble(), in.readDouble(), in.readDouble()};
            }
            throw new PackStream.Unexpected( PackType.FLOAT_TRIPLE, markerByte );
        }

        private PointValue unpackPoint2D() throws IOException
        {
            int crsCode = unpackInteger();
            CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( crsCode );
            double[] coordinates = unpackDoublePair();
            return pointValue( crs, coordinates );
        }

        private PointValue unpackPoint3D() throws IOException
        {
            int crsCode = unpackInteger();
            CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( crsCode );
            double[] coordinates = unpackDoubleTriple();
            return pointValue( crs, coordinates );
        }
    }
}
