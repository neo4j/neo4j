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

import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.packstream.PackedInputArray;
import org.neo4j.bolt.v1.packstream.PackedOutputArray;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.virtual.ListValue;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.virtual.VirtualValues.list;

public class Neo4jPackV2Test
{
    @Test
    public void shouldFailToPackPointWithIllegalDimensions() throws IOException
    {
        testPackingPointsWithWrongDimensions( 0 );
        testPackingPointsWithWrongDimensions( 1 );
        testPackingPointsWithWrongDimensions( 4 );
        testPackingPointsWithWrongDimensions( 100 );
    }

    @Test
    public void shouldFailToUnpack2DPointWithIncorrectCoordinate() throws IOException
    {
        Neo4jPackV2 neo4jPack = new Neo4jPackV2();
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( output );

        packer.packStructHeader( 3, Neo4jPackV2.POINT_2D );
        packer.pack( intValue( WGS84.getCode() ) );
        packer.pack( doubleValue( 42.42 ) );

        try
        {
            unpack( output );
            fail( "Exception expected" );
        }
        catch ( IOException ignore )
        {
        }
    }

    @Test
    public void shouldFailToUnpack3DPointWithIncorrectCoordinate() throws IOException
    {
        Neo4jPackV2 neo4jPack = new Neo4jPackV2();
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( output );

        packer.packStructHeader( 4, Neo4jPackV2.POINT_3D );
        packer.pack( intValue( Cartesian.getCode() ) );
        packer.pack( doubleValue( 1.0 ) );
        packer.pack( doubleValue( 100.1 ) );

        try
        {
            unpack( output );
            fail( "Exception expected" );
        }
        catch ( IOException ignore )
        {
        }
    }

    @Test
    public void shouldPackAndUnpack2DPoints() throws IOException
    {
        testPackingAndUnpackingOfPoints( 2 );
    }

    @Test
    public void shouldPackAndUnpack3DPoints() throws IOException
    {
        testPackingAndUnpackingOfPoints( 3 );
    }

    @Test
    public void shouldPackAndUnpackListsOf2DPoints() throws IOException
    {
        testPackingAndUnpackingOfListsOfPoints( 2 );
    }

    @Test
    public void shouldPackAndUnpackListsOf3DPoints() throws IOException
    {
        testPackingAndUnpackingOfListsOfPoints( 3 );
    }

    private static void testPackingAndUnpackingOfListsOfPoints( int pointDimension ) throws IOException
    {
        List<ListValue> pointLists = IntStream.range( 0, 1000 )
                .mapToObj( index -> randomListOfPoints( index, pointDimension ) )
                .collect( toList() );

        for ( ListValue original : pointLists )
        {
            ListValue unpacked = packAndUnpack( original );
            assertEquals( "Failed on " + original, original, unpacked );
        }
    }

    private static void testPackingAndUnpackingOfPoints( int dimension ) throws IOException
    {
        List<PointValue> points = IntStream.range( 0, 1000 )
                .mapToObj( index -> randomPoint( index, dimension ) )
                .collect( toList() );

        for ( PointValue original : points )
        {
            PointValue unpacked = packAndUnpack( original );
            assertEquals( "Failed on " + original, original, unpacked );
        }
    }

    private static void testPackingPointsWithWrongDimensions( int dimensions ) throws IOException
    {
        PointValue point = randomPoint( 0, dimensions );
        try
        {
            pack( point );
            fail( "Exception expected" );
        }
        catch ( IllegalArgumentException ignore )
        {
        }
    }

    private static <T extends AnyValue> T packAndUnpack( T value ) throws IOException
    {
        return unpack( pack( value ) );
    }

    private static PackedOutputArray pack( AnyValue value ) throws IOException
    {
        Neo4jPackV2 neo4jPack = new Neo4jPackV2();
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( output );
        packer.pack( value );
        return output;
    }

    @SuppressWarnings( "unchecked" )
    private static <T extends AnyValue> T unpack( PackedOutputArray output ) throws IOException
    {
        Neo4jPackV2 neo4jPack = new Neo4jPackV2();
        PackedInputArray input = new PackedInputArray( output.bytes() );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );
        AnyValue unpack = unpacker.unpack();
        return (T) unpack;
    }

    private static ListValue randomListOfPoints( int index, int pointDimension )
    {
        PointValue[] pointValues = ThreadLocalRandom.current()
                .ints( 100, 1, 100 )
                .mapToObj( i -> randomPoint( index, pointDimension ) )
                .toArray( PointValue[]::new );

        return list( pointValues );
    }

    private static PointValue randomPoint( int index, int dimension )
    {
        CoordinateReferenceSystem crs = index % 2 == 0 ? WGS84 : Cartesian;
        return pointValue( crs, ThreadLocalRandom.current().doubles( dimension, Double.MIN_VALUE, Double.MAX_VALUE ).toArray() );
    }
}
