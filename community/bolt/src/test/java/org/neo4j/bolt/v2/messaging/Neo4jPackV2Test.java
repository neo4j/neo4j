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
import org.neo4j.values.storable.PointValue;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.Values.pointValue;

public class Neo4jPackV2Test
{
    @Test
    public void shouldPackAndUnpackPoints() throws IOException
    {
        testPackingAndUnpackingOfPoints( 2 );
        testPackingAndUnpackingOfPoints( 3 );
        testPackingAndUnpackingOfPoints( 10 );
    }

    private static void testPackingAndUnpackingOfPoints( int dimension ) throws IOException
    {
        List<PointValue> points = IntStream.range( 0, 1000 )
                .mapToObj( index -> index % 2 == 0 ? WGS84 : Cartesian )
                .map( crs -> pointValue( crs, ThreadLocalRandom.current().doubles( dimension ).toArray() ) )
                .collect( toList() );

        for ( PointValue original : points )
        {
            PointValue unpacked = packAndUnpack( original );

            String message = "Failed on " + original;
            assertEquals( message, original.getCoordinateReferenceSystem(), unpacked.getCoordinateReferenceSystem() );
            assertEquals( message, original.getCoordinate(), unpacked.getCoordinate() );
        }
    }

    @SuppressWarnings( "unchecked" )
    private static <T extends AnyValue> T packAndUnpack( T value ) throws IOException
    {
        Neo4jPackV2 neo4jPack = new Neo4jPackV2();

        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( output );

        packer.pack( value );

        PackedInputArray input = new PackedInputArray( output.bytes() );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );

        return (T) unpacker.unpack();
    }
}
