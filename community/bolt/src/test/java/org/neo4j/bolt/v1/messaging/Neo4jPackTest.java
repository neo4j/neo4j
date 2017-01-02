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
package org.neo4j.bolt.v1.messaging;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.v1.packstream.PackedInputArray;
import org.neo4j.bolt.v1.packstream.PackedOutputArray;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Path;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.HexPrinter;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.v1.messaging.example.Nodes.ALICE;
import static org.neo4j.bolt.v1.messaging.example.Paths.ALL_PATHS;
import static org.neo4j.bolt.v1.messaging.example.Relationships.ALICE_KNOWS_BOB;
import static org.neo4j.helpers.collection.MapUtil.map;

public class Neo4jPackTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private byte[] packed( Object object ) throws IOException
    {
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = new Neo4jPack.Packer( output );
        packer.pack( object );
        return output.bytes();
    }

    private Object unpacked( byte[] bytes ) throws IOException
    {
        PackedInputArray input = new PackedInputArray( bytes );
        Neo4jPack.Unpacker unpacker = new Neo4jPack.Unpacker( input );
        return unpacker.unpack();
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldBeAbleToPackAndUnpackListStream() throws IOException
    {
        // Given
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = new Neo4jPack.Packer( output );
        packer.packListStreamHeader();
        List<String> expected = new ArrayList<>();
        for ( Label label : ALICE.getLabels() )
        {
            String labelName = label.name();
            packer.pack( labelName );
            expected.add( labelName );
        }
        packer.packEndOfStream();
        Object unpacked = unpacked( output.bytes() );

        // Then
        assertThat( unpacked, instanceOf( List.class ) );
        List<String> unpackedList = (List<String>) unpacked;
        assertThat( unpackedList, equalTo( expected ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldBeAbleToPackAndUnpackMapStream() throws IOException
    {
        // Given
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = new Neo4jPack.Packer( output );
        packer.packMapStreamHeader();
        for ( Map.Entry<String,Object> entry : ALICE.getAllProperties().entrySet() )
        {
            packer.pack( entry.getKey() );
            packer.pack( entry.getValue() );
        }
        packer.packEndOfStream();
        Object unpacked = unpacked( output.bytes() );

        // Then
        assertThat( unpacked, instanceOf( Map.class ) );
        Map<String,Object> unpackedMap = (Map<String,Object>) unpacked;
        assertThat( unpackedMap, equalTo( ALICE.getAllProperties() ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldFailWhenTryingToPackAndUnpackMapStreamContainingNullKeys() throws IOException
    {
        // Given
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = new Neo4jPack.Packer( output );
        packer.packMapStreamHeader();

        HashMap<String,Object> map = new HashMap<>();
        map.put(null, 42L);
        map.put("foo", 1337L);
        for ( Map.Entry<String,Object> entry : map.entrySet() )
        {
            packer.pack( entry.getKey() );
            packer.pack( entry.getValue() );
        }
        packer.packEndOfStream();

        // When
        PackedInputArray input = new PackedInputArray( output.bytes() );
        Neo4jPack.Unpacker unpacker = new Neo4jPack.Unpacker( input );
        unpacker.unpack();

        // Then
        assertThat(unpacker.consumeError().get(), equalTo(
                Neo4jError.from( Status.Request.Invalid,
                        "Value `null` is not supported as key in maps, must be a non-nullable string." )));
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldFailWhenNullKeysInMaps() throws IOException
    {
        // Given
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = new Neo4jPack.Packer( output );
        HashMap<String,Object> map = new HashMap<>();
        map.put(null, 42L);
        map.put("foo", 1337L);
        packer.pack( map );
        assertTrue( packer.hasErrors() );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldFailNicelyWhenPackingAMapWithUnpackableValues() throws IOException
    {
        // Given
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = new Neo4jPack.Packer( output );
        packer.packRawMap( map( "unpackable", new Unpackable() ) );
        Object unpacked = unpacked( output.bytes() );

        // Then
        assertThat( unpacked, instanceOf( Map.class ) );
        Map<String,Object> unpackedMap = (Map<String,Object>) unpacked;
        assertThat( unpackedMap, equalTo( map( "unpackable", null ) ) );
        assertTrue( packer.hasErrors() );
    }

    @Test
    public void shouldErrorOnUnpackingMapWithDuplicateKeys() throws IOException
    {
        // Given
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = new Neo4jPack.Packer( output );
        packer.packMapHeader( 2 );
        packer.pack( "key" );
        packer.pack( 1 );
        packer.pack( "key" );
        packer.pack( 2 );

        // When
        PackedInputArray input = new PackedInputArray( output.bytes() );
        Neo4jPack.Unpacker unpacker = new Neo4jPack.Unpacker( input );
        unpacker.unpack();

        // Then
        assertThat(unpacker.consumeError().get(), equalTo( Neo4jError.from( Status.Request.Invalid, "Duplicate map key `key`." ) ));
    }

    @Test
    public void shouldHandleDeletedNodesGracefully() throws IOException
    {
        // Given
        Node node = mock( Node.class );
        when( node.getId() ).thenReturn( 42L );
        doThrow( NotFoundException.class ).when( node ).getAllProperties();
        doThrow( NotFoundException.class ).when( node ).getLabels();

        // When
        byte[] packed = packed( node );

        // Then
        //Node (signature=0x4E)
        //{
        //    id: 42 (0x2A)
        //    labels: [] (90)
        //    props: {} (A0)
        //}
        assertThat( HexPrinter.hex( packed ), equalTo( "B3 4E 2A 90 A0" ) );
    }

    @Test
    public void shouldNotBeAbleToUnpackNode() throws IOException
    {
        // Expect
        exception.expect( BoltIOException.class );
        // When
        unpacked( packed( ALICE ) );
    }

    @Test
    public void shouldNotBeAbleToUnpackRelationship() throws IOException
    {
        // Expect
        exception.expect( BoltIOException.class );
        // When
        unpacked( packed( ALICE_KNOWS_BOB ) );
    }

    @Test
    public void shouldNotBeAbleToUnpackPaths() throws IOException
    {
        for ( Path path : ALL_PATHS )
        {
            // Expect
            exception.expect( BoltIOException.class );
            // When
            unpacked( packed( path ) );
        }
    }

    @Test
    public void shouldTreatSingleCharAsSingleCharacterString() throws IOException
    {
        // Given
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = new Neo4jPack.Packer( output );
        packer.pack( 'C' );
        Object unpacked = unpacked( output.bytes() );

        // Then
        assertThat( unpacked, instanceOf( String.class ) );
        assertThat( unpacked, equalTo( "C" ) );
    }

    @Test
    public void shouldTreatCharArrayAsListOfStrings() throws IOException
    {
        // Given
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = new Neo4jPack.Packer( output );
        packer.pack( new char[]{'W', 'H', 'Y'} );
        Object unpacked = unpacked( output.bytes() );

        // Then
        assertThat( unpacked, instanceOf( List.class ) );
        assertThat( unpacked, equalTo( asList( "W", "H", "Y" ) ) );
    }

    private static class Unpackable
    {

    }
}
