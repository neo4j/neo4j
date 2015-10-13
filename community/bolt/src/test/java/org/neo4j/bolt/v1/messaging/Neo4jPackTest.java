/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.v1.packstream.PackedInputArray;
import org.neo4j.bolt.v1.packstream.PackedOutputArray;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.example.Nodes.ALICE;
import static org.neo4j.bolt.v1.messaging.example.Paths.ALL_PATHS;
import static org.neo4j.bolt.v1.messaging.example.Relationships.ALICE_KNOWS_BOB;
import static org.neo4j.bolt.v1.messaging.example.Support.labels;

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

    @Test
    public void shouldBeAbleToPackAndUnpackMapStream() throws IOException
    {
        // Given
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = new Neo4jPack.Packer( output );
        packer.packMapStreamHeader();
        for ( Map.Entry<String, Object> entry : ALICE.getAllProperties().entrySet() )
        {
            packer.pack( entry.getKey() );
            packer.pack( entry.getValue() );
        }
        packer.packEndOfStream();
        Object unpacked = unpacked( output.bytes() );

        // Then
        assertThat( unpacked, instanceOf( Map.class ) );
        Map<String, Object> unpackedMap = (Map<String, Object>) unpacked;
        assertThat( unpackedMap, equalTo( ALICE.getAllProperties() ) );
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

        // Expect
        exception.expect( BoltIOException.class );

        // When
        unpacked( output.bytes() );
    }

    @Test
    public void shouldBeAbleToPackAndUnpackNode() throws IOException
    {
        // Given
        Object unpacked = unpacked( packed( ALICE ) );

        // Then
        assertThat( unpacked, instanceOf( Node.class ) );
        Node unpackedNode = (Node) unpacked;
        assertThat( unpackedNode.getId(), equalTo( ALICE.getId() ) );
        assertThat( labels( unpackedNode ), equalTo( labels( ALICE ) ) );
        assertThat( unpackedNode.getAllProperties(), equalTo( ALICE.getAllProperties() ) );
    }

    @Test
    public void shouldBeAbleToPackAndUnpackRelationship() throws IOException
    {
        // Given
        Object unpacked = unpacked( packed( ALICE_KNOWS_BOB ) );

        // Then
        assertThat( unpacked, instanceOf( Relationship.class ) );
        Relationship unpackedRelationship = (Relationship) unpacked;
        assertThat( unpackedRelationship.getId(), equalTo( ALICE_KNOWS_BOB.getId() ) );
        assertThat( unpackedRelationship.getStartNode().getId(),
                equalTo( ALICE_KNOWS_BOB.getStartNode().getId() ) );
        assertThat( unpackedRelationship.getEndNode().getId(),
                equalTo( ALICE_KNOWS_BOB.getEndNode().getId() ) );
        assertThat( unpackedRelationship.getType().name(),
                equalTo( ALICE_KNOWS_BOB.getType().name() ) );
        assertThat( unpackedRelationship.getAllProperties(),
                equalTo( ALICE_KNOWS_BOB.getAllProperties() ) );
    }

    @Test
    public void shouldBeAbleToPackAndUnpackPaths() throws IOException
    {
        for ( Path path : ALL_PATHS )
        {
            System.out.println(path);

            // Given
            Object unpacked = unpacked( packed( path ) );

            // Then
            assertThat( unpacked, instanceOf( Path.class ) );
            Path unpackedPath = (Path) unpacked;
            assertThat( unpackedPath, equalTo( path ) );

        }
    }

}
