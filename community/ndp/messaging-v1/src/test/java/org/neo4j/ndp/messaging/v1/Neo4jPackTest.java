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
package org.neo4j.ndp.messaging.v1;

import java.io.IOException;

import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.util.HexPrinter;
import org.neo4j.packstream.PackedInputArray;
import org.neo4j.packstream.PackedOutputArray;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.neo4j.ndp.messaging.v1.example.Nodes.ALICE;
import static org.neo4j.ndp.messaging.v1.example.Paths.ALL_PATHS;
import static org.neo4j.ndp.messaging.v1.example.Relationships.ALICE_KNOWS_BOB;
import static org.neo4j.ndp.messaging.v1.example.Support.labels;
import static org.neo4j.ndp.messaging.v1.example.Support.properties;

public class Neo4jPackTest
{

    private byte[] packed( Object object ) throws IOException
    {
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = new Neo4jPack.Packer( output );
        packer.pack( object );
        return output.bytes();
    }

    private Object unpacked( byte[] bytes ) throws IOException
    {
        System.out.println( HexPrinter.hex( bytes ) );
        PackedInputArray input = new PackedInputArray( bytes );
        Neo4jPack.Unpacker unpacker = new Neo4jPack.Unpacker( input );
        return unpacker.unpack();
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
        assertThat( properties( unpackedNode ), equalTo( properties( ALICE ) ) );
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
        assertThat( properties( unpackedRelationship ),
                equalTo( properties( ALICE_KNOWS_BOB ) ) );
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