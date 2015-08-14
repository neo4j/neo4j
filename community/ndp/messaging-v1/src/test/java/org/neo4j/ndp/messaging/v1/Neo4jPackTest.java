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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.util.HexPrinter;
import org.neo4j.packstream.BufferedChannelInput;
import org.neo4j.packstream.BufferedChannelOutput;
import org.neo4j.packstream.ObjectPacker;
import org.neo4j.packstream.ObjectUnpacker;
import org.neo4j.packstream.PackListType;
import org.neo4j.packstream.PackStream;
import org.neo4j.packstream.PackedInputArray;
import org.neo4j.packstream.PackedOutputArray;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

import static org.neo4j.ndp.messaging.v1.example.Nodes.ALICE;
import static org.neo4j.ndp.messaging.v1.example.Nodes.BOB;
import static org.neo4j.ndp.messaging.v1.example.Nodes.CAROL;
import static org.neo4j.ndp.messaging.v1.example.Nodes.DAVE;
import static org.neo4j.ndp.messaging.v1.example.Paths.ALL_PATHS;
import static org.neo4j.ndp.messaging.v1.example.Relationships.ALICE_KNOWS_BOB;
import static org.neo4j.ndp.messaging.v1.example.Relationships.ALICE_LIKES_CAROL;
import static org.neo4j.ndp.messaging.v1.example.Relationships.CAROL_DISLIKES_BOB;
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

    private static class Machine
    {

        private final ByteArrayOutputStream output;
        private final WritableByteChannel writable;
        private final Neo4jPack.Packer packer;

        public Machine()
        {
            this.output = new ByteArrayOutputStream();
            this.writable = Channels.newChannel( this.output );
            this.packer = new Neo4jPack.Packer( new BufferedChannelOutput( this.writable ) );
        }

        public Machine( int bufferSize )
        {
            this.output = new ByteArrayOutputStream();
            this.writable = Channels.newChannel( this.output );
            this.packer = new Neo4jPack.Packer( new BufferedChannelOutput( this.writable, bufferSize ) );
        }

        public void reset()
        {
            output.reset();
        }

        public byte[] output()
        {
            return output.toByteArray();
        }

        public Neo4jPack.Packer packer()
        {
            return packer;
        }

    }

    @SafeVarargs
    private final <T> void testCanPackAndUnpackList( T... items ) throws Throwable
    {
        // Given
        final Machine machine = new Machine();
        final Neo4jPack.Packer packer = machine.packer();

        List<T> list = asList(items);
        int listSize = list.size();
        assertThat( listSize, greaterThan( 0 ) );  // otherwise we can't ascertain type information
        PackListType listType = null;

        // When
        for ( T item : list )
        {
            if ( listType == null )  // have we packed the list type yet?
            {
                listType = PackListType.struct( Neo4jPack.StructType.fromClass( item.getClass() ) );
                packer.packListHeader( listSize, listType );
                packer.flush();
            }
            packer.pack( item );
            packer.flush();
        }

        byte[] output = machine.output();
        System.out.println( HexPrinter.hex( output ) );
        ByteArrayInputStream input = new ByteArrayInputStream( output );
        final Neo4jPack.Unpacker unpacker = new Neo4jPack.Unpacker(
                new BufferedChannelInput( 16 ).reset( Channels.newChannel( input ) ) );

        // Then
        assertThat( unpacker.unpackListHeader(), equalTo( (long) listSize ) );
        assertThat( unpacker.unpackListType(), equalTo( listType ) );

        for ( Object item : list )
        {
            assertThat( unpacker.unpack(), equalTo( item ) );
        }

    }

    @Test
    public void testCanPackAndUnpackListOfNodes() throws Throwable
    {
        testCanPackAndUnpackList( ALICE, BOB, CAROL, DAVE );
    }

    @Test
    public void testCanPackAndUnpackListOfRelationships() throws Throwable
    {
        testCanPackAndUnpackList( ALICE_KNOWS_BOB, ALICE_LIKES_CAROL, CAROL_DISLIKES_BOB );
    }

    @Test
    public void testCanPackAndUnpackListOfPaths() throws Throwable
    {
        testCanPackAndUnpackList( ALL_PATHS );
    }

}