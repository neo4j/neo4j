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
package org.neo4j.ndp.docs.v1;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.ndp.messaging.v1.PackStreamMessageFormatV1;
import org.neo4j.ndp.messaging.v1.Neo4jPack;
import org.neo4j.ndp.messaging.v1.RecordingByteChannel;
import org.neo4j.ndp.messaging.v1.RecordingMessageHandler;
import org.neo4j.ndp.messaging.v1.util.ArrayByteChannel;
import org.neo4j.packstream.BufferedChannelInput;
import org.neo4j.packstream.BufferedChannelOutput;
import org.neo4j.packstream.PackStream;

import static org.junit.Assert.assertEquals;
import static org.neo4j.ndp.docs.v1.DocStruct.struct_definition;
import static org.neo4j.ndp.docs.v1.DocsRepository.docs;

/** This tests that message data structures look the way we say they do */
@RunWith( Parameterized.class )
public class NDPMessageStructsDocTest
{
    @Parameterized.Parameter( 0 )
    public DocStruct struct;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> documentedTypeMapping()
    {
        Collection<Object[]> mappings = new ArrayList<>();

        // Load the documented mappings
        for ( DocStruct struct : docs().read(
                "dev/messaging.asciidoc",
                "code[data-lang=\"ndp_message_struct\"]",
                struct_definition ) )
        {
            mappings.add( new Object[]{struct} );
        }

        return mappings;
    }

    @Test
    public void ensureSerializingMessagesAsDocumentedWorks() throws Throwable
    {
        // Given
        RecordingByteChannel ch = new RecordingByteChannel();
        PackStream.Packer packer = new PackStream.Packer( new BufferedChannelOutput( ch, 128 ) );

        // When I pack a message according to the documentation
        packer.packStructHeader( struct.size(), struct.signature() );
        for ( DocStruct.Field field : struct )
        {
            packValueOf( field.type(), packer );
        }
        packer.flush();

        // Then it should get interpreted as the documented message
        RecordingMessageHandler messages = new RecordingMessageHandler();
        PackStreamMessageFormatV1.Reader reader = new PackStreamMessageFormatV1.Reader(
                new Neo4jPack.Unpacker(
                        new BufferedChannelInput( 128 ).reset( new ArrayByteChannel( ch.getBytes() ) ) ) );
        reader.read( messages );

        // Hello, future traveler. The assertion below is not strictly necessary. What we're trying to do here
        // is simply to ensure that the documented message type is what we get back out when we deserialize, the
        // name of the class does not strictly have to map to the name in the docs, if that is causing you trouble.
        assertEquals( struct.name(), messages.asList().get( 0 ).getClass().getSimpleName() );
    }

    private void packValueOf( String type, PackStream.Packer packer ) throws IOException
    {
        if ( type.equalsIgnoreCase( "Text" ) )
        {
            packer.pack( "Hello, world!" );
        }
        else if ( type.startsWith( "Map" ) )
        {
            packer.packMapHeader( 1 );
            packer.pack( "k" );
            packer.pack( 12345 );
        }
        else if ( type.startsWith( "List" ) )
        {
            packer.packListHeader( 2 );
            packer.pack( 1 );
            packer.pack( 2 );
        }
        else
        {
            throw new RuntimeException( "Unknown type: " + type );
        }
    }
}
