/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v1.docs;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.bolt.v1.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.RecordingByteChannel;
import org.neo4j.bolt.v1.messaging.infrastructure.ValueNode;
import org.neo4j.bolt.v1.messaging.infrastructure.ValueRelationship;
import org.neo4j.bolt.v1.messaging.message.RecordMessage;
import org.neo4j.bolt.v1.messaging.util.ArrayByteChannel;
import org.neo4j.bolt.v1.packstream.BufferedChannelInput;
import org.neo4j.bolt.v1.packstream.BufferedChannelOutput;
import org.neo4j.bolt.v1.packstream.PackStream;
import org.neo4j.bolt.v1.packstream.PackType;
import org.neo4j.graphdb.RelationshipType;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessageWriter.NO_BOUNDARY_HOOK;
import static org.neo4j.bolt.v1.messaging.example.Paths.PATH_WITH_LENGTH_TWO;
import static org.neo4j.bolt.v1.runtime.spi.Records.record;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.MapUtil.map;

/** This tests that Neo4j value mappings described in the documentation work the way we say they do. */
@RunWith( Parameterized.class )
public class BoltValueDocTest
{
    @Parameterized.Parameter( 0 )
    public String type;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> documentedTypeMapping()
    {
        Collection<Object[]> mappings = new ArrayList<>();

        // Load the documented mappings
        for ( DocTable.Row row : DocsRepository
                .docs().read( "dev/serialization.asciidoc", "#bolt-type-system-mapping", DocTable.table )
                .get( 0 ) )
        {
            mappings.add( new Object[]{row.get( 0 )} );
        }

        return mappings;
    }

    @Test
    public void mappingShouldBeCorrect() throws Throwable
    {
        assertThat( serialize( neoValue( type ) ), isPackstreamType( type ) );
    }

    public static Matcher<? super byte[]> isPackstreamType( final String packStreamType )
    {
        return new TypeSafeMatcher<byte[]>()
        {
            @Override
            protected boolean matchesSafely( byte[] recordWithValue )
            {
                PackStream.Unpacker unpacker = new PackStream.Unpacker(
                        new BufferedChannelInput( 11 ).reset( new ArrayByteChannel( recordWithValue ) ) );

                try
                {
                    // Wrapped in a "Record" struct
                    unpacker.unpackStructHeader();
                    unpacker.unpackStructSignature();
                    unpacker.unpackListHeader();

                    PackType type = unpacker.peekNextType();
                    if ( type.name().equalsIgnoreCase( "struct" ) )
                    {
                        String structName = null;
                        unpacker.unpackStructHeader();
                        char sig = unpacker.unpackStructSignature();
                        switch ( sig )
                        {
                        case Neo4jPack.NODE:
                            structName = "node";
                            break;
                        case Neo4jPack.RELATIONSHIP:
                            structName = "relationship";
                            break;
                        case Neo4jPack.PATH:
                            structName = "path";
                            break;
                        default:
                            fail( "Unknown struct type: " + sig );
                        }
                        assertThat( structName, equalTo( packStreamType.toLowerCase() ) );
                    }
                    else
                    {
                        assertThat( type.name().toLowerCase(), equalTo( packStreamType.toLowerCase() ) );
                    }
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }

                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "PackStream type " + packStreamType );
            }
        };
    }

    private Object neoValue( String type )
    {
        if ( type.equalsIgnoreCase( "float" ) )
        {
            return 12345.12345d;
        }
        else if ( type.equalsIgnoreCase( "integer" ) )
        {
            return 1337L;
        }
        else if ( type.equalsIgnoreCase( "boolean" ) )
        {
            return true;
        }
        else if ( type.equalsIgnoreCase( "string" ) )
        {
            return "Steven Brookreson";
        }
        else if ( type.equalsIgnoreCase( "list" ) )
        {
            return asList( 1, 2, 3 );
        }
        else if ( type.equalsIgnoreCase( "map" ) )
        {
            return map( "k", 1 );
        }
        else if ( type.equalsIgnoreCase( "node" ) )
        {
            return new ValueNode( 12, singletonList( label( "User" ) ), map() );
        }
        else if ( type.equalsIgnoreCase( "relationship" ) )
        {
            return new ValueRelationship( 12, 1, 2, RelationshipType.withName( "KNOWS" ), map() );
        }
        else if ( type.equalsIgnoreCase( "path" ) )
        {
            return PATH_WITH_LENGTH_TWO;
        }
        else if ( type.equalsIgnoreCase( "null" ) )
        {
            return null;
        }
        else if ( type.equalsIgnoreCase( "identity" ) )
        {
            return null; // TODO: No representation for identity yet
        }
        else
        {
            throw new RuntimeException( "Unknown neo type: " + type );
        }
    }

    private byte[] serialize( Object neoValue ) throws IOException
    {
        RecordingByteChannel channel = new RecordingByteChannel();
        BoltResponseMessageWriter writer = new BoltResponseMessageWriter(
                new Neo4jPack.Packer( new BufferedChannelOutput( channel ) ), NO_BOUNDARY_HOOK );

        writer.onRecord( record( neoValue ) );
        writer.flush();

        channel.eof();
        return channel.getBytes();
    }
}
