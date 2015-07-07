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

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.ndp.messaging.v1.MessageFormat;
import org.neo4j.ndp.messaging.v1.PackStreamMessageFormatV1;
import org.neo4j.ndp.messaging.v1.RecordingByteChannel;
import org.neo4j.ndp.messaging.v1.infrastructure.ValueNode;
import org.neo4j.ndp.messaging.v1.infrastructure.ValuePath;
import org.neo4j.ndp.messaging.v1.infrastructure.ValueRelationship;
import org.neo4j.ndp.messaging.v1.message.RecordMessage;
import org.neo4j.ndp.messaging.v1.util.ArrayByteChannel;
import org.neo4j.packstream.BufferedChannelInput;
import org.neo4j.packstream.BufferedChannelOutput;
import org.neo4j.packstream.PackStream;
import org.neo4j.packstream.PackType;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.ndp.docs.v1.DocTable.table;
import static org.neo4j.ndp.docs.v1.DocsRepository.docs;
import static org.neo4j.ndp.messaging.v1.PackStreamMessageFormatV1.Writer.NO_OP;
import static org.neo4j.ndp.runtime.spi.Records.record;

/** This tests that Neo4j value mappings described in the documentation work the way we say they do. */
@RunWith( Parameterized.class )
public class NDPValueDocTest
{
    @Parameterized.Parameter( 0 )
    public String type;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> documentedTypeMapping()
    {
        Collection<Object[]> mappings = new ArrayList<>();

        // Load the documented mappings
        for ( DocTable.Row row : docs().read( "dev/serialization.asciidoc", "#ndp-type-system-mapping", table )
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

    private Matcher<? super byte[]> isPackstreamType( final String packStreamType )
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
                        case PackStreamMessageFormatV1.NODE:
                            structName = "node";
                            break;
                        case PackStreamMessageFormatV1.RELATIONSHIP:
                            structName = "relationship";
                            break;
                        case PackStreamMessageFormatV1.PATH:
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
            return 1337l;
        }
        else if ( type.equalsIgnoreCase( "boolean" ) )
        {
            return true;
        }
        else if ( type.equalsIgnoreCase( "text" ) )
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
            return new ValueNode( 12, asList( label( "User" ) ), map() );
        }
        else if ( type.equalsIgnoreCase( "relationship" ) )
        {
            return new ValueRelationship( 12, 1, 2, DynamicRelationshipType.withName( "KNOWS" ), map() );
        }
        else if ( type.equalsIgnoreCase( "path" ) )
        {
            return new ValuePath();
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
        RecordMessage msg = new RecordMessage( record( neoValue ) );
        MessageFormat.Writer writer = new PackStreamMessageFormatV1.Writer(
                new PackStream.Packer( new BufferedChannelOutput( channel ) ), NO_OP );

        writer.write( msg ).flush();

        channel.eof();
        return channel.getBytes();
    }
}
