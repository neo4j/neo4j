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
package org.neo4j.bolt.v1.docs;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.bolt.v1.transport.socket.Chunker;

import static java.lang.Integer.parseInt;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.kernel.impl.util.Codecs.decodeHexString;
import static org.neo4j.kernel.impl.util.HexPrinter.hex;

@RunWith( Parameterized.class )
public class BoltTransportChunkingDocTest
{
    @Parameterized.Parameter( 0 )
    public DocSerializationExample example;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> documentedChunkingExamples()
    {
        Collection<Object[]> mappings = new ArrayList<>();

        // Load the documented mappings
        for ( DocSerializationExample ex : DocsRepository.docs().read(
                "dev/transport.asciidoc",
                "code[data-lang=\"bolt_chunking_example\"]",
                DocSerializationExample.serialization_example ) )
        {
            mappings.add( new Object[]{ex} );
        }

        return mappings;
    }

    @Test
    public void serializingLeadsToSpecifiedOutput() throws Throwable
    {
        int chunkSize = parseInt( example.attribute( "Chunk size" ) );

        assertThat( "Serialized data should match documented representation:\n" + example,
                normalizedHex( example.serializedData() ),
                equalTo( serialize( chunkSize, messages( example ) ) ) );
    }

    private String serialize( int maxChunkSize, byte[][] messages ) throws IOException
    {
        return normalizedHex( hex( Chunker.chunk( maxChunkSize, messages ) ) );
    }

    private byte[][] messages( DocSerializationExample ex )
    {
        // Not very generic, but gets the job done for now.
        if ( ex.attribute( "Message data" ) != null )
        {
            String hex = ex.attribute( "Message data" ).replace( " ", "" );
            return new byte[][]{decodeHexString( hex )};
        }
        else
        {
            return new byte[][]{
                    decodeHexString( ex.attribute( "Message 1 data" ).replace( " ", "" ) ),
                    decodeHexString( ex.attribute( "Message 2 data" ).replace( " ", "" ) )};
        }
    }

    /** Convert a hex string into a normalized format for string comparison */
    private static String normalizedHex( String dirtyHex )
    {
        StringBuilder str = new StringBuilder( dirtyHex.replace( "\n", "" ).replace( " ", "" ) );
        int idx = str.length() - 2;

        while ( idx > 0 )
        {
            str.insert( idx, " " );
            idx = idx - 2;
        }

        return str.toString().toUpperCase();
    }
}
