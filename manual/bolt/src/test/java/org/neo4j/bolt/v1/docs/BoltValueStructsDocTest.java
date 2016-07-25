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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.infrastructure.ValueNode;
import org.neo4j.bolt.v1.messaging.infrastructure.ValueUnboundRelationship;
import org.neo4j.bolt.v1.packstream.PackedOutputArray;
import org.neo4j.kernel.impl.util.HexPrinter;

import static java.lang.System.lineSeparator;
import static org.junit.Assert.assertEquals;
import static org.neo4j.bolt.v1.messaging.example.Nodes.ALICE;
import static org.neo4j.bolt.v1.messaging.example.Nodes.BOB;
import static org.neo4j.bolt.v1.messaging.example.Relationships.ALICE_KNOWS_BOB;

/**
 * This tests that message data structures look the way we say they do
 */
@RunWith(Parameterized.class)
public class BoltValueStructsDocTest
{

    // This lookup is used to check we hydrate structures to the correct types
    public static Map<String, String> expectedSerialization = new HashMap<>();

    static
    {
        expectedSerialization.put( "Node", "B3 4E C9 05" + lineSeparator() +
                                           "39 92 86 42" + lineSeparator() +
                                           "61 6E 61 6E" + lineSeparator() +
                                           "61 86 50 65" + lineSeparator() +
                                           "72 73 6F 6E" + lineSeparator() +
                                           "A1 81 6B C9" + lineSeparator() +
                                           "30 39" );
        expectedSerialization.put( "Relationship", "B5 52 C9 05" + lineSeparator() +
                                                   "39 C9 05 39" + lineSeparator() +
                                                   "C9 05 39 85" + lineSeparator() +
                                                   "31 32 33 34" + lineSeparator() +
                                                   "35 A1 81 6B" + lineSeparator() +
                                                   "C9 30 39" );
        expectedSerialization.put( "UnboundRelationship", "B3 72 C9 05" + lineSeparator() +
                                                          "39 87 20 22" + lineSeparator() +
                                                          "4B 4E 4F 57" + lineSeparator() +
                                                          "53 A1 81 6B" + lineSeparator() +
                                                          "C9 30 39" );
        expectedSerialization.put( "Path", "B3 50 92 B3" + lineSeparator() +
                                           "4E C9 03 E9" + lineSeparator() +
                                           "92 86 50 65" + lineSeparator() +
                                           "72 73 6F 6E" + lineSeparator() +
                                           "88 45 6D 70" + lineSeparator() +
                                           "6C 6F 79 65" + lineSeparator() +
                                           "65 A2 84 6E" + lineSeparator() +
                                           "61 6D 65 85" + lineSeparator() +
                                           "41 6C 69 63" + lineSeparator() +
                                           "65 83 61 67" + lineSeparator() +
                                           "65 21 B3 4E" + lineSeparator() +
                                           "C9 03 EA 92" + lineSeparator() +
                                           "86 50 65 72" + lineSeparator() +
                                           "73 6F 6E 88" + lineSeparator() +
                                           "45 6D 70 6C" + lineSeparator() +
                                           "6F 79 65 65" + lineSeparator() +
                                           "A2 84 6E 61" + lineSeparator() +
                                           "6D 65 83 42" + lineSeparator() +
                                           "6F 62 83 61" + lineSeparator() +
                                           "67 65 2C 91" + lineSeparator() +
                                           "B3 72 0C 85" + lineSeparator() +
                                           "4B 4E 4F 57" + lineSeparator() +
                                           "53 A1 85 73" + lineSeparator() +
                                           "69 6E 63 65" + lineSeparator() +
                                           "C9 07 CF 92" + lineSeparator() +
                                           "01 01" );
    }

    @Parameterized.Parameter(0)
    public DocStruct struct;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> documentedTypeMapping()
    {
        Collection<Object[]> mappings = new ArrayList<>();

        // Load the documented mappings
        for ( DocStruct struct : DocsRepository.docs().read(
                "dev/serialization.asciidoc",
                "code[data-lang=\"bolt_value_struct\"]",
                DocStruct.struct_definition ) )
        {
            mappings.add( new Object[]{struct} );
        }

        return mappings;
    }

    @Test
    public void ensureSerializingMessagesAsDocumentedWorks() throws Throwable
    {
        // Given
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = new Neo4jPack.Packer( output );

        // When I pack a structure according to the documentation
        packer.packStructHeader( struct.size(), struct.signature() );
        for ( DocStruct.Field field : struct )
        {
            packValueOf( field, packer );
        }
        packer.flush();

        // Then unpack again into a regular object
        byte[] bytes = output.bytes();
        String hex = HexPrinter.hex( bytes, 4, lineSeparator() );

        // Then it should get interpreted as the documented structure
        assertEquals( expectedSerialization.get( struct.name() ), hex );
    }

    private void packValueOf( DocStruct.Field field, Neo4jPack.Packer packer ) throws IOException
    {
        String type = field.type();
        if( type.equalsIgnoreCase( "Integer" ))
        {
            packer.pack( 1337 );
        }
        else if ( type.equalsIgnoreCase( "String" ) )
        {
            packer.pack( field.exampleValueOr( "12345" ) );
        }
        else if ( type.startsWith( "Map" ) )
        {
            packer.packMapHeader( 1 );
            packer.pack( "k" );
            packer.pack( 12345 );
        }
        else if ( type.startsWith( "List<String>" ) )
        {
            packer.packListHeader( 2 );
            packer.pack( "Banana" );
            packer.pack( "Person" );
        }
        else if ( type.startsWith( "List<Node>" ) )
        {
            packer.packListHeader( 2 );
            ValueNode.pack( packer, ALICE );
            ValueNode.pack( packer, BOB );
        }
        else if ( type.startsWith( "List<UnboundRelationship>" ) )
        {
            packer.packListHeader( 1 );
            ValueUnboundRelationship.pack( packer,
                    ValueUnboundRelationship.unbind( ALICE_KNOWS_BOB ) );
        }
        else if ( type.startsWith( "List<Integer>" ) )
        {
            packer.packListHeader( 2 );
            packer.pack( 1 );
            packer.pack( 1 );
        }
        else
        {
            throw new RuntimeException( "Unknown type: " + type );
        }
    }

}
