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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.ndp.messaging.v1.Neo4jPack;
import org.neo4j.ndp.messaging.v1.infrastructure.ValueNode;
import org.neo4j.ndp.messaging.v1.infrastructure.ValuePath;
import org.neo4j.ndp.messaging.v1.infrastructure.ValueRelationship;
import org.neo4j.ndp.messaging.v1.infrastructure.ValueUnboundRelationship;
import org.neo4j.packstream.PackedInputArray;
import org.neo4j.packstream.PackedOutputArray;

import static org.junit.Assert.assertEquals;

import static org.neo4j.ndp.docs.v1.DocStruct.struct_definition;
import static org.neo4j.ndp.docs.v1.DocsRepository.docs;
import static org.neo4j.ndp.messaging.v1.example.Nodes.ALICE;
import static org.neo4j.ndp.messaging.v1.example.Nodes.BOB;
import static org.neo4j.ndp.messaging.v1.example.Relationships.ALICE_KNOWS_BOB;

/**
 * This tests that message data structures look the way we say they do
 */
@RunWith(Parameterized.class)
public class NDPValueStructsDocTest
{

    // This lookup is used to check we hydrate structures to the correct types
    public static Map<String, Class> expectedClass = new HashMap<>();

    static
    {
        expectedClass.put( "Node", ValueNode.class );
        expectedClass.put( "Relationship", ValueRelationship.class );
        expectedClass.put( "UnboundRelationship", ValueUnboundRelationship.class );
        expectedClass.put( "Path", ValuePath.class );
    }

    @Parameterized.Parameter(0)
    public DocStruct struct;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> documentedTypeMapping()
    {
        Collection<Object[]> mappings = new ArrayList<>();

        // Load the documented mappings
        for ( DocStruct struct : docs().read(
                "dev/serialization.asciidoc",
                "code[data-lang=\"ndp_value_struct\"]",
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
        //System.out.println("Serialized bytes:\n" + HexPrinter.hex( bytes ));
        PackedInputArray input = new PackedInputArray( bytes );
        Neo4jPack.Unpacker unpacker = new Neo4jPack.Unpacker( input );
        Object unpacked = unpacker.unpack();

        // Then it should get interpreted as the documented structure
        assertEquals( expectedClass.get( struct.name() ), unpacked.getClass() );

        // Hello, future traveler. The assertion above is not strictly necessary. What we're trying
        // to do here is simply to ensure that the documented structure is what we get back out
        // when we deserialize, the name of the class does not strictly have to map to the name in
        // the docs, if that is causing you trouble.
    }

    private void packValueOf( DocStruct.Field field, Neo4jPack.Packer packer ) throws IOException
    {
        String name = field.name().toLowerCase();
        String type = field.type();
        if ( name.endsWith( "nodeidentity" ) )
        {
            packer.pack( field.exampleValueOr( "node/12345" ) );
        }
        else if ( name.endsWith( "relidentity" ) )
        {
            packer.pack( field.exampleValueOr( "rel/12345" ) );
        }
        else if ( type.equalsIgnoreCase( "Text" ) )
        {
            packer.pack( field.exampleValueOr( "12345" ) );
        }
        else if ( type.startsWith( "Map" ) )
        {
            packer.packMapHeader( 1 );
            packer.pack( "k" );
            packer.pack( 12345 );
        }
        else if ( type.startsWith( "List<Text>" ) )
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
