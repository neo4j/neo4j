/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.server.rest.repr.formats;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.http.cypher.format.output.json.StreamingJsonFormat;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;
import org.neo4j.server.rest.repr.NodeRepresentation;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.ValueRepresentation;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class StreamingJsonFormatTest
{
    private OutputFormat json;
    private ByteArrayOutputStream stream;

    @BeforeEach
    void createOutputFormat() throws Exception
    {
        stream = new ByteArrayOutputStream();
        json = new OutputFormat( new StreamingJsonFormat().writeTo(stream).usePrettyPrinter(), new URI( "http://localhost/" ) );
    }

    @Test
    void canFormatNode()
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction transaction = db.beginTx() )
        {
            final Node n = transaction.createNode();
            json.assemble( new NodeRepresentation( n ) );
        }
        finally
        {
            managementService.shutdown();
        }
        assertTrue( stream.toString().contains( "\"self\" : \"http://localhost/node/0\"," ) );
    }
    @Test
    void canFormatString()
    {
        json.assemble( ValueRepresentation.string( "expected value" ) );
        assertEquals( stream.toString(), "\"expected value\"" );
    }

    @Test
    void canFormatListOfStrings()
    {
        json.assemble( ListRepresentation.strings( "hello", "world" ) );
        String expectedString = JsonHelper.createJsonFrom( Arrays.asList( "hello", "world" ) );
        assertEquals( expectedString, stream.toString() );
    }

    @Test
    void canFormatInteger()
    {
        json.assemble( ValueRepresentation.number( 10 ) );
        assertEquals( "10", stream.toString() );
    }

    @Test
    void canFormatEmptyObject()
    {
        json.assemble( new MappingRepresentation( "empty" )
        {
            @Override
            protected void serialize( MappingSerializer serializer )
            {
            }
        } );
        assertEquals( JsonHelper.createJsonFrom( Collections.emptyMap() ), stream.toString() );
    }

    @Test
    void canFormatObjectWithStringField()
    {
        json.assemble( new MappingRepresentation( "string" )
        {
            @Override
            protected void serialize( MappingSerializer serializer )
            {
                serializer.putString( "key", "expected string" );
            }
        } );
        assertEquals( JsonHelper.createJsonFrom( Collections.singletonMap( "key", "expected string" ) ), stream.toString() );
    }

    @Test
    void canFormatObjectWithUriField()
    {
        json.assemble( new MappingRepresentation( "uri" )
        {
            @Override
            protected void serialize( MappingSerializer serializer )
            {
                serializer.putRelativeUri( "URL", "subpath" );
            }
        } );

        assertEquals( JsonHelper.createJsonFrom( Collections.singletonMap( "URL", "http://localhost/subpath" ) ),
                stream.toString() );
    }

    @Test
    void canFormatObjectWithNestedObject()
    {
        json.assemble( new MappingRepresentation( "nesting" )
        {
            @Override
            protected void serialize( MappingSerializer serializer )
            {
                serializer.putMapping( "nested", new MappingRepresentation( "data" )
                {
                    @Override
                    protected void serialize( MappingSerializer nested )
                    {
                        nested.putString( "data", "expected data" );
                    }
                } );
            }
        } );
        assertEquals(
                JsonHelper.createJsonFrom( Collections.singletonMap( "nested",
                        Collections.singletonMap( "data", "expected data" ) ) ), stream.toString() );
    }
}
