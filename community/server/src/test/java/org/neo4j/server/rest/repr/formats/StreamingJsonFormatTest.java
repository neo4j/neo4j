/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.rest.repr.formats;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;
import org.neo4j.server.rest.repr.NodeRepresentation;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.ValueRepresentation;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamingJsonFormatTest
{
    private OutputFormat json;
    private ByteArrayOutputStream stream;

    @Before
    public void createOutputFormat() throws Exception
    {
        stream = new ByteArrayOutputStream();
        json = new OutputFormat( new StreamingJsonFormat().writeTo(stream).usePrettyPrinter(), new URI( "http://localhost/" ), null );
    }

    @Test
    public void canFormatNode() throws Exception
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try ( Transaction transaction = db.beginTx() )
        {
            final Node n = db.createNode();
            json.assemble( new NodeRepresentation( n ) );
        }
        finally
        {
            db.shutdown();
        }
        assertTrue( stream.toString().contains( "\"self\" : \"http://localhost/node/0\"," ) );
    }

    @Test
    public void canFormatString() throws Exception
    {
        json.assemble( ValueRepresentation.string( "expected value" ) );
        assertEquals( stream.toString(), "\"expected value\"" );
    }

    @Test
    public void canFormatListOfStrings() throws Exception
    {
        json.assemble( ListRepresentation.strings( "hello", "world" ) );
        String expectedString = JsonHelper.createJsonFrom( Arrays.asList( "hello", "world" ) );
        assertEquals( expectedString, stream.toString() );
    }

    @Test
    public void canFormatInteger() throws Exception
    {
        json.assemble( ValueRepresentation.number( 10 ) );
        assertEquals( "10", stream.toString() );
    }

    @Test
    public void canFormatEmptyObject() throws Exception
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
    public void canFormatObjectWithStringField() throws Exception
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
    public void canFormatObjectWithUriField() throws Exception
    {
        json.assemble( new MappingRepresentation( "uri" )
        {
            @Override
            protected void serialize( MappingSerializer serializer )
            {
                serializer.putUri( "URL", "subpath" );
            }
        } );

        assertEquals( JsonHelper.createJsonFrom( Collections.singletonMap( "URL", "http://localhost/subpath" ) ),
                stream.toString() );
    }

    @Test
    public void canFormatObjectWithNestedObject() throws Exception
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
