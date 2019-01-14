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

import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationType;
import org.neo4j.server.rest.repr.ServerListRepresentation;
import org.neo4j.server.rest.repr.ValueRepresentation;

import static org.junit.Assert.assertEquals;

public class JsonFormatTest
{
    private OutputFormat json;

    @Before
    public void createOutputFormat() throws Exception
    {
        json = new OutputFormat( new JsonFormat(), new URI( "http://localhost/" ), null );
    }

    @Test
    public void canFormatString()
    {
        String entity = json.assemble( ValueRepresentation.string( "expected value" ) );
        assertEquals( entity, "\"expected value\"" );
    }

    @Test
    public void canFormatListOfStrings()
    {
        String entity = json.assemble( ListRepresentation.strings( "hello", "world" ) );
        String expectedString = JsonHelper.createJsonFrom( Arrays.asList( "hello", "world" ) );
        assertEquals( expectedString, entity );
    }

    @Test
    public void canFormatInteger()
    {
        String entity = json.assemble( ValueRepresentation.number( 10 ) );
        assertEquals( "10", entity );
    }

    @Test
    public void canFormatEmptyObject()
    {
        String entity = json.assemble( new MappingRepresentation( "empty" )
        {
            @Override
            protected void serialize( MappingSerializer serializer )
            {
            }
        } );
        assertEquals( JsonHelper.createJsonFrom( Collections.emptyMap() ), entity );
    }

    @Test
    public void canFormatObjectWithStringField()
    {
        String entity = json.assemble( new MappingRepresentation( "string" )
        {
            @Override
            protected void serialize( MappingSerializer serializer )
            {
                serializer.putString( "key", "expected string" );
            }
        } );
        assertEquals( JsonHelper.createJsonFrom( Collections.singletonMap( "key", "expected string" ) ), entity );
    }

    @Test
    public void canFormatObjectWithUriField()
    {
        String entity = json.assemble( new MappingRepresentation( "uri" )
        {
            @Override
            protected void serialize( MappingSerializer serializer )
            {
                serializer.putRelativeUri( "URL", "subpath" );
            }
        } );

        assertEquals( JsonHelper.createJsonFrom( Collections.singletonMap( "URL", "http://localhost/subpath" ) ),
                entity );
    }

    @Test
    public void canFormatObjectWithNestedObject()
    {
        String entity = json.assemble( new MappingRepresentation( "nesting" )
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
                        Collections.singletonMap( "data", "expected data" ) ) ), entity );
    }

    @Test
    public void canFormatNestedMapsAndLists() throws Exception
    {
        String entity = json.assemble( new MappingRepresentation( "test" )
        {
            @Override
            protected void serialize( MappingSerializer serializer )
            {
                ArrayList<Representation> maps = new ArrayList<>();
                maps.add( new MappingRepresentation( "map" )
                {

                    @Override
                    protected void serialize( MappingSerializer serializer )
                    {
                        serializer.putString( "foo", "bar" );

                    }
                } );
                serializer.putList( "foo", new ServerListRepresentation( RepresentationType.MAP, maps ) );
            }
        } );

        assertEquals( "bar",((Map)((List) JsonHelper.jsonToMap(entity).get("foo")).get(0)).get("foo") );
    }
}
