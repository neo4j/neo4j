/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;

class JsonFormatTest
{
    private OutputFormat json;

    @BeforeEach
    void createOutputFormat() throws Exception
    {
        json = new OutputFormat( new JsonFormat(), new URI( "http://localhost/" ) );
    }

    @Test
    void canFormatString()
    {
        String entity = json.assemble( ValueRepresentation.string( "expected value" ) );
        assertEquals( "\"expected value\"", entity );
    }

    @Test
    void canFormatListOfStrings()
    {
        String entity = json.assemble( ListRepresentation.strings( "hello", "world" ) );
        String expectedString = createJsonFrom( Arrays.asList( "hello", "world" ) );
        assertEquals( expectedString, entity );
    }

    @Test
    void canFormatInteger()
    {
        String entity = json.assemble( ValueRepresentation.number( 10 ) );
        assertEquals( "10", entity );
    }

    @Test
    void canFormatEmptyObject()
    {
        String entity = json.assemble( new MappingRepresentation( "empty" )
        {
            @Override
            protected void serialize( MappingSerializer serializer )
            {
            }
        } );
        assertEquals( createJsonFrom( Collections.emptyMap() ), entity );
    }

    @Test
    void canFormatObjectWithStringField()
    {
        String entity = json.assemble( new MappingRepresentation( "string" )
        {
            @Override
            protected void serialize( MappingSerializer serializer )
            {
                serializer.putString( "key", "expected string" );
            }
        } );
        assertEquals( createJsonFrom( singletonMap( "key", "expected string" ) ), entity );
    }

    @Test
    void canFormatObjectWithUriField()
    {
        String entity = json.assemble( new MappingRepresentation( "uri" )
        {
            @Override
            protected void serialize( MappingSerializer serializer )
            {
                serializer.putRelativeUri( "URL", "subpath" );
            }
        } );

        assertEquals( createJsonFrom( singletonMap( "URL", "http://localhost/subpath" ) ), entity );
    }

    @Test
    void canFormatObjectWithNestedObject()
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
                createJsonFrom( singletonMap( "nested",
                        singletonMap( "data", "expected data" ) ) ), entity );
    }

    @Test
    void canFormatNestedMapsAndLists() throws Exception
    {
        String entity = json.assemble( new MappingRepresentation( "test" )
        {
            @Override
            protected void serialize( MappingSerializer serializer )
            {
                List<Representation> maps = new ArrayList<>();
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

        assertEquals( "bar", ((Map) ((List) JsonHelper.jsonToMap( entity ).get( "foo" )).get( 0 )).get( "foo" ) );
    }
}
