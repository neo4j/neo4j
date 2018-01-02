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
package org.neo4j.server.rest.repr;

import org.junit.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.formats.JsonFormat;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.map;

public class MapRepresentationTest
{
    @Test
    public void shouldSerializeMapWithSimpleTypes() throws Exception
    {
        MapRepresentation rep = new MapRepresentation( map( "nulls", null, "strings", "a string", "numbers", 42,
                "booleans", true ) );
        OutputFormat format = new OutputFormat( new JsonFormat(), new URI( "http://localhost/" ), null );

        String serializedMap = format.assemble( rep );

        Map<String, Object> map = JsonHelper.jsonToMap( serializedMap );
        assertThat( map.get( "nulls" ), is( nullValue() ) );
        assertThat( (String) map.get( "strings" ), is( "a string" ) );
        assertThat( (Integer) map.get( "numbers" ), is( 42 ) );
        assertThat( (Boolean) map.get( "booleans" ), is( true ) );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSerializeMapWithArrayTypes() throws Exception
    {
        MapRepresentation rep = new MapRepresentation( map(
                "strings", new String[]{"a string", "another string"},
                "numbers", new int[]{42, 87},
                "booleans", new boolean[]{true, false},
                "Booleans", new Boolean[]{TRUE, FALSE}
        ) );
        OutputFormat format = new OutputFormat( new JsonFormat(), new URI( "http://localhost/" ), null );

        String serializedMap = format.assemble( rep );

        Map<String, Object> map = JsonHelper.jsonToMap( serializedMap );
        assertThat( (List<String>) map.get( "strings" ), is( asList( "a string", "another string" ) ) );
        assertThat( (List<Integer>) map.get( "numbers" ), is( asList( 42, 87 ) ) );
        assertThat( (List<Boolean>) map.get( "booleans" ), is( asList( true, false ) ) );
        assertThat( (List<Boolean>) map.get( "Booleans" ), is( asList( true, false ) ) );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSerializeMapWithListsOfSimpleTypes() throws Exception
    {
        MapRepresentation rep = new MapRepresentation( map( "lists of nulls", asList( null, null ),
                "lists of strings", asList( "a string", "another string" ), "lists of numbers", asList( 23, 87, 42 ),
                "lists of booleans", asList( true, false, true ) ) );
        OutputFormat format = new OutputFormat( new JsonFormat(), new URI( "http://localhost/" ), null );

        String serializedMap = format.assemble( rep );

        Map<String, Object> map = JsonHelper.jsonToMap( serializedMap );
        assertThat( (List<Object>) map.get( "lists of nulls" ), is( asList( null, null ) ) );
        assertThat( (List<String>) map.get( "lists of strings" ), is( asList( "a string", "another string" ) ) );
        assertThat( (List<Integer>) map.get( "lists of numbers" ), is( asList( 23, 87, 42 ) ) );
        assertThat( (List<Boolean>) map.get( "lists of booleans" ), is( asList( true, false, true ) ) );
    }

    @Test
    public void shouldSerializeMapWithMapsOfSimpleTypes() throws Exception
    {
        MapRepresentation rep = new MapRepresentation( map( "maps with nulls", map( "nulls", null ),
                "maps with strings", map( "strings", "a string" ),
                "maps with numbers", map( "numbers", 42 ),
                "maps with booleans", map( "booleans", true ) ) );
        OutputFormat format = new OutputFormat( new JsonFormat(), new URI( "http://localhost/" ), null );

        String serializedMap = format.assemble( rep );

        Map<String, Object> map = JsonHelper.jsonToMap( serializedMap );
        assertThat( ((Map) map.get( "maps with nulls" )).get( "nulls" ), is( nullValue() ) );
        assertThat( (String) ((Map) map.get( "maps with strings" )).get( "strings" ), is( "a string" ) );
        assertThat( (Integer) ((Map) map.get( "maps with numbers" )).get( "numbers" ), is( 42 ) );
        assertThat( (Boolean) ((Map) map.get( "maps with booleans" )).get( "booleans" ), is( true ) );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSerializeArbitrarilyNestedMapsAndLists() throws Exception
    {
        MapRepresentation rep = new MapRepresentation(
                map(
                        "a map with a list in it", map( "a list", asList( 42, 87 ) ),
                        "a list with a map in it", asList( map( "foo", "bar", "baz", false ) )
                )
        );
        OutputFormat format = new OutputFormat( new JsonFormat(), new URI( "http://localhost/" ), null );

        String serializedMap = format.assemble( rep );

        Map<String, Object> map = JsonHelper.jsonToMap( serializedMap );
        assertThat( (List<Integer>) ((Map) map.get( "a map with a list in it" )).get( "a list" ), is( asList( 42,
                87 ) ) );
        assertThat( (String) ((Map) ((List) map.get( "a list with a map in it" )).get( 0 )).get( "foo" ), is( "bar" ) );
        assertThat( (Boolean) ((Map) ((List) map.get( "a list with a map in it" )).get( 0 )).get( "baz" ),
                is( false ) );
    }

    @Test
    public void shouldSerializeMapsWithNullKeys() throws Exception
    {
        Object[] values = {null,
                "string",
                42,
                true,
                new String[]{"a string", "another string"},
                new int[]{42, 87},
                new boolean[]{true, false},
                asList( true, false, true ),
                map( "numbers", 42, null, "something" ),
                map( "a list", asList( 42, 87 ), null, asList( "a", "b" ) ),
                asList( map( "foo", "bar", null, false ) )};

        for ( Object value : values )
        {
            MapRepresentation rep = new MapRepresentation( map( (Object) null, value ) );
            OutputFormat format = new OutputFormat( new JsonFormat(), new URI( "http://localhost/" ), null );

            String serializedMap = format.assemble( rep );

            Map<String,Object> map = JsonHelper.jsonToMap( serializedMap );

            assertEquals( 1, map.size() );
            Object actual = map.get( "null" );
            if ( value == null )
            {
                assertNull( actual );
            }
            else
            {
                assertNotNull( actual );
            }
        }
    }
}
