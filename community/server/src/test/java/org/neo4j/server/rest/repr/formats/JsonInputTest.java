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

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

public class JsonInputTest
{
    private final JsonFormat input = new JsonFormat();

    @Test
    public void canReadEmptyMap() throws Exception
    {
        Map<String, Object> map = input.readMap( "{}" );
        assertNotNull( map );
        assertTrue( "map is not empty", map.isEmpty() );
    }

    @Test
    public void canReadMapWithTwoValues() throws Exception
    {
        Map<String, Object> map = input.readMap( "{\"key1\":\"value1\",     \"key2\":\"value11\"}" );
        assertNotNull( map );
        assertThat( map, hasEntry( "key1", (Object) "value1" ) );
        assertThat( map, hasEntry( "key2", (Object) "value11" ) );
        assertTrue( "map contained extra values", map.size() == 2 );
    }

    @Test
    public void canReadMapWithNestedMap() throws Exception
    {
        Map<String, Object> map = input.readMap( "{\"nested\": {\"key\": \"valuable\"}}" );
        assertNotNull( map );
        assertThat( map, hasKey( "nested" ) );
        assertTrue( "map contained extra values", map.size() == 1 );
        Object nested = map.get( "nested" );
        assertThat( nested, instanceOf( Map.class ) );
        @SuppressWarnings( "unchecked" ) Map<String, String> nestedMap = (Map<String, String>) nested;
        assertThat( nestedMap, hasEntry( "key", "valuable" ) );
    }
    
    @Test
    public void canReadStringWithLineBreaks() throws Exception
    {
        Map<String, Object> map = input.readMap( "{\"key\": \"v1\\nv2\"}" );
        assertNotNull( map );
        assertEquals( map.get( "key" ), "v1\nv2"  );
    }
}
