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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import org.neo4j.server.rest.repr.DefaultFormat;
import org.neo4j.server.rest.repr.MediaTypeNotSupportedException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultFormatTest
{
    private DefaultFormat input;

    @BeforeEach
    public void setUp()
    {
        JsonFormat inner = new JsonFormat();
        List<MediaType> supported = new ArrayList<>();
        MediaType requested = MediaType.APPLICATION_JSON_TYPE;
        input = new DefaultFormat( inner, supported, requested );
    }

    @Test
    public void canReadEmptyMap() throws Exception
    {
        Map<String, Object> map = input.readMap( "{}" );
        assertNotNull( map );
        assertTrue( map.isEmpty(), "map is not empty" );
    }

    @Test
    public void canReadMapWithTwoValues() throws Exception
    {
        Map<String, Object> map = input.readMap( "{\"key1\":\"value1\",     \"key2\":\"value11\"}" );
        assertNotNull( map );
        assertThat( map ).containsEntry( "key1", "value1" );
        assertThat( map ).containsEntry( "key2", "value11" );
        assertEquals( 2, map.size(), "map contained extra values" );
    }

    @Test
    public void canReadMapWithNestedMap() throws Exception
    {
        Map<String, Object> map = input.readMap( "{\"nested\": {\"key\": \"valuable\"}}" );
        assertNotNull( map );
        assertThat( map ).containsKey( "nested" );
        assertEquals( 1, map.size(), "map contained extra values" );
        Object nested = map.get( "nested" );
        assertThat( nested ).isInstanceOf( Map.class );
        @SuppressWarnings( "unchecked" )
        Map<String, String> nestedMap = (Map<String, String>) nested;
        assertThat( nestedMap ).containsEntry( "key", "valuable" );
    }

    @Test
    public void failsWithTheCorrectExceptionWhenGettingTheWrongInput()
    {
        assertThrows( MediaTypeNotSupportedException.class, () -> input.readValue( "<xml />" ) );
    }

    @Test
    public void failsWithTheCorrectExceptionWhenGettingTheWrongInput2()
    {
        assertThrows( MediaTypeNotSupportedException.class, () -> input.readMap( "<xml />" ) );
    }

    @Test
    public void failsWithTheCorrectExceptionWhenGettingTheWrongInput3()
    {
        assertThrows( MediaTypeNotSupportedException.class, () -> input.readUri( "<xml />" ) );
    }
}
