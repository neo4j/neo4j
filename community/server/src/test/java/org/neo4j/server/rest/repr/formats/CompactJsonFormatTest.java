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

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.ValueRepresentation;

public class CompactJsonFormatTest
{
    private OutputFormat json;

    @Before
    public void createOutputFormat() throws Exception
    {
        json = new OutputFormat( new CompactJsonFormat(), new URI( "http://localhost/" ), null );
    }

    @Test
    public void canFormatString() throws Exception
    {
        String entity = json.assemble( ValueRepresentation.string( "expected value" ) );
        assertEquals( entity, "\"expected value\"" );
    }

    @Test
    public void canFormatListOfStrings() throws Exception
    {
        String entity = json.assemble( ListRepresentation.strings( "hello", "world" ) );
        String expectedString = JsonHelper.createJsonFrom( Arrays.asList( "hello", "world" ) );
        assertEquals( expectedString, entity );
    }

    @Test
    public void canFormatInteger() throws Exception
    {
        String entity = json.assemble( ValueRepresentation.number( 10 ) );
        assertEquals( "10", entity );
    }

    @Test
    public void canFormatObjectWithStringField() throws Exception
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

}
