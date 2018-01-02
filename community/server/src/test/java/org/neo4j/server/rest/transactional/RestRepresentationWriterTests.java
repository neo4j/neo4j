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
package org.neo4j.server.rest.transactional;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.neo4j.server.rest.domain.JsonParseException;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.domain.JsonHelper.jsonNode;

public class RestRepresentationWriterTests
{
    @Test
    public void shouldWriteNestedMaps() throws Exception
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonGenerator json = new JsonFactory( new Neo4jJsonCodec() ).createJsonGenerator( out );

        JsonNode rest = serialize( out, json, new RestRepresentationWriter( URI.create( "localhost" ) ) );

        MatcherAssert.assertThat( rest.size(), equalTo( 1 ) );

        JsonNode firstCell = rest.get( 0 );
        MatcherAssert.assertThat( firstCell.get( "one" ).get( "two" ).size(), is( 2 ) );
        MatcherAssert.assertThat( firstCell.get( "one" ).get( "two" ).get( 0 ).asBoolean(), is( true ) );
        MatcherAssert.assertThat( firstCell.get( "one" ).get( "two" ).get( 1 ).get( "three" ).asInt(), is( 42 ) );
    }

    private JsonNode serialize( ByteArrayOutputStream out, JsonGenerator json, ResultDataContentWriter
            resultDataContentWriter ) throws IOException, JsonParseException
    {
        json.writeStartObject();
        // RETURN {one:{two:[true, {three: 42}]}}
        resultDataContentWriter.write( json, asList( "the column" ), new MapRow(
                map( "the column", map( "one", map( "two", asList( true, map( "three", 42 ) ) ) ) ) ) );
        json.writeEndObject();
        json.flush();
        json.close();

        String jsonAsString = out.toString();
        return jsonNode( jsonAsString ).get( "rest" );
    }
}
