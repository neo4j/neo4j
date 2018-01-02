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
package org.neo4j.server.rest;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

/*
 * Naive implementation of a JSON prettifier.
 */
public class JSONPrettifier
{
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting()
            .create();
    private static final JsonParser JSON_PARSER = new JsonParser();
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ObjectWriter WRITER = MAPPER.writerWithDefaultPrettyPrinter();

    public static String parse( final String json )
    {
        if ( json == null )
        {
            return "";
        }

        String result = json;

        try
        {
            if ( json.contains( "\"exception\"" ) )
            {
                // the gson renderer is much better for stacktraces
                result = gsonPrettyPrint( json );
            }
            else
            {
                result = jacksonPrettyPrint( json );
            }
        }
        catch ( Exception e )
        {
            /*
            * Enable the output to see where exceptions happen.
            * We need to be able to tell the rest docs tools to expect
            * a json parsing error from here, then we can simply throw an exception instead.
            * (we have tests sending in broken json to test the response)
            */
            // System.out.println( "***************************************" );
            // System.out.println( json );
            // System.out.println( "***************************************" );
        }
        return result;
    }

    private static String gsonPrettyPrint( final String json ) throws Exception
    {
        JsonElement element = JSON_PARSER.parse( json );
        return GSON.toJson( element );
    }

    private static String jacksonPrettyPrint( final String json )
            throws Exception
    {
        Object myObject = MAPPER.readValue( json, Object.class );
        return WRITER.writeValueAsString( myObject );
    }
}
