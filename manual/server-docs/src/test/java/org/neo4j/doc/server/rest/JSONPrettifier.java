/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.doc.server.rest;

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
