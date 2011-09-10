/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.rest.domain;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.server.rest.web.PropertyValueException;

public class JsonHelper
{

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @SuppressWarnings( "unchecked" )
    public static Map<String, Object> jsonToMap( String json ) throws JsonParseException
    {
        return (Map<String, Object>) readJson( json );
    }

    @SuppressWarnings( "unchecked" )
    public static List<Map<String, Object>> jsonToList( String json ) throws JsonParseException
    {
        return (List<Map<String, Object>>) readJson( json );
    }
    
    public static Object readJson( String json ) throws JsonParseException
    {
        ObjectMapper mapper = new ObjectMapper();
        try
        {
            return mapper.readValue( json, Object.class );
        }
        catch ( IOException e )
        {
            throw new JsonParseException( e );
        }
    }

    public static Object jsonToSingleValue( String json ) throws org.neo4j.server.rest.web.PropertyValueException
    {
        Object jsonObject = readJson( json );
        return jsonObject instanceof Collection<?> ? jsonObject : assertSupportedPropertyValue( jsonObject );
    }

    private static Object assertSupportedPropertyValue( Object jsonObject ) throws PropertyValueException
    {
        if ( jsonObject == null )
        {
            throw new org.neo4j.server.rest.web.PropertyValueException( "null value not supported" );

        }

        if ( jsonObject instanceof String )
        {
        }
        else if ( jsonObject instanceof Number )
        {
        }
        else if ( jsonObject instanceof Boolean )
        {
        }
        else
        {
            throw new org.neo4j.server.rest.web.PropertyValueException(
                    "Unsupported value type " + jsonObject.getClass() + "."
                            + " Supported value types are all java primitives (byte, char, short, int, "
                            + "long, float, double) and String, as well as arrays of all those types" );
        }
        return jsonObject;
    }

    public static String createJsonFrom( Object data ) throws JsonBuildRuntimeException
    {
        try
        {
            StringWriter writer = new StringWriter();
            JsonGenerator generator = OBJECT_MAPPER.getJsonFactory()
                    .createJsonGenerator( writer )
                    .useDefaultPrettyPrinter();
            OBJECT_MAPPER.writeValue( generator, data );
            writer.close();
            return writer.getBuffer()
                    .toString();
        }
        catch ( IOException e )
        {
            throw new JsonBuildRuntimeException( e );
        }
    }
}
