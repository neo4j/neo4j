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
package org.neo4j.server.rest.domain;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Map;

import org.neo4j.server.rest.web.PropertyValueException;

public class JsonHelper
{
    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private JsonHelper()
    {
    }

    public static JsonNode jsonNode( String json ) throws JsonParseException
    {
        try
        {
            return OBJECT_MAPPER.readTree( json );
        }
        catch ( IOException e )
        {
            throw new JsonParseException( e );
        }
    }

    @SuppressWarnings( "unchecked" )
    public static Map<String, Object> jsonToMap( String json ) throws JsonParseException
    {
        return (Map<String, Object>) readJson( json );
    }

    public static Object readJson( String json ) throws JsonParseException
    {
        try
        {
            return OBJECT_MAPPER.readValue( json, Object.class );
        }
        catch ( com.fasterxml.jackson.core.JsonParseException e )
        {
            String message = e.getMessage().split( "\\r?\\n" )[0];
            JsonLocation location = e.getLocation();
            throw new JsonParseException( String.format( "%s [line: %d, column: %d]", message, location.getLineNr(),
                    location.getColumnNr() ), e );
        }
        catch ( IOException e )
        {
            throw new JsonParseException( e );
        }
    }

    public static Object assertSupportedPropertyValue( Object jsonObject ) throws PropertyValueException
    {
        if ( jsonObject instanceof Collection<?> )
        {
            return jsonObject;
        }
        if ( jsonObject == null )
        {
            throw new PropertyValueException( "null value not supported" );
        }
        if ( !(jsonObject instanceof String ||
               jsonObject instanceof Number ||
               jsonObject instanceof Boolean) )
        {
            throw new PropertyValueException(
                    "Unsupported value type " + jsonObject.getClass() + '.'
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
            JsonGenerator generator = OBJECT_MAPPER.getFactory()
                .createGenerator( writer )
                .useDefaultPrettyPrinter();
            writeValue( generator, data );

            return writer.getBuffer().toString();
        }
        catch ( IOException e )
        {
            throw new JsonBuildRuntimeException( e );
        }
    }

    public static void writeValue( JsonGenerator jgen, Object value ) throws IOException
    {
        OBJECT_MAPPER.writeValue( jgen, value );
    }
}
