/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.server.web;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.logging.Log;

public class HttpHeaderUtils
{

    public static final String MAX_EXECUTION_TIME_HEADER = "max-execution-time";

    public static final Map<String,String> CHARSET =
            Collections.singletonMap( "charset", StandardCharsets.UTF_8.name() );

    private HttpHeaderUtils()
    {
    }

    public static MediaType mediaTypeWithCharsetUtf8( String mediaType )
    {
        return new MediaType( mediaType, null, CHARSET );
    }

    public static MediaType mediaTypeWithCharsetUtf8( MediaType mediaType )
    {
        Map<String,String> parameters = mediaType.getParameters();
        if ( parameters.isEmpty() )
        {
            return new MediaType( mediaType.getType(), mediaType.getSubtype(), CHARSET );
        }
        if ( parameters.containsKey( "charset" ) )
        {
            return mediaType;
        }
        Map<String,String> paramsWithCharset = new HashMap<>( parameters );
        paramsWithCharset.putAll( CHARSET );
        return new MediaType( mediaType.getType(), mediaType.getSubtype(), paramsWithCharset );
    }

    /**
     * Retrieve custom transaction timeout in milliseconds from numeric {@link #MAX_EXECUTION_TIME_HEADER} request
     * header.
     * If header is not set returns -1.
     * @param request http request
     * @param errorLog errors log for header parsing errors
     * @return custom timeout if header set, -1 otherwise or when value is not a valid number.
     */
    public static long getTransactionTimeout( HttpServletRequest request, Log errorLog )
    {
        String headerValue = request.getHeader( MAX_EXECUTION_TIME_HEADER );
        if ( headerValue != null )
        {
            try
            {
                return Long.parseLong( headerValue );
            }
            catch ( NumberFormatException e )
            {
                errorLog.error( String.format( "Fail to parse `%s` header with value: '%s'. Should be a positive number.",
                        MAX_EXECUTION_TIME_HEADER, headerValue), e );
            }
        }
        return GraphDatabaseSettings.UNSPECIFIED_TIMEOUT;
    }

    /**
     * Validates given HTTP header name. Does not allow blank names and names with control characters, like '\n' (LF) and '\r' (CR).
     * Can be used to detect and neutralize CRLF in HTTP headers.
     *
     * @param name the HTTP header name, like 'Accept' or 'Content-Type'.
     * @return {@code true} when given name represents a valid HTTP header, {@code false} otherwise.
     */
    public static boolean isValidHttpHeaderName( String name )
    {
        if ( name == null || name.length() == 0 )
        {
            return false;
        }
        boolean isBlank = true;
        for ( int i = 0; i < name.length(); i++ )
        {
            char c = name.charAt( i );
            if ( Character.isISOControl( c ) )
            {
                return false;
            }
            if ( !Character.isWhitespace( c ) )
            {
                isBlank = false;
            }
        }
        return !isBlank;
    }
}
