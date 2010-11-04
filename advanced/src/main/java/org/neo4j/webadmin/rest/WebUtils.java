/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.webadmin.rest;

import java.io.UnsupportedEncodingException;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.neo4j.rest.domain.JsonRenderers;
import org.neo4j.rest.domain.Renderer;
import org.neo4j.webadmin.domain.ExceptionRepresentation;

/**
 * Static helpers for web services. Mostly copied from neo4-rest.
 * 
 */
public class WebUtils
{

    public static final String UTF8 = "UTF-8";
    public static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";

    /**
     * Add necessary headers and ensure content is UTF-8 encoded.
     * 
     * @param builder
     * @return builder with content length and utf-8 headers.
     */
    public static ResponseBuilder addHeaders( ResponseBuilder builder )
    {
        String entity = (String) builder.clone().build().getEntity();

        // Check if response contains any data
        if ( entity != null )
        {
            byte[] entityAsBytes;
            try
            {
                entityAsBytes = entity.getBytes( UTF8 );
            }
            catch ( UnsupportedEncodingException e )
            {
                throw new RuntimeException( "Could not encode string as UTF-8",
                        e );
            }
            builder = builder.entity( entityAsBytes );
            builder = builder.header( HttpHeaders.CONTENT_LENGTH,
                    String.valueOf( entityAsBytes.length ) );
            builder = builder.header( HttpHeaders.CONTENT_ENCODING, UTF8 );
        }

        return builder;
    }

    /**
     * Check a string for unicode BOM. If present, remove it.
     * 
     * @param string
     * @return
     */
    public static String dodgeStartingUnicodeMarker( String string )
    {
        if ( string != null && string.length() > 0 )
        {
            if ( string.charAt( 0 ) == 0xfeff )
            {
                return string.substring( 1 );
            }
        }
        return string;
    }

    /**
     * Construct an error response for bad JSON input.
     * 
     * @param json
     * @param e
     * @param renderer
     * @return
     */
    public static Response buildBadJsonExceptionResponse( String json,
            Exception e, Renderer renderer )
    {
        return buildExceptionResponse( Status.BAD_REQUEST, "\n----\n" + json
                                                           + "\n----", e,
                renderer );
    }

    /**
     * Construct a generic exception response.
     * 
     * @param status
     * @param message
     * @param e
     * @param renderer
     * @return
     */
    public static Response buildExceptionResponse( Status status,
            String message, Exception e, Renderer renderer )
    {
        ExceptionRepresentation eRep = new ExceptionRepresentation( e );
        String entity = JsonRenderers.DEFAULT.render( eRep );
        return addHeaders( Response.status( status ).entity( entity ) ).build();
    }
}
