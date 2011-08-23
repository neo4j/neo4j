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
package org.neo4j.server.rest.repr;

import java.io.UnsupportedEncodingException;
import java.net.URI;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

public class OutputFormat
{
    private static final String UTF8 = "UTF-8";
    private final RepresentationFormat format;
    private final ExtensionInjector extensions;
    private final URI baseUri;

    public OutputFormat( RepresentationFormat format, URI baseUri, ExtensionInjector extensions )
    {
        this.format = format;
        this.baseUri = baseUri;
        this.extensions = extensions;
    }

    public final Response ok( Representation representation )
    {
        if ( representation.isEmpty() ) return noContent();
        return response( Response.ok(), representation );
    }

    public final <REPR extends Representation & EntityRepresentation> Response created( REPR representation )
            throws BadInputException
    {
        return response( Response.created( uri( representation ) ), representation );
    }

    public final Response response( Status status, Representation representation ) throws BadInputException
    {
        return response( Response.status( status ), representation );
    }

    public Response badRequest( Throwable exception )
    {
        return response( Response.status( Status.BAD_REQUEST ), new ExceptionRepresentation( exception ) );
    }

    public Response forbidden( Throwable exception )
    {
        return response( Response.status( Status.FORBIDDEN ), new ExceptionRepresentation( exception ) );
    }

    public Response notFound( Throwable exception )
    {
        return response( Response.status( Status.NOT_FOUND ), new ExceptionRepresentation( exception ) );
    }

    public Response notFound()
    {
        return Response.status( Status.NOT_FOUND )
                .build();
    }

    public Response conflict( Throwable exception )
    {
        return response( Response.status( Status.CONFLICT ), new ExceptionRepresentation( exception ) );
    }

    public Response serverError( Throwable exception )
    {
        return response( Response.status( Status.INTERNAL_SERVER_ERROR ), new ExceptionRepresentation( exception ) );
    }

    private URI uri( EntityRepresentation representation ) throws BadInputException
    {
        return URI.create( format( representation.selfUri() ) );
    }

    protected Response response( ResponseBuilder response, Representation representation )
    {
        String entity = format( representation );
        byte[] entityAsBytes;
        try
        {
            entityAsBytes = entity.getBytes( UTF8 );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( "Could not encode string as UTF-8", e );
        }
        return response.entity( entityAsBytes )
                .header( HttpHeaders.CONTENT_ENCODING, UTF8 )
                .type( getMediaType() )
                .build();
    }

    public MediaType getMediaType()
    {
        return format.mediaType;
    }

    public String format( Representation representation )
    {
        return representation.serialize( format, baseUri, extensions );
    }

    public Response noContent()
    {
        return Response.status( Status.NO_CONTENT )
                .build();
    }

    public Response methodNotAllowed(UnsupportedOperationException e) {
        return response( Response.status(405), new ExceptionRepresentation( e ) );
    }
}
