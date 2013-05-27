/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import javax.management.relation.RelationNotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import org.neo4j.server.rest.web.NodeNotFoundException;

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
        if ( representation.isEmpty() )
        {
            return noContent();
        }
        return response( Response.ok(), representation );
    }

    public final <REPR extends Representation & EntityRepresentation> Response okIncludeLocation( REPR representation ) throws BadInputException
    {
        if ( representation.isEmpty() )
        {
            return noContent();
        }
        return response( Response.ok().header( HttpHeaders.LOCATION, uri( representation ) ), representation );
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

    public final <REPR extends Representation & EntityRepresentation> Response conflict( REPR representation )
            throws BadInputException
    {
        return response( Response.status( Status.CONFLICT ), representation );
    }

    public Response serverError( Throwable exception )
    {
        return response( Response.status( Status.INTERNAL_SERVER_ERROR ), new ExceptionRepresentation( exception ) );
    }

    private URI uri( EntityRepresentation representation ) throws BadInputException
    {
        return URI.create( assemble( representation.selfUri() ) );
    }

    protected Response response( ResponseBuilder response, Representation representation )
    {
        return formatRepresentation( response, representation )
                .header( HttpHeaders.CONTENT_ENCODING, UTF8 )
                .type( getMediaType() )
                .build();
    }

    private ResponseBuilder formatRepresentation( ResponseBuilder response, final Representation representation )
    {
        if ( format instanceof StreamingFormat )
        {
            return response.entity( stream( representation, (StreamingFormat) format ) );
        }
        else
        {
            return response.entity( toBytes( assemble( representation ) ) );
        }
    }

    private Object stream( final Representation representation, final StreamingFormat streamingFormat )
    {
        return new StreamingOutput()
        {
            public void write( OutputStream output ) throws IOException, WebApplicationException
            {
                RepresentationFormat outputStreamFormat = streamingFormat.writeTo( output );
                try
                {
                    representation.serialize( outputStreamFormat, baseUri, extensions );
                }
                catch ( Exception e )
                {
                    if ( e instanceof NodeNotFoundException || e instanceof RelationNotFoundException )
                    {
                        new WebApplicationException( notFound( e ) );
                    }
                    if ( e instanceof BadInputException )
                    {
                        throw new WebApplicationException( badRequest( e ) );
                    }
                    throw new WebApplicationException( serverError( e ) );
                }
            }
        };
    }

    private byte[] toBytes( String entity )
    {
        byte[] entityAsBytes;
        try
        {
            entityAsBytes = entity.getBytes( UTF8 );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( "Could not encode string as UTF-8", e );
        }
        return entityAsBytes;
    }

    public MediaType getMediaType()
    {
        return format.mediaType;
    }

    public String assemble( Representation representation )
    {
        return representation.serialize( format, baseUri, extensions );
    }

    public Response noContent()
    {
        return Response.status( Status.NO_CONTENT )
                .build();
    }

    public Response methodNotAllowed( UnsupportedOperationException e )
    {
        return response( Response.status( 405 ), new ExceptionRepresentation( e ) );
    }
}
