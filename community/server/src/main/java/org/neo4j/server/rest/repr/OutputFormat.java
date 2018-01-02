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
package org.neo4j.server.rest.repr;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import org.neo4j.server.rest.web.NodeNotFoundException;
import org.neo4j.server.rest.web.RelationshipNotFoundException;
import org.neo4j.server.web.HttpHeaderUtils;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;

public class OutputFormat
{
    private static final String UTF8 = "UTF-8";
    private final RepresentationFormat format;
    private final ExtensionInjector extensions;
    private final URI baseUri;

    private RepresentationWriteHandler representationWriteHandler = RepresentationWriteHandler.DO_NOTHING;

    public OutputFormat( RepresentationFormat format, URI baseUri, ExtensionInjector extensions )
    {
        this.format = format;
        this.baseUri = baseUri;
        this.extensions = extensions;
    }

    public void setRepresentationWriteHandler( RepresentationWriteHandler representationWriteHandler ) {

        this.representationWriteHandler = representationWriteHandler;
    }

    public RepresentationWriteHandler getRepresentationWriteHandler() {
        return this.representationWriteHandler;
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

    public final Response response( Response.StatusType status, Representation representation )
    {
        return response( Response.status( status ), representation );
    }

    /**
     * Before the 'errors' response existed, we would just spit out stack traces.
     * For new endpoints, we should return the new 'errors' response format, which will bundle stack traces only on
     * unknown problems.
     * @param exception the error
     * @return the bad request response     */
    public Response badRequestWithoutLegacyStacktrace( Throwable exception )
    {
        return response( Response.status( BAD_REQUEST ), new ExceptionRepresentation( exception, false ) );
    }

    public Response badRequest( Throwable exception )
    {
        return response( Response.status( BAD_REQUEST ), new ExceptionRepresentation( exception ) );
    }

    public Response notFound( Throwable exception )
    {
        return response( Response.status( Status.NOT_FOUND ), new ExceptionRepresentation( exception ) );
    }

    public Response notFound()
    {
        representationWriteHandler.onRepresentationFinal();
        return Response.status( Status.NOT_FOUND )
                .build();
    }

    public Response seeOther( URI uri )
    {
        return Response.seeOther( baseUri.resolve( uri ) ).build();
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

    /** Server error with stack trace included as needed, see {@link #badRequestWithoutLegacyStacktrace}.
     * @param exception the error
     * @return the internal server error response
     */    public Response serverErrorWithoutLegacyStacktrace( Throwable exception )
    {
        return response( Response.status( Status.INTERNAL_SERVER_ERROR ), new ExceptionRepresentation( exception, false ) );
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
                .type( HttpHeaderUtils.mediaTypeWithCharsetUtf8( getMediaType() ) )
                .build();
    }

    private ResponseBuilder formatRepresentation( ResponseBuilder response, final Representation representation )
    {
        representationWriteHandler.onRepresentationStartWriting();

        boolean mustFail = representation instanceof ExceptionRepresentation;

        if ( format instanceof StreamingFormat )
        {
            return response.entity( stream( representation, (StreamingFormat) format, mustFail ) );
        }
        else
        {
            return response.entity( toBytes( assemble( representation ), mustFail ) );
        }
    }

    private Object stream( final Representation representation, final StreamingFormat streamingFormat, final boolean mustFail )
    {
        return new StreamingOutput()
        {
            public void write( OutputStream output ) throws IOException, WebApplicationException
            {
                RepresentationFormat outputStreamFormat = streamingFormat.writeTo( output );
                try
                {
                    representation.serialize( outputStreamFormat, baseUri, extensions );

                    if ( !mustFail ) representationWriteHandler.onRepresentationWritten();
                }
                catch ( Exception e )
                {
                    if ( e instanceof NodeNotFoundException || e instanceof RelationshipNotFoundException )
                    {
                        throw new WebApplicationException( notFound( e ) );
                    }
                    if ( e instanceof BadInputException )
                    {
                        throw new WebApplicationException( badRequest( e ) );
                    }
                    throw new WebApplicationException( e, serverError( e ) );
                }
                finally
                {
                    representationWriteHandler.onRepresentationFinal();
                }
            }
        };
    }

    public static void write( Representation representation, RepresentationFormat format, URI baseUri )
    {
        representation.serialize( format, baseUri, null );
    }

    private byte[] toBytes( String entity, boolean mustFail )
    {
        byte[] entityAsBytes;
        try
        {
            entityAsBytes = entity.getBytes( UTF8 );
            if (! mustFail) representationWriteHandler.onRepresentationWritten();
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( "Could not encode string as UTF-8", e );
        }
        finally
        {
            representationWriteHandler.onRepresentationFinal();
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
        representationWriteHandler.onRepresentationStartWriting();
        representationWriteHandler.onRepresentationWritten();
        representationWriteHandler.onRepresentationFinal();
        return Response.status( Status.NO_CONTENT )
                .build();
    }

    public Response methodNotAllowed( UnsupportedOperationException e )
    {
        return response( Response.status( 405 ), new ExceptionRepresentation( e ) );
    }

    public Response ok()
    {
        representationWriteHandler.onRepresentationStartWriting();
        representationWriteHandler.onRepresentationWritten();
        representationWriteHandler.onRepresentationFinal();
        return Response.ok().build();
    }

    public Response badRequest( MediaType mediaType, String entity )
    {
        representationWriteHandler.onRepresentationStartWriting();
        representationWriteHandler.onRepresentationFinal();
        return Response.status( BAD_REQUEST ).type( mediaType  ).entity( entity ).build();
    }

    public Response unauthorized( Representation representation, String authChallenge )
    {
        return formatRepresentation( Response.status( UNAUTHORIZED ), representation )
                .header( HttpHeaders.WWW_AUTHENTICATE, authChallenge )
                .build();
    }
}
