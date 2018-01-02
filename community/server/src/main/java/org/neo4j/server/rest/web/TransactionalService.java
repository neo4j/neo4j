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
package org.neo4j.server.rest.web;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.neo4j.server.rest.transactional.ExecutionResultSerializer;
import org.neo4j.server.rest.transactional.TransactionFacade;
import org.neo4j.server.rest.transactional.TransactionHandle;
import org.neo4j.server.rest.transactional.TransactionTerminationHandle;
import org.neo4j.server.rest.transactional.error.Neo4jError;
import org.neo4j.server.rest.transactional.error.TransactionLifecycleException;

import static java.util.Arrays.asList;


/**
 * This does basic mapping from HTTP to {@link org.neo4j.server.rest.transactional.TransactionFacade}, and should not
 * do anything more complicated than that.
 */
@Path("/transaction")
public class TransactionalService
{
    private final TransactionFacade facade;
    private final TransactionUriScheme uriScheme;

    public TransactionalService( @Context TransactionFacade facade, @Context UriInfo uriInfo )
    {
        this.facade = facade;
        this.uriScheme = new TransactionUriBuilder( uriInfo );
    }

    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    public Response executeStatementsInNewTransaction( final InputStream input, @Context final UriInfo uriInfo,
                                                       @Context final HttpServletRequest request )
    {
        try
        {
            TransactionHandle transactionHandle = facade.newTransactionHandle( uriScheme );
            return createdResponse( transactionHandle, executeStatements( input, transactionHandle, uriInfo.getBaseUri(), request ) );
        }
        catch ( TransactionLifecycleException e )
        {
            return invalidTransaction( e, uriInfo.getBaseUri() );
        }
    }

    @POST
    @Path("/{id}")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    public Response executeStatements( @PathParam("id") final long id, final InputStream input,
                                       @Context final UriInfo uriInfo, @Context final HttpServletRequest request )
    {
        final TransactionHandle transactionHandle;
        try
        {
            transactionHandle = facade.findTransactionHandle( id );
        }
        catch ( TransactionLifecycleException e )
        {
            return invalidTransaction( e, uriInfo.getBaseUri() );
        }
        return okResponse( executeStatements( input, transactionHandle, uriInfo.getBaseUri(), request ) );
    }

    @POST
    @Path("/{id}/commit")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    public Response commitTransaction( @PathParam("id") final long id, final InputStream input, @Context final UriInfo uriInfo,
                                       @Context final HttpServletRequest request )
    {
        final TransactionHandle transactionHandle;
        try
        {
            transactionHandle = facade.findTransactionHandle( id );
        }
        catch ( TransactionLifecycleException e )
        {
            return invalidTransaction( e, uriInfo.getBaseUri() );
        }
        return okResponse( executeStatementsAndCommit( input, transactionHandle, uriInfo.getBaseUri(), false, request ) );
    }

    @POST
    @Path("/commit")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    public Response commitNewTransaction( final InputStream input, @Context final UriInfo uriInfo,
                                          @Context final HttpServletRequest request )
    {
        final TransactionHandle transactionHandle;
        try
        {
            transactionHandle = facade.newTransactionHandle( uriScheme );
        }
        catch ( TransactionLifecycleException e )
        {
            return invalidTransaction( e, uriInfo.getBaseUri() );
        }
        final StreamingOutput streamingResults = executeStatementsAndCommit( input,
                transactionHandle,
                uriInfo.getBaseUri(),
                true, request );
        return okResponse( streamingResults );
    }

    @DELETE
    @Path("/{id}")
    @Consumes({MediaType.APPLICATION_JSON})
    public Response rollbackTransaction( @PathParam("id") final long id, @Context UriInfo uriInfo )
    {
        final TransactionHandle transactionHandle;
        try
        {
            transactionHandle = facade.terminate( id );
        }
        catch ( TransactionLifecycleException e )
        {
            return invalidTransaction( e, uriInfo.getBaseUri() );
        }
        return okResponse( rollback( transactionHandle, uriInfo.getBaseUri() ) );
    }

    private Response invalidTransaction( final TransactionLifecycleException e, final URI baseUri )
    {
        return Response.status( Response.Status.NOT_FOUND )
                .entity( serializeError( e.toNeo4jError(), baseUri ) )
                .build();
    }

    private Response createdResponse( TransactionHandle transactionHandle, StreamingOutput streamingResults )
    {
        return Response.created( transactionHandle.uri() )
                .entity( streamingResults )
                .build();
    }

    private Response okResponse( StreamingOutput streamingResults )
    {
        return Response.ok()
                .entity( streamingResults )
                .build();
    }

    private StreamingOutput executeStatements( final InputStream input, final TransactionHandle transactionHandle,
                                               final URI baseUri, final HttpServletRequest request )
    {
        return new StreamingOutput()
        {
            @Override
            public void write( OutputStream output ) throws IOException, WebApplicationException
            {
                transactionHandle.execute( facade.deserializer( input ), facade.serializer( output, baseUri ),
                        request );
            }
        };
    }

    private StreamingOutput executeStatementsAndCommit( final InputStream input,
                                                        final TransactionHandle transactionHandle,
                                                        final URI baseUri,
                                                        final boolean pristine,
                                                        final HttpServletRequest request )
    {
        return new StreamingOutput()
        {
            @Override
            public void write( OutputStream output ) throws IOException, WebApplicationException
            {
                OutputStream wrappedOutput = pristine ? new InterruptingOutputStream( output,
                        transactionHandle ) : output;
                transactionHandle.commit( facade.deserializer( input ), facade.serializer( wrappedOutput,
                        baseUri ), pristine, request );
            }
        };
    }

    private StreamingOutput rollback( final TransactionHandle transactionHandle, final URI baseUri )
    {
        return new StreamingOutput()
        {
            @Override
            public void write( OutputStream output ) throws IOException, WebApplicationException
            {
                if ( transactionHandle != null )
                {
                    transactionHandle.rollback( facade.serializer( output, baseUri ) );
                }
            }
        };
    }

    private StreamingOutput serializeError( final Neo4jError neo4jError, final URI baseUri )
    {
        return new StreamingOutput()
        {
            @Override
            public void write( OutputStream output ) throws IOException, WebApplicationException
            {
                ExecutionResultSerializer serializer = facade.serializer( output, baseUri );
                serializer.errors( asList( neo4jError ) );
                serializer.finish();
            }
        };
    }

    public static class TransactionUriBuilder implements TransactionUriScheme
    {
        private final UriInfo uriInfo;

        public TransactionUriBuilder( UriInfo uriInfo )
        {
            this.uriInfo = uriInfo;
        }

        @Override
        public URI txUri( long id )
        {
            return builder( id ).build();
        }

        @Override
        public URI txCommitUri( long id )
        {
            return builder( id ).path( "/commit" ).build();
        }

        private UriBuilder builder( long id )
        {
            return uriInfo.getBaseUriBuilder().path( TransactionalService.class ).path( "/" + id );
        }
    }

    private class InterruptingOutputStream extends OutputStream
    {
        private final OutputStream delegate;
        private final TransactionTerminationHandle terminationHandle;

        private InterruptingOutputStream( OutputStream delegate, TransactionTerminationHandle terminationHandle )
        {
            this.delegate = delegate;
            this.terminationHandle = terminationHandle;
        }

        @Override
        public void write( byte[] b ) throws IOException
        {
            try
            {
                delegate.write( b );
            }
            catch ( IOException e )
            {
                terminate();
                throw e;
            }
        }

        @Override
        public void write( byte[] b, int off, int len ) throws IOException
        {
            try
            {
                delegate.write( b, off, len );
            }
            catch ( IOException e )
            {
                terminate();
                throw e;
            }
        }

        @Override
        public void flush() throws IOException
        {
            try
            {
                delegate.flush();
            }
            catch ( IOException e )
            {
                terminate();
                throw e;
            }
        }

        @Override
        public void close() throws IOException
        {
            try
            {
                delegate.close();
            }
            catch ( IOException e )
            {
                terminate();
                throw e;
            }
        }

        @Override
        public void write( int b ) throws IOException
        {
            try
            {
                delegate.write( b );
            }
            catch ( IOException e )
            {
                terminate();
                throw e;
            }
        }

        private void terminate()
        {
            terminationHandle.terminate();
        }
    }
}
