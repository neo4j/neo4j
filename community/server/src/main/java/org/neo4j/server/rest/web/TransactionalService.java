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
package org.neo4j.server.rest.web;

import static org.neo4j.helpers.collection.IteratorUtil.singletonIterator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.neo4j.server.rest.transactional.ExecutionResultSerializer;
import org.neo4j.server.rest.transactional.TransactionFacade;
import org.neo4j.server.rest.transactional.StatementDeserializer;
import org.neo4j.server.rest.transactional.TransactionHandle;
import org.neo4j.server.rest.transactional.error.Neo4jError;

/**
 * This does basic mapping from HTTP to {@link org.neo4j.server.rest.transactional.TransactionFacade}, and should not do anything more complicated
 * than that.
 */
@Path("/transaction")
public class TransactionalService
{
    private final TransactionFacade actions;
    private final TransactionUriScheme uriScheme;

    public TransactionalService( @Context TransactionFacade actions, @Context UriInfo uriInfo )
    {
        this.actions = actions;
        this.uriScheme = new TransactionUriBuilder( uriInfo );
    }

    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    public Response executeStatementsInNewTransaction( final InputStream input )
    {
        final TransactionHandle transactionHandle;
        try
        {
            transactionHandle = actions.newTransactionHandle();
        }
        catch ( Neo4jError neo4jError )
        {
            return invalidTransaction( neo4jError );
        }
        return createdResponse( transactionHandle, executeStatements( input, transactionHandle ) );
    }

    @POST
    @Path("/{id}")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    public Response executeStatements( @PathParam("id") final long id, final InputStream input )
    {
        final TransactionHandle transactionHandle;
        try
        {
            transactionHandle = actions.findTransactionHandle( id );
        }
        catch ( Neo4jError neo4jError )
        {
            return invalidTransaction( neo4jError );
        }
        return okResponse( executeStatements( input, transactionHandle ) );
    }

    @POST
    @Path("/{id}/commit")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    public Response commitTransaction( @PathParam("id") final long id, final InputStream input )
    {
        final TransactionHandle transactionHandle;
        try
        {
            transactionHandle = actions.findTransactionHandle( id );
        }
        catch ( Neo4jError neo4jError )
        {
            return invalidTransaction( neo4jError );
        }
        return okResponse( executeStatementsAndCommit( input, transactionHandle ) );
    }

    @POST
    @Path("/commit")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    public Response commitNewTransaction( final InputStream input )
    {
        final TransactionHandle transactionHandle;
        try
        {
            transactionHandle = actions.newTransactionHandle();
        }
        catch ( Neo4jError neo4jError )
        {
            return invalidTransaction( neo4jError );
        }
        return okResponse( executeStatementsAndCommit( input, transactionHandle ) );
    }

    @DELETE
    @Path("/{id}")
    @Consumes({MediaType.APPLICATION_JSON})
    public Response rollbackTransaction( @PathParam("id") final long id )
    {
        final TransactionHandle transactionHandle;
        try
        {
            transactionHandle = actions.findTransactionHandle( id );
        }
        catch ( Neo4jError neo4jError )
        {
            return invalidTransaction( neo4jError );
        }
        return okResponse( rollback( transactionHandle ) );
    }

    private Response invalidTransaction( Neo4jError neo4jError )
    {
        return Response.status( Response.Status.NOT_FOUND )
                .header( HttpHeaders.CONTENT_ENCODING, "UTF-8" )
                .entity( serializeError( neo4jError ) ).build();
    }

    private Response createdResponse( TransactionHandle transactionHandle, StreamingOutput streamingResults )
    {
        return Response.created( uriScheme.txUri( transactionHandle.getId() ) )
                .header( HttpHeaders.CONTENT_ENCODING, "UTF-8" )
                .entity( streamingResults ).build();
    }

    private Response okResponse( StreamingOutput streamingResults )
    {
        return Response.ok()
                .header( HttpHeaders.CONTENT_ENCODING, "UTF-8" )
                .entity( streamingResults ).build();
    }

    private StreamingOutput executeStatements( final InputStream input, final TransactionHandle
            transactionHandle )
    {
        return new StreamingOutput()
        {
            @Override
            public void write( OutputStream output ) throws IOException, WebApplicationException
            {
                transactionHandle.execute(
                        new StatementDeserializer( input ),
                        new ExecutionResultSerializer( output, uriScheme ) );
            }
        };
    }

    private StreamingOutput executeStatementsAndCommit( final InputStream input, final TransactionHandle transactionHandle )
    {
        return new StreamingOutput()
        {
            @Override
            public void write( OutputStream output ) throws IOException, WebApplicationException
            {
                transactionHandle.commit(
                        new StatementDeserializer( input ),
                        new ExecutionResultSerializer( output, uriScheme ) );
            }
        };
    }

    private StreamingOutput rollback( final TransactionHandle transactionHandle )
    {
        return new StreamingOutput()
        {
            @Override
            public void write( OutputStream output ) throws IOException, WebApplicationException
            {
                transactionHandle.rollback( new ExecutionResultSerializer( output, uriScheme ) );
            }
        };
    }

    private StreamingOutput serializeError( final Neo4jError neo4jError )
    {
        return new StreamingOutput()
        {
            @Override
            public void write( OutputStream output ) throws IOException, WebApplicationException
            {
                ExecutionResultSerializer serializer = new ExecutionResultSerializer( output, uriScheme );
                serializer.errorsOnly( singletonIterator( neo4jError ) );
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
}
