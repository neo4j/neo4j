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
package org.neo4j.server.http.cypher;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.logging.Log;
import org.neo4j.server.http.cypher.format.api.InputEventStream;
import org.neo4j.server.rest.dbms.AuthorizedRequestWrapper;
import org.neo4j.server.rest.web.HttpConnectionInfoFactory;

import static org.neo4j.server.web.HttpHeaderUtils.getTransactionTimeout;

@Path( "/transaction" )
public class CypherResource
{

    private final TransactionFacade facade;
    private final TransactionUriScheme uriScheme;
    private final Log log;
    private final UriInfo uriInfo;

    public CypherResource( @Context TransactionFacade facade, @Context UriInfo uriInfo, @Context Log log )
    {
        this.facade = facade;
        this.uriScheme = new TransactionUriBuilder( uriInfo );
        this.log = log;
        this.uriInfo = uriInfo;
    }

    @POST
    public Response executeStatementsInNewTransaction( @Context HttpHeaders headers, InputEventStream inputEventStream, @Context HttpServletRequest request )
    {

        inputEventStream = ensureNotNull( inputEventStream );

        TransactionHandle transactionHandle = createNewTransactionHandle( headers, request, false );

        Invocation invocation = new Invocation( log, transactionHandle, uriScheme.txCommitUri( transactionHandle.getId() ), inputEventStream, false );
        OutputEventStreamImpl outputEventStream =
                new OutputEventStreamImpl( inputEventStream.getParameters(), facade.getTransactionContainer(), uriInfo, invocation::execute );
        return Response.created( transactionHandle.uri() ).entity( outputEventStream ).build();
    }

    @POST
    @Path( "/{id}" )
    @Consumes( {MediaType.APPLICATION_JSON} )
    @Produces( {MediaType.APPLICATION_JSON} )
    public Response executeStatements( @PathParam( "id" ) long id, InputEventStream inputEventStream, @Context UriInfo uriInfo,
            @Context HttpServletRequest request )
    {
        return executeInExistingTransaction( id, inputEventStream, false );
    }

    @POST
    @Path( "/{id}/commit" )
    @Consumes( {MediaType.APPLICATION_JSON} )
    @Produces( {MediaType.APPLICATION_JSON} )
    public Response commitTransaction( @PathParam( "id" ) long id, InputEventStream inputEventStream )
    {
        return executeInExistingTransaction( id, inputEventStream, true );
    }

    @POST
    @Path( "/commit" )
    @Consumes( {MediaType.APPLICATION_JSON} )
    @Produces( {MediaType.APPLICATION_JSON} )
    public Response commitNewTransaction( @Context HttpHeaders headers, InputEventStream inputEventStream, @Context HttpServletRequest request )
    {
        inputEventStream = ensureNotNull( inputEventStream );

        TransactionHandle transactionHandle = createNewTransactionHandle( headers, request, true );

        Invocation invocation = new Invocation( log, transactionHandle, null, inputEventStream, true );
        OutputEventStreamImpl outputEventStream =
                new OutputEventStreamImpl( inputEventStream.getParameters(), facade.getTransactionContainer(), uriInfo, invocation::execute );
        return Response.ok( outputEventStream ).build();
    }

    @DELETE
    @Path( "/{id}" )
    @Consumes( {MediaType.APPLICATION_JSON} )
    public Response rollbackTransaction( @PathParam( "id" ) final long id )
    {
        TransactionHandle transactionHandle;
        try
        {
            transactionHandle = facade.terminate( id );
        }
        catch ( TransactionLifecycleException e )
        {
            return invalidTransaction( e, Collections.emptyMap() );
        }

        RollbackInvocation invocation = new RollbackInvocation( log, transactionHandle );

        OutputEventStreamImpl outputEventStream =
                new OutputEventStreamImpl( Collections.emptyMap(), facade.getTransactionContainer(), uriInfo, invocation::execute );
        return Response.ok().entity( outputEventStream ).build();
    }

    private TransactionHandle createNewTransactionHandle( @Context HttpHeaders headers, @Context HttpServletRequest request, boolean implicitTransaction )
    {
        LoginContext loginContext = AuthorizedRequestWrapper.getLoginContextFromHttpServletRequest( request );
        long customTransactionTimeout = getTransactionTimeout( headers, log );
        ClientConnectionInfo connectionInfo = HttpConnectionInfoFactory.create( request );
        return facade.newTransactionHandle( uriScheme, implicitTransaction, loginContext, connectionInfo, customTransactionTimeout );
    }

    private Response executeInExistingTransaction( long transactionId, InputEventStream inputEventStream, boolean finishWithCommit )
    {
        inputEventStream = ensureNotNull( inputEventStream );
        TransactionHandle transactionHandle;
        try
        {
            transactionHandle = facade.findTransactionHandle( transactionId );
        }
        catch ( TransactionLifecycleException e )
        {
            return invalidTransaction( e, inputEventStream.getParameters() );
        }
        Invocation invocation =
                new Invocation( log, transactionHandle, uriScheme.txCommitUri( transactionHandle.getId() ), inputEventStream, finishWithCommit );
        OutputEventStreamImpl outputEventStream =
                new OutputEventStreamImpl( inputEventStream.getParameters(), facade.getTransactionContainer(), uriInfo, invocation::execute );
        return Response.ok( outputEventStream ).build();
    }

    private Response invalidTransaction( TransactionLifecycleException e, Map<String,Object> parameters )
    {
        ErrorInvocation errorInvocation = new ErrorInvocation( e.toNeo4jError() );
        return Response.status( Response.Status.NOT_FOUND ).entity(
                new OutputEventStreamImpl( parameters, facade.getTransactionContainer(), uriInfo, errorInvocation::execute ) ).build();
    }

    private InputEventStream ensureNotNull( InputEventStream inputEventStream )
    {
        if ( inputEventStream != null )
        {
            return inputEventStream;
        }

        return InputEventStream.EMPTY;
    }

    private static class TransactionUriBuilder implements TransactionUriScheme
    {
        private final UriInfo uriInfo;

        TransactionUriBuilder( UriInfo uriInfo )
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
            return uriInfo.getBaseUriBuilder().path( CypherResource.class ).path( "/" + id );
        }
    }
}
