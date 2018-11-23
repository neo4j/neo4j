/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.server.rest.transactional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceContext;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.dbms.AuthorizedRequestWrapper;
import org.neo4j.server.rest.repr.RepresentationWriteHandler;
import org.neo4j.server.rest.web.BatchOperationService;
import org.neo4j.server.rest.web.CypherService;
import org.neo4j.server.rest.web.DatabaseMetadataService;
import org.neo4j.server.rest.web.ExtensionService;
import org.neo4j.server.rest.web.HttpConnectionInfoFactory;
import org.neo4j.server.rest.web.RestfulGraphDatabase;
import org.neo4j.server.web.JettyHttpConnection;

import static org.neo4j.server.rest.repr.RepresentationWriteHandler.DO_NOTHING;

public class TransactionalContainerFilter implements ContainerRequestFilter
{
    @Context
    private Database database;

    @Context
    private ResourceInfo resourceInfo;

    @Context
    private ResourceContext resourceContext;

    @Context
    private HttpServletResponse response;

    @Override
    public void filter( ContainerRequestContext requestContext )
    {
        RepresentationWriteHandler representationWriteHandler = DO_NOTHING;

        LoginContext loginContext = AuthorizedRequestWrapper.getLoginContextFromContainerRequestContext( requestContext );
        ClientConnectionInfo clientConnection = getConnectionInfo();

        final Object resource = resourceContext.getResource( resourceInfo.getResourceClass() );
        final GraphDatabaseFacade graph = database.getGraph();
        if ( resource instanceof RestfulGraphDatabase )
        {
            RestfulGraphDatabase restfulGraphDatabase = (RestfulGraphDatabase) resource;

            final Transaction transaction = graph.beginTransaction( KernelTransaction.Type.implicit, loginContext, clientConnection );

            representationWriteHandler = new CommitOnSuccessfulStatusCodeRepresentationWriteHandler( response, transaction );
            restfulGraphDatabase.getOutputFormat().setRepresentationWriteHandler( representationWriteHandler );
        }
        else if ( resource instanceof BatchOperationService )
        {
            BatchOperationService batchOperationService = (BatchOperationService) resource;

            final Transaction transaction = graph.beginTransaction( KernelTransaction.Type.explicit, loginContext, clientConnection );

            representationWriteHandler = new CommitOnSuccessfulStatusCodeRepresentationWriteHandler( response, transaction );
            batchOperationService.setRepresentationWriteHandler( representationWriteHandler );
        }
        else if ( resource instanceof CypherService )
        {
            CypherService cypherService = (CypherService) resource;

            final Transaction transaction = graph.beginTransaction( KernelTransaction.Type.explicit, loginContext, clientConnection );

            representationWriteHandler = new CommitOnSuccessfulStatusCodeRepresentationWriteHandler( response, transaction );
            cypherService.getOutputFormat().setRepresentationWriteHandler( representationWriteHandler );
        }
        else if ( resource instanceof DatabaseMetadataService )
        {
            DatabaseMetadataService databaseMetadataService = (DatabaseMetadataService) resource;

            final Transaction transaction = graph.beginTransaction( KernelTransaction.Type.implicit, loginContext, clientConnection );
            representationWriteHandler = new RepresentationWriteHandler()
            {
                @Override
                public void onRepresentationStartWriting()
                {
                    // do nothing
                }

                @Override
                public void onRepresentationWritten()
                {
                    // doesn't need to commit
                }

                @Override
                public void onRepresentationFinal()
                {
                    transaction.close();
                }
            };
            databaseMetadataService.setRepresentationWriteHandler( representationWriteHandler );
        }
        else if ( resource instanceof ExtensionService )
        {
            ExtensionService extensionService = (ExtensionService) resource;
            representationWriteHandler = new RepresentationWriteHandler()
            {
                Transaction transaction;

                @Override
                public void onRepresentationStartWriting()
                {
                    transaction = graph.beginTransaction( KernelTransaction.Type.implicit, loginContext, clientConnection );
                }

                @Override
                public void onRepresentationWritten()
                {
                    // doesn't need to commit
                }

                @Override
                public void onRepresentationFinal()
                {
                    if ( transaction != null )
                    {
                        transaction.close();
                    }
                }
            };
            extensionService.getOutputFormat().setRepresentationWriteHandler( representationWriteHandler );
        }
    }

    private static ClientConnectionInfo getConnectionInfo()
    {
        JettyHttpConnection httpConnection = JettyHttpConnection.getCurrentJettyHttpConnection();
        if ( httpConnection == null )
        {
            // if we do not have connection binded we are not in a phase of handling client request and that can be considered as embedded.
            return ClientConnectionInfo.EMBEDDED_CONNECTION;
        }
        HttpServletRequest request = httpConnection.getHttpChannel().getRequest();
        return HttpConnectionInfoFactory.create( request );
    }
}
