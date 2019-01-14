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
package org.neo4j.server.rest.transactional;

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.spi.dispatch.RequestDispatcher;

import org.neo4j.graphdb.Transaction;
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
import org.neo4j.server.rest.web.RestfulGraphDatabase;

import static org.neo4j.server.rest.repr.RepresentationWriteHandler.DO_NOTHING;

public class TransactionalRequestDispatcher implements RequestDispatcher
{
    private final Database database;
    private final RequestDispatcher requestDispatcher;

    public TransactionalRequestDispatcher( Database database, RequestDispatcher requestDispatcher )
    {
        this.database = database;
        this.requestDispatcher = requestDispatcher;
    }

    @Override
    public void dispatch( Object o, final HttpContext httpContext )
    {
        RepresentationWriteHandler representationWriteHandler = DO_NOTHING;

        LoginContext loginContext = AuthorizedRequestWrapper.getLoginContextFromHttpContext( httpContext );

        final GraphDatabaseFacade graph = database.getGraph();
        if ( o instanceof RestfulGraphDatabase )
        {
            RestfulGraphDatabase restfulGraphDatabase = (RestfulGraphDatabase) o;

            final Transaction transaction = graph.beginTransaction( KernelTransaction.Type.implicit, loginContext );

            restfulGraphDatabase.getOutputFormat().setRepresentationWriteHandler( representationWriteHandler = new
                    CommitOnSuccessfulStatusCodeRepresentationWriteHandler( httpContext, transaction ));
        }
        else if ( o instanceof BatchOperationService )
        {
            BatchOperationService batchOperationService = (BatchOperationService) o;

            final Transaction transaction = graph.beginTransaction( KernelTransaction.Type.explicit, loginContext );

            batchOperationService.setRepresentationWriteHandler( representationWriteHandler = new
                    CommitOnSuccessfulStatusCodeRepresentationWriteHandler( httpContext, transaction ) );
        }
        else if ( o instanceof CypherService )
        {
            CypherService cypherService = (CypherService) o;

            final Transaction transaction = graph.beginTransaction( KernelTransaction.Type.explicit, loginContext );

            cypherService.getOutputFormat().setRepresentationWriteHandler( representationWriteHandler = new
                    CommitOnSuccessfulStatusCodeRepresentationWriteHandler( httpContext, transaction ) );
        }
        else if ( o instanceof DatabaseMetadataService )
        {
            DatabaseMetadataService databaseMetadataService = (DatabaseMetadataService) o;

            final Transaction transaction = graph.beginTransaction( KernelTransaction.Type.implicit, loginContext );

            databaseMetadataService.setRepresentationWriteHandler( representationWriteHandler = new
                    RepresentationWriteHandler()
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
            } );
        }
        else if ( o instanceof ExtensionService )
        {
            ExtensionService extensionService = (ExtensionService) o;
            extensionService.getOutputFormat().setRepresentationWriteHandler( representationWriteHandler = new
                    RepresentationWriteHandler()
            {
                Transaction transaction;

                @Override
                public void onRepresentationStartWriting()
                {
                    transaction = graph.beginTransaction( KernelTransaction.Type.implicit, loginContext );
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
            } );
        }

        try
        {
            requestDispatcher.dispatch( o, httpContext );
        }
        catch ( RuntimeException e )
        {
            representationWriteHandler.onRepresentationFinal();

            throw e;
        }
    }
}
