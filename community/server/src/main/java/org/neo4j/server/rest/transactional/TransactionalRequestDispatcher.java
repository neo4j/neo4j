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
package org.neo4j.server.rest.transactional;

import static org.neo4j.server.rest.repr.RepresentationWriteHandler.DO_NOTHING;

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.spi.dispatch.RequestDispatcher;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.repr.RepresentationWriteHandler;
import org.neo4j.server.rest.web.BatchOperationService;
import org.neo4j.server.rest.web.CypherService;
import org.neo4j.server.rest.web.DatabaseMetadataService;
import org.neo4j.server.rest.web.ExtensionService;
import org.neo4j.server.rest.web.RestfulGraphDatabase;

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

        if ( o instanceof RestfulGraphDatabase )
        {
            RestfulGraphDatabase restfulGraphDatabase = (RestfulGraphDatabase) o;

            final Transaction transaction = database.getGraph().beginTx();

            restfulGraphDatabase.getOutputFormat().setRepresentationWriteHandler( representationWriteHandler = new
                    CommitOnSuccessfulStatusCodeRepresentationWriteHandler( httpContext, transaction ));
        }
        else if ( o instanceof BatchOperationService )
        {
            BatchOperationService batchOperationService = (BatchOperationService) o;

            final Transaction transaction = database.getGraph().beginTx();

            batchOperationService.setRepresentationWriteHandler( representationWriteHandler = new
                    CommitOnSuccessfulStatusCodeRepresentationWriteHandler( httpContext, transaction ) );
        }
        else if ( o instanceof CypherService )
        {
            CypherService cypherService = (CypherService) o;

            final Transaction transaction = database.getGraph().beginTx();

            cypherService.getOutputFormat().setRepresentationWriteHandler( representationWriteHandler = new
                    CommitOnSuccessfulStatusCodeRepresentationWriteHandler( httpContext, transaction ) );
        }
        else if ( o instanceof DatabaseMetadataService )
        {
            DatabaseMetadataService databaseMetadataService = (DatabaseMetadataService) o;

            final Transaction transaction = database.getGraph().beginTx();

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
                    transaction = database.getGraph().beginTx();
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
