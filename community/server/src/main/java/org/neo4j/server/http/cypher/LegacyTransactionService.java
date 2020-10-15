/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.logging.Log;

@Path( LegacyTransactionService.DB_TRANSACTION_PATH )
public class LegacyTransactionService extends AbstractCypherResource
{
    private static final String TRANSACTION = "transaction";
    static final String DB_TRANSACTION_PATH = "/" + TRANSACTION;

    public LegacyTransactionService(
            @Context Config config,
            @Context HttpTransactionManager httpTransactionManager,
            @Context UriInfo uriInfo,
            @Context Log log )
    {
        super( httpTransactionManager, uriInfo, log, config.get( GraphDatabaseSettings.default_database ) );
    }

    @Override
    protected URI dbUri( UriInfo uriInfo, String databaseName )
    {
        UriBuilder builder = uriInfo.getBaseUriBuilder();
        return builder.build();
    }

    @Override
    protected URI cypherUri( UriInfo uriInfo, String databaseName )
    {
        UriBuilder builder = uriInfo.getBaseUriBuilder();
        return builder.path( TRANSACTION ).build();
    }
}
