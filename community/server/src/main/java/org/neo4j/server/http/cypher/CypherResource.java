/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.http.cypher;

import java.net.URI;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import org.neo4j.configuration.Config;
import org.neo4j.logging.InternalLog;
import org.neo4j.memory.MemoryPool;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.time.SystemNanoClock;

@Path(CypherResource.DB_TRANSACTION_PATH)
public class CypherResource extends AbstractCypherResource {
    public static final String NAME = "transaction";
    private static final String DB_PATH_PARAM_NAME = "databaseName";
    private static final String TRANSACTION_PATH_FRAGMENT = "tx";
    static final String DB_TRANSACTION_PATH = "/{" + DB_PATH_PARAM_NAME + "}/" + TRANSACTION_PATH_FRAGMENT;

    public CypherResource(
            @Context HttpTransactionManager httpTransactionManager,
            @Context UriInfo uriInfo,
            @Context MemoryPool memoryPool,
            @Context InternalLog log,
            @Context HttpHeaders headers,
            @Context HttpServletRequest request,
            @PathParam(DB_PATH_PARAM_NAME) String databaseName,
            @Context SystemNanoClock clock) {
        super(httpTransactionManager, uriInfo, memoryPool, log, databaseName, clock);
    }

    @Override
    protected URI dbUri(UriInfo uriInfo, String databaseName) {
        UriBuilder builder = uriInfo.getBaseUriBuilder().path(databaseName);
        return builder.build();
    }

    @Override
    protected URI cypherUri(UriInfo uriInfo, String databaseName) {
        UriBuilder builder = uriInfo.getBaseUriBuilder().path(databaseName);
        return builder.path(TRANSACTION_PATH_FRAGMENT).build();
    }

    public static String absoluteDatabaseTransactionPath(Config config) {
        return config.get(ServerSettings.db_api_path).getPath() + DB_TRANSACTION_PATH;
    }
}
