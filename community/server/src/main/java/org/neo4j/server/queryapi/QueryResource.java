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
package org.neo4j.server.queryapi;

import static org.neo4j.server.queryapi.response.error.HttpErrorResponse.singleError;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.queryapi.metrics.QueryAPIMetricsMonitor;
import org.neo4j.server.queryapi.request.AccessMode;
import org.neo4j.server.queryapi.request.QueryRequest;

@Path(QueryResource.ROOT_PATH)
@Produces({QueryMimeTypes.UNTYPED_JSON, QueryMimeTypes.TYPED_JSON, QueryMimeTypes.TYPED_JSON_V1x0})
@Consumes({QueryMimeTypes.UNTYPED_JSON, QueryMimeTypes.TYPED_JSON, QueryMimeTypes.TYPED_JSON_V1x0})
public class QueryResource {

    public static final String NAME = "query";
    private static final String DB_PATH_PARAM_NAME = "databaseName";
    private static final String TX_ID_PATH_PARAM_NAME = "txId";
    public static final String ROOT_PATH = "/{" + DB_PATH_PARAM_NAME + "}/query/v2";
    private final QueryAPIMetricsMonitor monitor;
    private final QueryController queryController;

    public QueryResource(@Context QueryController queryController, @Context QueryAPIMetricsMonitor monitor) {
        this.queryController = queryController;
        this.monitor = monitor;
    }

    @POST
    public Response execute(
            @PathParam(DB_PATH_PARAM_NAME) String databaseName,
            QueryRequest request,
            @Context HttpServletRequest rawRequest,
            @Context HttpHeaders headers) {
        meterRequest(request);
        var clientErrorResponse = validateRequest(request);

        if (clientErrorResponse != null) {
            return clientErrorResponse;
        }

        return queryController.executeQuery(request, rawRequest, databaseName);
    }

    @POST
    @Path("/tx")
    public Response beginTransaction(
            @PathParam(DB_PATH_PARAM_NAME) String databaseName,
            QueryRequest request,
            @Context HttpServletRequest rawRequest,
            @Context HttpHeaders headers) {
        meterRequest(request);
        return queryController.beginTransaction(request, rawRequest, databaseName);
    }

    @POST
    @Path("tx/{" + TX_ID_PATH_PARAM_NAME + "}")
    public Response continueTransaction(
            @PathParam(DB_PATH_PARAM_NAME) String databaseName,
            @PathParam(TX_ID_PATH_PARAM_NAME) String requestedTxId,
            QueryRequest request,
            @Context HttpServletRequest rawRequest,
            @Context HttpHeaders headers) {
        meterRequest(request);
        return queryController.continueTransaction(request, rawRequest, databaseName, requestedTxId);
    }

    @POST
    @Path("tx/{" + TX_ID_PATH_PARAM_NAME + "}/commit")
    public Response commitTransaction(
            @PathParam(DB_PATH_PARAM_NAME) String databaseName,
            @PathParam(TX_ID_PATH_PARAM_NAME) String txId,
            QueryRequest request,
            @Context HttpServletRequest rawRequest,
            @Context HttpHeaders headers) {
        meterRequest(request);
        return queryController.commitTransaction(request, rawRequest, databaseName, txId);
    }

    @DELETE
    @Path("tx/{" + TX_ID_PATH_PARAM_NAME + "}")
    public Response rollbackTransaction(
            @PathParam(DB_PATH_PARAM_NAME) String databaseName,
            @PathParam(TX_ID_PATH_PARAM_NAME) String txId,
            QueryRequest request,
            @Context HttpServletRequest rawRequest,
            @Context HttpHeaders headers) {
        return queryController.rollbackTransaction(txId, rawRequest, databaseName);
    }

    public static String absoluteDatabaseTransactionPath(Config config) {
        return config.get(ServerSettings.db_api_path).getPath() + ROOT_PATH;
    }

    private void meterRequest(QueryRequest request) {
        if (request.accessMode() != null && request.accessMode().equals(AccessMode.READ)) {
            monitor.readRequest();
        }
        if (request.parameters() != null && !request.parameters().isEmpty()) {
            monitor.parameter();
        }
    }

    private static Response validateRequest(QueryRequest request) {
        if (request.statement() == null || request.statement().isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(singleError(Status.Request.Invalid.code().serialize(), "statement cannot be null or empty"))
                    .build();
        }
        return null;
    }
}
