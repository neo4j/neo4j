/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.server.webadmin.rest;

import static org.neo4j.server.webadmin.rest.WebUtils.addHeaders;
import static org.neo4j.server.webadmin.rest.WebUtils.buildExceptionResponse;

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response.Status;

import org.neo4j.server.rest.domain.JsonRenderers;
import org.neo4j.server.webadmin.domain.ExportRepresentation;
import org.neo4j.server.webadmin.domain.ExportServiceRepresentation;
import org.neo4j.server.webadmin.task.GraphMLExporter;

/**
 * Provides export functionality via REST.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
@Path(ExportService.ROOT_PATH)
public class ExportService {
    public static final String ROOT_PATH = "export";
    
    private static GraphMLExporter exportTask = new GraphMLExporter();
    private boolean isExporting = false;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getServiceDefinition(@Context UriInfo uriInfo) {

        String entity = JsonRenderers.DEFAULT.render(new ExportServiceRepresentation(uriInfo.getBaseUri()));

        return addHeaders(Response.ok(entity, JsonRenderers.DEFAULT.getMediaType())).build();
    }

    /**
     * Trigger a full export of the underlying database. Will perform the export
     * and return a JSON object explaining where to download the export at.
     * 
     * This will delete any previous exports that may be stored on the server.
     * 
     * If an export is already underway, this will wait for that export to
     * complete and return it's URL instead.
     * 
     * @return Response
     */
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response createExport() {

        makeSureExportIsRunning();

        try {
            // Wait until export is done
            while (isExporting) {
                Thread.sleep(13);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // TODO: Use injected baseURI here after merge with neo4j-rest is
        // complete
        String exportLink = "/" + GraphMLExporter.EXPORT_FOLDER_PATH + "/" + GraphMLExporter.EXPORT_FILE_PATH;

        String entity;
        try {
            entity = JsonRenderers.DEFAULT.render(new ExportRepresentation(new URI(exportLink)));
        } catch (URISyntaxException e) {
            return buildExceptionResponse(Status.INTERNAL_SERVER_ERROR, "Internal error determining export URL.", e, JsonRenderers.DEFAULT);
        }

        return addHeaders(Response.ok(entity, JsonRenderers.DEFAULT.getMediaType())).build();
    }

    //
    // INTERNALS
    //

    /**
     * Starts an export job if one is not already running.
     */
    private synchronized void makeSureExportIsRunning() {

        if (!isExporting) {
            try {
                isExporting = true;
                exportTask.doExport();
                isExporting = false;
            } catch (Exception e) {
                // Catch-all to avoid isExporting getting stuck in "true" mode
                isExporting = false;
                e.printStackTrace();
            }
        }

    }

}
