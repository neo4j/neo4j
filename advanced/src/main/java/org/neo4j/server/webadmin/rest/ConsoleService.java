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

import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;
import static org.neo4j.server.webadmin.rest.WebUtils.addHeaders;
import static org.neo4j.server.webadmin.rest.WebUtils.buildExceptionResponse;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response.Status;

import org.apache.log4j.Logger;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonRenderers;
import org.neo4j.server.webadmin.console.ConsoleSessions;
import org.neo4j.server.webadmin.domain.ConsoleServiceRepresentation;

/**
 * A web service that keeps track of client sessions and then passes control
 * down to console worker classes.
 * 
 * Essentially this is the EvaluationServlet from Webling adapted to the Grizzly
 * + Jersey environment of Neo4j WebAdmin.
 * 
 * @author Pavel A. Yaskevich, Jacob Hansson <jacob@voltvoodoo.com> (adapted to
 *         jersey service)
 * 
 */
@Path( ConsoleService.ROOT_PATH )
public class ConsoleService
{

    public static final String ROOT_PATH = "/server/console";

    public static final String EXEC_PATH = "";
    Logger log = Logger.getLogger( ConsoleService.class );

    //
    // PUBLIC
    //

    @GET
    @Produces( MediaType.APPLICATION_JSON )
    public Response getServiceDefinition( @Context UriInfo uriInfo )
    {

        String entity = JsonRenderers.DEFAULT.render( new ConsoleServiceRepresentation(
                uriInfo.getBaseUri() ) );

        return addHeaders(
                Response.ok( entity, JsonRenderers.DEFAULT.getMediaType() ) ).build();
    }

    @POST
    @Produces( MediaType.APPLICATION_JSON )
    @Consumes( MediaType.APPLICATION_FORM_URLENCODED )
    public Response formExec( @Context HttpServletRequest req,
            @FormParam( "value" ) String data )
    {
        return exec( req, data );
    }

    @POST
    @Produces( MediaType.APPLICATION_JSON )
    @Consumes( MediaType.APPLICATION_JSON )
    public Response exec( @Context HttpServletRequest req, String data )
    {
        try
        {
            Map<String, Object> args = jsonToMap( data );

            if ( !args.containsKey( "command" ) )
            {
                throw new IllegalArgumentException(
                        "Missing 'command' parameter in arguments." );
            }

            HttpSession session = req.getSession( true );
            log.info( session.toString() );
            String sessionId = session.getId();
            
            List<String> resultLines = ConsoleSessions.getSession( sessionId ).evaluate(
                    (String) args.get( "command" ) );

            String entity = JsonHelper.createJsonFrom( resultLines );

            return addHeaders( Response.ok( entity ) ).build();
        }
        catch ( IllegalArgumentException e )
        {
            return buildExceptionResponse( Status.BAD_REQUEST,
                    "Invalid request arguments.", e, JsonRenderers.DEFAULT );
        }
    }

}
