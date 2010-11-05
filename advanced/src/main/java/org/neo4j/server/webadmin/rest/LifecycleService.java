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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.neo4j.server.rest.domain.JsonRenderers;
import org.neo4j.server.webadmin.domain.LifecycleRepresentation;
import org.neo4j.server.webadmin.domain.LifecycleServiceRepresentation;

/**
 * REST service to start, stop and restart the neo4j backend.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
@Path( LifecycleService.ROOT_PATH )
public class LifecycleService
{

    public static final String ROOT_PATH = "/lifecycle";
    public static final String STATUS_PATH = "/status";
    public static final String START_PATH = "/start";
    public static final String STOP_PATH = "/stop";
    public static final String RESTART_PATH = "/restart";

    /**
     * TODO: This is a bad way of keeping track of the status of the neo4j
     * server, it would be better to add this capability to DatabaseLocator,
     * which actually has the capability to check if the server is running or
     * not.
     */
    protected static volatile LifecycleRepresentation.Status serverStatus = LifecycleRepresentation.Status.RUNNING;

    @GET
    @Produces( MediaType.APPLICATION_JSON )
    public Response getServiceDefinition( @Context UriInfo uriInfo )
    {

        String entity = JsonRenderers.DEFAULT.render( new LifecycleServiceRepresentation(
                uriInfo.getBaseUri() ) );

        return addHeaders(
                Response.ok( entity, JsonRenderers.DEFAULT.getMediaType() ) ).build();
    }

    @GET
    @Produces( MediaType.APPLICATION_JSON )
    @Path( STATUS_PATH )
    public synchronized Response status()
    {

        LifecycleRepresentation status = new LifecycleRepresentation(
                serverStatus, LifecycleRepresentation.PerformedAction.NONE );
        String entity = JsonRenderers.DEFAULT.render( status );

        return addHeaders(
                Response.ok( entity, JsonRenderers.DEFAULT.getMediaType() ) ).build();

    }

//    @POST
//    @Produces( MediaType.APPLICATION_JSON )
//    @Path( START_PATH )
//    public synchronized Response start()
//    {
//        LifecycleRepresentation status;

//        if ( !DatabaseLocator.databaseIsRunning() )
//        {
//            DatabaseLocator.unblockGraphDatabase();
//            int restPort = WebServerFactory.getDefaultWebServer().getPort();
//            WebServerFactory.getDefaultWebServer().startServer( restPort );
//            ConsoleSessions.destroyAllSessions();
//
//            status = new LifecycleRepresentation(
//                    LifecycleRepresentation.Status.RUNNING,
//                    LifecycleRepresentation.PerformedAction.STARTED );
//        }
//        else
//        {
//            status = new LifecycleRepresentation(
//                    LifecycleRepresentation.Status.RUNNING,
//                    LifecycleRepresentation.PerformedAction.NONE );
//        }
//
//        serverStatus = LifecycleRepresentation.Status.RUNNING;
//
//        String entity = JsonRenderers.DEFAULT.render( status );
//
//        return addHeaders(
//                Response.ok( entity, JsonRenderers.DEFAULT.getMediaType() ) ).build();
//    }
//
//    @POST
//    @Produces( MediaType.APPLICATION_JSON )
//    @Path( STOP_PATH )
//    public synchronized Response stop()
//    {
//        LifecycleRepresentation status;
//
//        if ( DatabaseLocator.databaseIsRunning() )
//        {
//            try
//            {
//                WebServerFactory.getDefaultWebServer().stopServer();
//            }
//            catch ( NullPointerException e )
//            {
//                // REST server was not running
//            }
//            DatabaseLocator.shutdownAndBlockGraphDatabase();
//            status = new LifecycleRepresentation(
//                    LifecycleRepresentation.Status.STOPPED,
//                    LifecycleRepresentation.PerformedAction.STOPPED );
//        }
//        else
//        {
//            status = new LifecycleRepresentation(
//                    LifecycleRepresentation.Status.STOPPED,
//                    LifecycleRepresentation.PerformedAction.NONE );
//        }
//
//        serverStatus = LifecycleRepresentation.Status.STOPPED;
//        String entity = JsonRenderers.DEFAULT.render( status );
//
//        return addHeaders(
//                Response.ok( entity, JsonRenderers.DEFAULT.getMediaType() ) ).build();
//    }
//
//    @POST
//    @Produces( MediaType.APPLICATION_JSON )
//    @Path( RESTART_PATH )
//    public synchronized Response restart()
//    {
//
//        try
//        {
//            WebServerFactory.getDefaultWebServer().stopServer();
//        }
//        catch ( NullPointerException e )
//        {
//            // REST server was not running
//        }
//        DatabaseLocator.shutdownGraphDatabase();
//
//        int restPort = WebServerFactory.getDefaultWebServer().getPort();
//
//        WebServerFactory.getDefaultWebServer().startServer( restPort );
//        ConsoleSessions.destroyAllSessions();
//
//        LifecycleRepresentation status = new LifecycleRepresentation(
//                LifecycleRepresentation.Status.RUNNING,
//                LifecycleRepresentation.PerformedAction.RESTARTED );
//        String entity = JsonRenderers.DEFAULT.render( status );
//
//        serverStatus = LifecycleRepresentation.Status.RUNNING;
//
//        return addHeaders(
//                Response.ok( entity, JsonRenderers.DEFAULT.getMediaType() ) ).build();
//    }
}
