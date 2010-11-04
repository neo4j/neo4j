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

package org.neo4j.webadmin.rest;

import static org.neo4j.rest.domain.JsonHelper.jsonToMap;
import static org.neo4j.webadmin.rest.WebUtils.addHeaders;
import static org.neo4j.webadmin.rest.WebUtils.buildExceptionResponse;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.neo4j.rest.domain.JsonParseRuntimeException;
import org.neo4j.rest.domain.JsonRenderers;
import org.neo4j.webadmin.backup.BackupJobDescription;
import org.neo4j.webadmin.backup.BackupManager;
import org.neo4j.webadmin.backup.BackupPerformer;
import org.neo4j.webadmin.domain.BackupJobDescriptionRepresentation;
import org.neo4j.webadmin.domain.BackupServiceRepresentation;
import org.neo4j.webadmin.domain.BackupStatusRepresentation;
import org.neo4j.webadmin.domain.NoBackupFoundationException;
import org.neo4j.webadmin.domain.NoBackupPathException;
import org.neo4j.webadmin.domain.NoSuchPropertyException;
import org.neo4j.webadmin.properties.ServerConfiguration;

/**
 * Lays the groundwork for online backups, allows triggering of backup jobs,
 * exposes backup status.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
@Path( BackupService.ROOT_PATH )
public class BackupService
{
    public static final String ROOT_PATH = "/server/backup";

    public static final String STATUS_PATH = "/status";
    public static final String MANUAL_TRIGGER_PATH = "/trigger";
    public static final String MANUAL_FOUNDATION_TRIGGER_PATH = "/triggerfoundation";
    public static final String JOBS_PATH = "/job";
    public static final String JOB_PATH = JOBS_PATH + "/{id}";
    public static final String JOB_FOUNDATION_TRIGGER_PATH = JOB_PATH
                                                             + "/triggerfoundation";

    protected ServerConfiguration properties;

    //
    // CONSTRUCT
    //

    public BackupService() throws IOException
    {
        properties = ServerConfiguration.getInstance();
    }

    @GET
    @Produces( MediaType.APPLICATION_JSON )
    public Response getServiceDefinition( @Context UriInfo uriInfo )
    {

        String entity = JsonRenderers.DEFAULT.render( new BackupServiceRepresentation(
                uriInfo.getBaseUri() ) );

        return addHeaders(
                Response.ok( entity, JsonRenderers.DEFAULT.getMediaType() ) ).build();
    }

    @Deprecated
    @GET
    @Produces( MediaType.APPLICATION_JSON )
    @Path( STATUS_PATH )
    public synchronized Response status()
    {

        try
        {
            // Is there some sort of action running?

            BackupStatusRepresentation.CurrentAction currentAction = BackupStatusRepresentation.CurrentAction.IDLE;
            Date actionStarted = null;
            Date actionEta = null;
            BackupStatusRepresentation backupInfo = new BackupStatusRepresentation(
                    currentAction, actionStarted, actionEta );
            String entity = JsonRenderers.DEFAULT.render( backupInfo );

            return addHeaders(
                    Response.ok( entity, JsonRenderers.DEFAULT.getMediaType() ) ).build();
        }
        catch ( Exception e )
        {
            return buildExceptionResponse( Status.INTERNAL_SERVER_ERROR,
                    "An unexpected internal server error occurred.", e,
                    JsonRenderers.DEFAULT );
        }
    }

    @POST
    @Path( MANUAL_TRIGGER_PATH )
    @Produces( MediaType.APPLICATION_JSON )
    public Response triggerBackup()
    {
        try
        {

            BackupPerformer.doBackup( getConfiguredBackupPath() );
            return addHeaders( Response.ok() ).build();

        }
        catch ( NoBackupFoundationException e )
        {
            return buildExceptionResponse( Response.Status.BAD_REQUEST,
                    "Configured backup path needs backup foundation.", e,
                    JsonRenderers.DEFAULT );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return buildExceptionResponse( Status.INTERNAL_SERVER_ERROR,
                    "An unexpected internal server error occurred.", e,
                    JsonRenderers.DEFAULT );
        }
    }

    @POST
    @Path( MANUAL_FOUNDATION_TRIGGER_PATH )
    @Produces( MediaType.APPLICATION_JSON )
    public Response triggerBackupFoundation()
    {
        try
        {

            BackupPerformer.doBackupFoundation( getConfiguredBackupPath() );

            return addHeaders( Response.ok() ).build();

        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return buildExceptionResponse( Status.INTERNAL_SERVER_ERROR,
                    "An unexpected internal server error occurred.", e,
                    JsonRenderers.DEFAULT );
        }
    }

    @POST
    @Path( JOB_FOUNDATION_TRIGGER_PATH )
    @Produces( MediaType.APPLICATION_JSON )
    public Response triggerBackupFoundation( @PathParam( "id" ) Integer id )
    {
        try
        {

            BackupJobDescription job = BackupManager.INSTANCE.getJobDescription( id );
            if ( job != null )
            {
                BackupPerformer.doBackupFoundation( new File( job.getPath() ) );
                BackupManager.INSTANCE.getLog().logSuccess( new Date(), job );
            }
            return addHeaders( Response.ok() ).build();

        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return buildExceptionResponse( Status.INTERNAL_SERVER_ERROR,
                    "An unexpected internal server error occurred.", e,
                    JsonRenderers.DEFAULT );
        }
    }

    @GET
    @Path( JOBS_PATH )
    @Produces( MediaType.APPLICATION_JSON )
    public Response listBackupJobs()
    {
        try
        {
            String entity = JsonRenderers.DEFAULT.render( BackupManager.INSTANCE.getConfig() );

            return addHeaders(
                    Response.ok( entity, JsonRenderers.DEFAULT.getMediaType() ) ).build();

        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return buildExceptionResponse( Status.INTERNAL_SERVER_ERROR,
                    "An unexpected internal server error occurred.", e,
                    JsonRenderers.DEFAULT );
        }
    }

    @PUT
    @Path( JOBS_PATH )
    @Produces( MediaType.APPLICATION_JSON )
    @Consumes( MediaType.APPLICATION_JSON )
    public Response setBackupJob( String json )
    {
        try
        {
            BackupJobDescription jobDesc = BackupJobDescriptionRepresentation.deserialize( jsonToMap( json ) );

            BackupManager.INSTANCE.getConfig().setJobDescription( jobDesc );
            BackupManager.INSTANCE.restart();

            return addHeaders( Response.ok() ).build();
        }
        catch ( JsonParseRuntimeException e )
        {
            return buildExceptionResponse( Status.BAD_REQUEST,
                    "The json data you provided is invalid.", e,
                    JsonRenderers.DEFAULT );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return buildExceptionResponse( Status.INTERNAL_SERVER_ERROR,
                    "An unexpected internal server error occurred.", e,
                    JsonRenderers.DEFAULT );
        }
    }

    @DELETE
    @Path( JOB_PATH )
    @Produces( MediaType.APPLICATION_JSON )
    public Response deleteBackupJob( @PathParam( "id" ) Integer id )
    {
        try
        {
            BackupManager.INSTANCE.getConfig().removeJobDescription( id );
            BackupManager.INSTANCE.restart();

            return addHeaders( Response.ok() ).build();

        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return buildExceptionResponse( Status.INTERNAL_SERVER_ERROR,
                    "An unexpected internal server error occurred.", e,
                    JsonRenderers.DEFAULT );
        }
    }

    protected File getConfiguredBackupPath() throws IOException
    {

        try
        {
            String strPath = ServerConfiguration.getInstance().get(
                    "general.backup.path" ).getValue();
            if ( strPath.length() > 0 )
            {
                return new File( strPath );
            }
            else
            {
                throw new NoBackupPathException(
                        "The backup path property is empty." );
            }
        }
        catch ( NoSuchPropertyException e )
        {
            throw new NoBackupPathException(
                    "The backup path property is empty." );
        }
    }
}
