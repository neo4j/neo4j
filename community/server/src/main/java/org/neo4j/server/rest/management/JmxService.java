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
package org.neo4j.server.rest.management;

import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.neo4j.jmx.Kernel;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.repr.InputFormat;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.management.repr.JmxDomainRepresentation;
import org.neo4j.server.rest.management.repr.JmxMBeanRepresentation;
import org.neo4j.server.rest.management.repr.ServiceDefinitionRepresentation;

@Path(JmxService.ROOT_PATH)
public class JmxService implements AdvertisableService
{
    public static final String ROOT_PATH = "server/jmx";

    public static final String DOMAINS_PATH = "/domain";
    public static final String DOMAIN_TEMPLATE = DOMAINS_PATH + "/{domain}";
    public static final String BEAN_TEMPLATE = DOMAIN_TEMPLATE + "/{objectName}";
    public static final String QUERY_PATH = "/query";
    public static final String KERNEL_NAME_PATH = "/kernelquery";
    private final OutputFormat output;

    public JmxService( @Context OutputFormat output, @Context InputFormat input )
    {
        this.output = output;
    }

    @GET
    public Response getServiceDefinition()
    {
        ServiceDefinitionRepresentation serviceDef = new ServiceDefinitionRepresentation( ROOT_PATH );
        serviceDef.resourceUri( "domains", JmxService.DOMAINS_PATH );
        serviceDef.resourceTemplate( "domain", JmxService.DOMAIN_TEMPLATE );
        serviceDef.resourceTemplate( "bean", JmxService.BEAN_TEMPLATE );
        serviceDef.resourceUri( "query", JmxService.QUERY_PATH );
        serviceDef.resourceUri( "kernelquery", JmxService.KERNEL_NAME_PATH );

        return output.ok( serviceDef );
    }

    @GET
    @Path(DOMAINS_PATH)
    public Response listDomains() throws NullPointerException
    {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ListRepresentation domains = ListRepresentation.strings( server.getDomains() );
        return output.ok( domains );
    }

    @GET
    @Path(DOMAIN_TEMPLATE)
    public Response getDomain( @PathParam("domain") String domainName )
    {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        JmxDomainRepresentation domain = new JmxDomainRepresentation( domainName );

        for ( Object objName : server.queryNames( null, null ) )
        {
            if ( objName.toString()
                    .startsWith( domainName ) )
            {
                domain.addBean( (ObjectName) objName );
            }
        }

        return output.ok( domain );
    }

    @GET
    @Path(BEAN_TEMPLATE)
    public Response getBean( @PathParam("domain") String domainName, @PathParam("objectName") String objectName )
    {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        ArrayList<JmxMBeanRepresentation> beans = new ArrayList<JmxMBeanRepresentation>();
        for ( Object objName : server.queryNames( createObjectName( domainName, objectName ), null ) )
        {
            beans.add( new JmxMBeanRepresentation( (ObjectName) objName ) );
        }

        return output.ok( new ListRepresentation( "bean", beans ) );
    }

    private ObjectName createObjectName( final String domainName, final String objectName )
    {
        try
        {
            return new ObjectName( domainName + ":" + URLDecoder.decode( objectName, "UTF-8" ) );
        }
        catch ( MalformedObjectNameException e )
        {
            throw new WebApplicationException( e, 400 );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new WebApplicationException( e, 400 );
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path(QUERY_PATH)
    @SuppressWarnings("unchecked")
    public Response queryBeans( String query )
    {
        try
        {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();

            String json = dodgeStartingUnicodeMarker( query );
            Collection<Object> queries = (Collection<Object>) JsonHelper.readJson( json );

            ArrayList<JmxMBeanRepresentation> beans = new ArrayList<JmxMBeanRepresentation>();
            for ( Object queryObj : queries )
            {
                assert queryObj instanceof String;
                for ( Object objName : server.queryNames( new ObjectName( (String) queryObj ), null ) )
                {
                    beans.add( new JmxMBeanRepresentation( (ObjectName) objName ) );
                }
            }

            return output.ok( new ListRepresentation( "jmxBean", beans ) );
        }
        catch ( JsonParseException e )
        {
            return output.badRequest( e );
        }
        catch ( MalformedObjectNameException e )
        {
            return output.badRequest( e );
        }
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Path(QUERY_PATH)
    public Response formQueryBeans( @FormParam("value") String data )
    {
        return queryBeans( data );
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path(KERNEL_NAME_PATH)
    public Response currentKernelInstance( @Context Database database )
    {
        Kernel kernelBean = database.getGraph().getDependencyResolver().resolveDependency( JmxKernelExtension.class )
                .getSingleManagementBean( Kernel.class );
        return Response.ok( "\"" + kernelBean.getMBeanQuery()
                .toString() + "\"" )
                .type( MediaType.APPLICATION_JSON )
                .build();
    }

    public String getName()
    {
        return "jmx";
    }

    public String getServerPath()
    {
        return ROOT_PATH;
    }

    private static String dodgeStartingUnicodeMarker( String string )
    {
        if ( string != null && string.length() > 0 )
        {
            if ( string.charAt( 0 ) == 0xfeff )
            {
                return string.substring( 1 );
            }
        }
        return string;
    }
}
