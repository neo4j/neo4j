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
import static org.neo4j.server.webadmin.rest.WebUtils.dodgeStartingUnicodeMarker;

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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response.Status;

import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.management.Kernel;
import org.neo4j.server.NeoServer;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonRenderers;
import org.neo4j.server.webadmin.domain.JmxDomainListRepresentation;
import org.neo4j.server.webadmin.domain.JmxDomainRepresentation;
import org.neo4j.server.webadmin.domain.JmxMBeanRepresentation;
import org.neo4j.server.webadmin.domain.JmxServiceRepresentation;

/**
 * Exposes the underlying neo4j instances JMX interface.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */

@Path(JmxService.ROOT_PATH)
public class JmxService {

    public static final String ROOT_PATH = "/server/jmx";

    public static final String DOMAINS_PATH = "/domain";
    public static final String DOMAIN_PATH = DOMAINS_PATH + "/{domain}";
    public static final String BEAN_PATH = DOMAIN_PATH + "/{objectName}";
    public static final String QUERY_PATH = "/query";
    public static final String KERNEL_NAME_PATH = "/kernelquery";

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getServiceDefinition(@Context UriInfo uriInfo) {

        String entity = JsonRenderers.DEFAULT.render(new JmxServiceRepresentation(uriInfo.getBaseUri()));

        return addHeaders(Response.ok(entity, JsonRenderers.DEFAULT.getMediaType())).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path(DOMAINS_PATH)
    public Response listDomains() throws NullPointerException {

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        String entity = JsonRenderers.DEFAULT.render(new JmxDomainListRepresentation(server.getDomains()));

        return addHeaders(Response.ok(entity, JsonRenderers.DEFAULT.getMediaType())).build();

    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path(DOMAIN_PATH)
    public Response getDomain(@PathParam("domain") String domainName) throws NullPointerException {

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        JmxDomainRepresentation domain = new JmxDomainRepresentation(domainName);

        for (Object objName : server.queryNames(null, null)) {
            if (objName.toString().startsWith(domainName)) {
                domain.addBean((ObjectName) objName);
            }
        }

        String entity = JsonRenderers.DEFAULT.render(domain);

        return addHeaders(Response.ok(entity, JsonRenderers.DEFAULT.getMediaType())).build();

    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path(BEAN_PATH)
    public Response getBean(@PathParam("domain") String domainName, @PathParam("objectName") String objectName) {
        try {

            MBeanServer server = ManagementFactory.getPlatformMBeanServer();

            ArrayList<Object> beans = new ArrayList<Object>();
            for (Object objName : server.queryNames(new ObjectName(domainName + ":" + URLDecoder.decode(objectName, WebUtils.UTF8)), null)) {
                beans.add((new JmxMBeanRepresentation((ObjectName) objName)).serialize());
            }

            String entity = JsonHelper.createJsonFrom(beans);

            return addHeaders(Response.ok(entity, JsonRenderers.DEFAULT.getMediaType())).build();

        } catch (MalformedObjectNameException e) {
            return buildExceptionResponse(Status.BAD_REQUEST, "Invalid bean name.", e, JsonRenderers.DEFAULT);
        } catch (UnsupportedEncodingException e) {
            return buildExceptionResponse(Status.INTERNAL_SERVER_ERROR, "UTF-8 encoding not supported by host system.", e, JsonRenderers.DEFAULT);
        }
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path(QUERY_PATH)
    @SuppressWarnings("unchecked")
    public Response queryBeans(String query) {

        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();

            String json = dodgeStartingUnicodeMarker(query);
            Collection<Object> queries = (Collection<Object>) JsonHelper.jsonToSingleValue(json);

            ArrayList<Object> beans = new ArrayList<Object>();
            for (Object queryObj : queries) {
                assert queryObj instanceof String;
                for (Object objName : server.queryNames(new ObjectName((String) queryObj), null)) {
                    beans.add((new JmxMBeanRepresentation((ObjectName) objName)).serialize());
                }
            }

            String entity = JsonHelper.createJsonFrom(beans);

            return addHeaders(Response.ok(entity, JsonRenderers.DEFAULT.getMediaType())).build();

        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
            return buildExceptionResponse(Status.BAD_REQUEST, "Invalid query.", e, JsonRenderers.DEFAULT);
        }

    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Path(QUERY_PATH)
    public Response formQueryBeans(@FormParam("value") String data) {
        return queryBeans(data);
    }

    /**
     * This returns the instance name for the current "main" neo4j kernel, ie.
     * the one that runs behind the REST server.
     * 
     * @return
     * @throws DatabaseBlockedException
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path(KERNEL_NAME_PATH)
    public Response currentKernelInstance() throws DatabaseBlockedException {
        // if ( DatabaseLocator.isLocalDatabase() )
        // {
        Kernel kernelBean = ((EmbeddedGraphDatabase) NeoServer.server().database().db).getManagementBean(Kernel.class);
        return addHeaders(Response.ok("\"" + kernelBean.getMBeanQuery().toString() + "\"", JsonRenderers.DEFAULT.getMediaType())).build();
        // }
        // else
        // {
        // return addHeaders(
        // Response.ok( "null", JsonRenderers.DEFAULT.getMediaType() )
        // ).build();
        // }
    }
}
