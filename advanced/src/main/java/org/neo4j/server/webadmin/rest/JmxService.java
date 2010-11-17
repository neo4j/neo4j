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

import org.neo4j.management.Kernel;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.renderers.JsonRenderers;
import org.neo4j.server.webadmin.rest.representations.ExceptionRepresentation;
import org.neo4j.server.webadmin.rest.representations.JmxDomainListRepresentation;
import org.neo4j.server.webadmin.rest.representations.JmxDomainRepresentation;
import org.neo4j.server.webadmin.rest.representations.JmxMBeanRepresentation;
import org.neo4j.server.webadmin.rest.representations.JmxServiceRepresentation;


@Path(JmxService.ROOT_PATH)
public class JmxService implements AdvertisableService {

    public static final String ROOT_PATH = "server/jmx";

    public static final String DOMAINS_PATH = "/domain";
    public static final String DOMAIN_PATH = DOMAINS_PATH + "/{domain}";
    public static final String BEAN_PATH = DOMAIN_PATH + "/{objectName}";
    public static final String QUERY_PATH = "/query";
    public static final String KERNEL_NAME_PATH = "/kernelquery";
    

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getServiceDefinition(@Context UriInfo uriInfo) {

        String entity = JsonRenderers.DEFAULT.render(new JmxServiceRepresentation(uriInfo.getBaseUri()));

        return Response.ok(entity).type(MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path(DOMAINS_PATH)
    public Response listDomains() throws NullPointerException {

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        String entity = JsonRenderers.DEFAULT.render(new JmxDomainListRepresentation(server.getDomains()));

        return Response.ok(entity).type(MediaType.APPLICATION_JSON).build();

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

        return Response.ok(entity).type(MediaType.APPLICATION_JSON).build();

    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path(BEAN_PATH)
    public Response getBean(@PathParam("domain") String domainName, @PathParam("objectName") String objectName) {
        try {

            MBeanServer server = ManagementFactory.getPlatformMBeanServer();

            ArrayList<Object> beans = new ArrayList<Object>();
            for (Object objName : server.queryNames(new ObjectName(domainName + ":" + URLDecoder.decode(objectName, "UTF-8")), null)) {
                beans.add((new JmxMBeanRepresentation((ObjectName) objName)).serialize());
            }

            String entity = JsonHelper.createJsonFrom(beans);

            return Response.ok(entity).type(MediaType.APPLICATION_JSON).build();

        } catch (MalformedObjectNameException e) {
            return buildExceptionResponse(Status.BAD_REQUEST, e);
        } catch (UnsupportedEncodingException e) {
            return buildExceptionResponse(Status.INTERNAL_SERVER_ERROR, e);
        }
    }

    private Response buildExceptionResponse(Status errorStatus, Exception e) {
        return Response.status(errorStatus).entity(JsonRenderers.DEFAULT.render(new ExceptionRepresentation(e))).build();
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

            return Response.ok(entity).type(MediaType.APPLICATION_JSON).build();

        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
            return buildExceptionResponse(Status.BAD_REQUEST, e);
        }

    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Path(QUERY_PATH)
    public Response formQueryBeans(@FormParam("value") String data) {
        return queryBeans(data);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path(KERNEL_NAME_PATH)
    public Response currentKernelInstance(@Context Database database) throws DatabaseBlockedException {
        Kernel kernelBean = database.graph.getManagementBean(Kernel.class);
        return Response.ok("\"" + kernelBean.getMBeanQuery().toString() + "\"").type(MediaType.APPLICATION_JSON).build();
    }
    
    public String getName() {
        return "jmx";
    }
    public String getServerPath() {
        return ROOT_PATH;
    }
    
    private static String dodgeStartingUnicodeMarker(String string) {
        if (string != null && string.length() > 0) {
            if (string.charAt(0) == 0xfeff) {
                return string.substring(1);
            }
        }
        return string;
    }
}
