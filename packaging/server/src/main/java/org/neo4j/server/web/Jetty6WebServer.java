/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.web;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.SessionManager;
import org.mortbay.jetty.handler.MovedContextHandler;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.HashSessionManager;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.servlet.SessionHandler;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.resource.Resource;
import org.mortbay.thread.QueuedThreadPool;
import org.neo4j.server.NeoServer;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.rest.web.AllowAjaxFilter;

import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;

public class Jetty6WebServer implements WebServer
{
    public static final Logger log = Logger.getLogger( Jetty6WebServer.class );


    private Server jetty;
    private int jettyPort = 80;

    private HashMap<String, String> staticContent = new HashMap<String, String>();
    private HashMap<String, ServletHolder> jaxRSPackages = new HashMap<String, ServletHolder>();
    private NeoServer server;


    public void setNeoServer( NeoServer server )
    {
        this.server = server;
    }

    public void start() {
        jetty = new Server(jettyPort);
        jetty.setStopAtShutdown(true);
        MovedContextHandler redirector = new MovedContextHandler();

        jetty.addHandler( redirector );
        
        loadStaticContent();
        loadJAXRSPackages();

        try {
            jetty.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void stop()
    {
        try
        {
            jetty.stop();
            jetty.join();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setPort(int portNo) {
        jettyPort = portNo;
    }

    public void setMaxThreads(int maxThreads) {
        jetty.setThreadPool(new QueuedThreadPool(maxThreads));
    }

    public void addJAXRSPackages(List<String> packageNames, String mountPoint) {
        // We don't want absolute URIs at this point
        mountPoint = ensureRelativeUri(mountPoint);
        
        // Trim any trailing slash to keep Jetty happy
        mountPoint = trimTrailingSlash(mountPoint);
        
        ServletContainer container = new NeoServletContainer(server);
        ServletHolder servletHolder = new ServletHolder(container);
        servletHolder.setInitParameter("com.sun.jersey.config.property.packages", toCommaSeparatedList(packageNames));        
        servletHolder.setInitParameter(ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS, AllowAjaxFilter.class.getName());
        log.info("Adding JAXRS package %s at [%s]", packageNames, mountPoint);
        jaxRSPackages.put(mountPoint, servletHolder);
    }

    private String trimTrailingSlash(String mountPoint) {
        if(mountPoint.equals("/")) {
            return mountPoint;
        }
        
        if(mountPoint.endsWith("/")) {
            mountPoint = mountPoint.substring(0, mountPoint.length() -1);
        }
        return mountPoint;
    }

    private String ensureRelativeUri(String mountPoint) {
        try {
            URI result = new URI(mountPoint);
            if(result.isAbsolute()) {
                return result.getPath();
            } else {
                return result.toString();
            }
        } catch (URISyntaxException e) {
            log.debug("Unable to translate [%s] to a relative URI in ensureRelativeUri(String mountPoint)", mountPoint);
            return mountPoint;
        }
    }

    public void addStaticContent(String contentLocation, String serverMountPoint) {
        staticContent.put(serverMountPoint, contentLocation);
    }

    private void loadStaticContent() {
        for (String mountPoint : staticContent.keySet()) {
            String contentLocation = staticContent.get(mountPoint);
            log.info("Mounting static content at [%s] from [%s]", mountPoint, contentLocation);
            try {
                final WebAppContext staticContext = new WebAppContext();
                staticContext.setServer(jetty);
                staticContext.setContextPath(mountPoint);
                URL resourceLoc = getClass().getClassLoader().getResource(contentLocation);
                if(resourceLoc != null) {
                    log.info("Found [%s]", resourceLoc);
                    URL url = resourceLoc.toURI().toURL();
                    final Resource resource = Resource.newResource(url);
                    staticContext.setBaseResource(resource);
                    log.info("Mounting static content from [%s] at [%s]", url, mountPoint);
                    jetty.addHandler(staticContext);
                } else {
                    log.error("No static content available for Neo Server at port [%d], management console may not be available.", jettyPort);
                }
            } catch (Exception e) {
                log.error(e);
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    private void loadJAXRSPackages() {
        for (String mountPoint : jaxRSPackages.keySet()) {
            
            ServletHolder servletHolder = jaxRSPackages.get(mountPoint);
            log.info("Mounting servlet at [%s]", mountPoint);
            Context jerseyContext = new Context(jetty, mountPoint);
            SessionManager sm = new HashSessionManager();
            SessionHandler sh = new SessionHandler(sm);
            jerseyContext.addServlet(servletHolder, "/*");
            jerseyContext.setSessionHandler(sh);
        }
    }

    private String toCommaSeparatedList(List<String> packageNames) {
        StringBuilder sb = new StringBuilder();

        for (String str : packageNames) {
            sb.append(str);
            sb.append(", ");
        }

        String result = sb.toString();
        return result.substring(0, result.length() - 2);
    }
}
