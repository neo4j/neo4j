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

package org.neo4j.server.web;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.DefaultHandler;
import org.mortbay.jetty.handler.HandlerList;
import org.mortbay.jetty.handler.ResourceHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;

import com.sun.jersey.spi.container.servlet.ServletContainer;

public class Jetty6WebServer implements WebServer {

    private static final String DEFAULT_CONTENT_CONTEXT_BASE = "";
    private static final String DEFAULT_CONTENT_RESOURCE_PATH = "html";
    
    private Server jetty;
    private int jettyPort = 80;
    private ServletHolder jerseyServletHolder = new ServletHolder(ServletContainer.class);
    
    private String contentResourcePath = DEFAULT_CONTENT_RESOURCE_PATH;
    private String contentContextBase = DEFAULT_CONTENT_CONTEXT_BASE;

    public void start() {
        jetty = new Server(jettyPort);
        jetty.setStopAtShutdown(true);

        
        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setResourceBase(contentResourcePath);
        resourceHandler.setWelcomeFiles(new String[]{ "welcome.html" });
        
        resourceHandler.setResourceBase(contentContextBase);
        
        
        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[] { resourceHandler, new DefaultHandler() });
        jetty.addHandler(handlers);

        
        
//        Context contentContext = new Context(jetty, "/" + contentContextBase);
//        contentContext.setContextPath(contentResourcePath);
//        contentContext.setResourceBase(contentContextBase);
//        contentContext.setWelcomeFiles(new String[] {"welcome.html"});
//        
//        ServletHolder sh = new ServletHolder();
//        sh.setServlet(new DefaultServlet());
//        contentContext.addServlet(sh, "/*");
        
            
//        Context jerseyContext = new Context(jetty, "/");
//        jerseyContext.addServlet(jerseyServletHolder, "/*");

        try {
            jetty.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        try {
            jetty.stop();
            jetty.join();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setPort(int portNo) {
        jettyPort = portNo;
    }

    public int getPort() {
        return jettyPort;
    }

    public void addPackages(String packageNames) {
        if (packageNames == null) {
            return;
        }

        jerseyServletHolder.setInitParameter("com.sun.jersey.config.property.packages", packageNames);

    }

    public void setMaxThreads(int maxThreads) {
        jetty.setThreadPool(new QueuedThreadPool(maxThreads));
    }

    public URI getBaseUri() throws URISyntaxException {
        StringBuilder sb = new StringBuilder();

        sb.append("http");
        if (jettyPort == 443) {
            sb.append("s");
        }
        sb.append("://");
        try {
            sb.append(InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            sb.append("localhost");
        }

        if (jettyPort != 80) {
            sb.append(":");
            sb.append(jettyPort);
        }

        sb.append("/");
        
        return new URI(sb.toString());
    }

    public URI getWelcomeUri() throws URISyntaxException {
        if(contentContextBase == "") {
            return new URI(getBaseUri().toString() + "welcome.html");
        } else {
            return new URI(getBaseUri().toString() + this.contentContextBase + "/" + "welcome.html");
        }
    }

    public void setStaticContentDir(String staticContentResourcePath) {
        this.contentResourcePath = staticContentResourcePath;
    }

    public void setStaticContextRoot(String contextRoot) {
        this.contentContextBase = contextRoot;
    }
}
