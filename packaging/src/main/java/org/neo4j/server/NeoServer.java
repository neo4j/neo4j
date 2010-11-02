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

package org.neo4j.server;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.web.JettyWebServer;
import org.neo4j.server.web.WebServer;
import org.tanukisoftware.wrapper.WrapperListener;

/**
 * Application entry point for the Neo4j Server.
 */
public class NeoServer implements WrapperListener {
    public static final Logger log = Logger.getLogger(NeoServer.class);

    private static final String WEBSERVICE_PACKAGES = "org.neo4j.webservice.packages";
    private static final String DATABASE_LOCATION = "org.neo4j.database.location";
    private static final String WEBSERVER_PORT = "org.neo4j.webserver.port";


    private static final String NEO_CONFIGDIR_PROPERTY = "org.neo4j.server.properties";
    private static final String DEFAULT_NEO_CONFIGDIR = File.separator + "etc" + File.separator + "neo";

    private Configurator configurator;
    private Database database;
    private WebServer webServer;

    private static NeoServer theServer;

    /**
     * For test purposes only.
     */
    NeoServer(Configurator configurator, Database db, WebServer ws) {
        this.configurator = configurator;
        this.database = db;
        this.webServer = ws;
    }

    public NeoServer() {
        this.configurator = new Configurator(getConfigFile());
        
        JettyWebServer ws = new JettyWebServer();
        ws.setPort(configurator.configuration().getInt(WEBSERVER_PORT));
        ws.addPackages(convertPropertiesToSingleString(configurator.configuration().getStringArray(WEBSERVICE_PACKAGES)));
        this.webServer = ws;

        this.database = new Database(configurator.configuration().getString(DATABASE_LOCATION));
    }

    public static void main(String[] args) {        
        theServer = new NeoServer();
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Neo Server shutdown initiated by kill signal");
                theServer.stop();
            }
        });

        theServer.start(args);
    }

    /**
     * Just for functional testing purposes
     */
    static NeoServer server() {
        return theServer;
    }

    public Integer start(String[] args) {
        try {
            webServer.start();
            log.info("Started Neo Server on port [%s]", webServer.getPort());
            return 0;
        } catch (Exception e) {
            log.error("Failed to start Neo Server on port [%s]", webServer.getPort());
            return 1;
        }
    }

    protected void stop() {
        stop(0);
    }
    
    public int stop(int stopArg) {
        int portNo = -1;
        String location = "unknown";
        try {
            if (database != null) {
                location = database.getLocation();
                database.shutdown();
                database = null;
            }
            if (webServer != null) {
                portNo = webServer.getPort();
                webServer.shutdown();
                webServer = null;
            }
            configurator = null;
            log.info("Successfully shutdown Neo Server on port [%d], database [%s]", portNo, location);
            return 0;
        } catch (Exception e) {
            log.error("Failed to cleanly shutdown Neo Server on port [%d], database [%s]", portNo, location);
            return 1;
        }
    }

    public GraphDatabaseService database() {
        return database.db;
    }

    public WebServer webServer() {
        return webServer;
    }

    public Configuration configuration() {
        return configurator.configuration();
    }

    public void controlEvent(int controlArg) {
       // Do nothing for now, this is needed by the WrapperListener interface
    }
    
    private static File getConfigFile() {
        return new File(System.getProperty(NEO_CONFIGDIR_PROPERTY, DEFAULT_NEO_CONFIGDIR));
    }

    private static String convertPropertiesToSingleString(String[] properties) {
        if(properties == null || properties.length < 1) {
            return null;
        }
        
        StringBuilder sb = new StringBuilder();
        
        // Nasty string hacks - commons config gives us nice-ish collections, but Jetty wants to load with stringified properties
        for(String s : properties) {
            sb.append(s);
            sb.append(", ");
        }
        
        String str = sb.toString();
        return str.substring(0, str.length() -2);
    }
}
