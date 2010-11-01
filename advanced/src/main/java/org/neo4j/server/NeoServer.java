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
import java.util.HashSet;

import org.apache.commons.configuration.Configuration;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.web.JettyWebServer;
import org.neo4j.server.web.WebServer;

/**
 * Application entry point for the Neo4j Server.
 */
public class NeoServer {
    public static final Logger log = Logger.getLogger(NeoServer.class);

    private static final String WEBSERVICE_PACKAGES = "webservice.packages";
    private static final String DATABASE_LOCATION = "database.location";
    private static final String WEBSERVER_PORT = "webserver.port";


    private static final String NEO_CONFIGDIR_PROPERTY = "org.neo4j.server.properties";
    private static final String DEFAULT_NEO_CONFIGDIR = File.separator + "etc" + File.separator + "neo";

    private Configurator configurator;
    private Database database;
    private WebServer webServer;

    private static NeoServer theServer;

    public NeoServer(Configurator configurator, Database db, WebServer ws) {
        this.configurator = configurator;
        this.database = db;
        this.webServer = ws;
    }

    public static void main(String[] args) {
        Configurator conf = new Configurator(getConfigFile());

        JettyWebServer ws = new JettyWebServer();
        ws.setPort(conf.configuration().getInt(WEBSERVER_PORT));
        //ws.addPackages(getPackagesToLoad(conf.configuration().getStringArray(WEBSERVICE_PACKAGES)));
        ws.addPackages("org.example.coffeeshop, org.example.petshop");

        theServer = new NeoServer(conf, new Database(conf.configuration().getString(DATABASE_LOCATION)), ws);
        theServer.start();
    }

//    private static HashSet<String> getPackagesToLoad(String[] commaSeparatedPackageValues) {
//        StringBuilder sb = new StringBuilder();
//
//        
//        
//        return sb.toString();
//    }

    private static File getConfigFile() {
        return new File(System.getProperty(NEO_CONFIGDIR_PROPERTY, DEFAULT_NEO_CONFIGDIR));
    }

    public void start() {
        log.info("Starting Neo Server on port [%s]", webServer.getPort());

        try {
            webServer.start();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    log.info("Neo Server shutdown initiated by kill signal");
                    shutdown();
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void shutdown() {
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
        } catch (Exception e) {
            log.error("Failed to cleanly shutdown Neo Server on port [%d], database [%s]", portNo, location);
            throw new RuntimeException(e);
        }
    }

    /**
     * Just for functional testing purposes
     */
    static NeoServer server() {
        return theServer;
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
}
