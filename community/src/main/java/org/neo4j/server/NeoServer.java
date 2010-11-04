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

import org.apache.commons.configuration.Configuration;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.validation.DatabaseLocationMustBeSpecifiedRule;
import org.neo4j.server.configuration.validation.Validator;
import org.neo4j.server.database.Database;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.startup.healthcheck.ConfigFileMustBePresentRule;
import org.neo4j.server.startup.healthcheck.StartupHealthCheck;
import org.neo4j.server.startup.healthcheck.StartupHealthCheckFailedException;
import org.neo4j.server.web.Jetty6WebServer;
import org.neo4j.server.web.WebServer;
import org.neo4j.webadmin.backup.BackupManager;
import org.neo4j.webadmin.rrd.RrdManager;
import org.neo4j.webadmin.rrd.RrdSampler;
import org.tanukisoftware.wrapper.WrapperListener;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Application entry point for the Neo4j Server.
 */
public class NeoServer implements WrapperListener {
    
    public static final String WEBADMIN_NAMESPACE = "org.neo4j.server.webadmin.";

    static final String MANAGE_PATH = "/db/manage";

    static final String WEBADMIN_PATH = "/webadmin";

    static final String WEB_ADMIN_REST_API_PACKAGE = "org.neo4j.webadmin.rest";

    public static final String REST_API_PATH = "/db/data";
    public static final String REST_API_PACKAGE = "org.neo4j.rest.web";

    public static final Logger log = Logger.getLogger(NeoServer.class);

    public static final String NEO_CONFIG_FILE_PROPERTY = "org.neo4j.server.properties";
    public static final String DEFAULT_NEO_CONFIGDIR = File.separator + "etc" + File.separator + "neo";

    public static final String DATABASE_LOCATION = "org.neo4j.database.location";
    private static final String WEBSERVER_PORT = "org.neo4j.webserver.port";
    private static final int DEFAULT_WEBSERVER_PORT = 7474;

    private Configurator configurator;
    private Database database;
    private WebServer webServer;

    private int webServerPort;
    public static NeoServer INSTANCE;

    /**
     * For test purposes only.
     */
    NeoServer(Configurator configurator, Database db, WebServer ws) {
        this.configurator = configurator;
        this.database = db;
        this.webServer = ws;
    }

    public NeoServer() {
        StartupHealthCheck healthCheck = new StartupHealthCheck(new ConfigFileMustBePresentRule());
        if(!healthCheck.run()) {
            throw new StartupHealthCheckFailedException("Startup healthcheck failed, server is not properly configured. Check logs for details.");
        }
        
        this.configurator = new Configurator(new Validator(new DatabaseLocationMustBeSpecifiedRule()), getConfigFile());
        this.webServer = new Jetty6WebServer();
        this.database = new Database(configurator.configuration().getString(DATABASE_LOCATION));
    }

    public Integer start(String[] args) {
        INSTANCE = this;
        
        webServerPort = configurator.configuration().getInt(WEBSERVER_PORT, DEFAULT_WEBSERVER_PORT);
        try {
            //Start webserver
            startWebserver();

            
            //start the others
            //log.info(  "Starting backup scheduler.." );

            //BackupManager.INSTANCE.start();

            System.out.println( "Starting round-robin system state sampler.." );

            RrdSampler.INSTANCE.start();

            
            return null; //yes, that's right!
        } catch (Exception e) {
            log.error("Failed to start Neo Server on port [%s]", webServerPort);
            return 1;
        }
    }

    private void startWebserver()
    {
        webServer.setPort(webServerPort);
        // webadmin assumes root
        log.info("Mounting static html at [%s]", WEBADMIN_PATH);
        webServer.addStaticContent("html", WEBADMIN_PATH);
        
        log.info("Mounting REST at [%s]", REST_API_PATH);
        webServer.addJAXRSPackages(listFrom(new String[] {REST_API_PACKAGE}), REST_API_PATH);

        log.info("Mounting manage API at [%s]", MANAGE_PATH);
        webServer.addJAXRSPackages(listFrom(new String[] {WEB_ADMIN_REST_API_PACKAGE}), MANAGE_PATH);
        
        webServer.start();
        
        log.info("Started Neo Server on port [%s]", webServerPort);
    }

    private List<String> listFrom(String[] strings) {
        ArrayList<String> al = new ArrayList<String>();
        
        if(strings != null) {
            for(String str : strings) {
                al.add(str);
            }
         }
        
        return al;
    }

    protected void stop() {
        stop(0);
    }
    
    public int stop(int stopArg) {
        String location = "unknown";
        try {
         // Kill the round robin sampler
            System.out.println( "\nShutting down the round robin database" );
            RrdSampler.INSTANCE.stop();
            RrdManager.getRrdDB().close();
            if (database != null) {
                location = database.getLocation();
                database.shutdown();
                database = null;
            }
            if (webServer != null) {
                webServer.stop();
                webServer = null;
            }
            configurator = null;
            
            log.info("Successfully shutdown Neo Server on port [%d], database [%s]", webServerPort, location);
            return 0;
        } catch (Exception e) {
            log.error("Failed to cleanly shutdown Neo Server on port [%d], database [%s]. Reason [%s] ", webServerPort, location, e.getMessage());
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
        return new File(System.getProperty(NEO_CONFIG_FILE_PROPERTY, DEFAULT_NEO_CONFIGDIR));
    }

    public static void main(String args[]) {
        final NeoServer neo = new NeoServer();

        Runtime.getRuntime().addShutdownHook(new Thread() {
	            @Override
	            public void run() {
	                log.info("Neo Server shutdown initiated by kill signal");
	                neo.stop();
	            }
	        });

        neo.start(args);
    }
}
