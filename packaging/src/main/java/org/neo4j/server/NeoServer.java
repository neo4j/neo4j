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
import org.tanukisoftware.wrapper.WrapperListener;

import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Application entry point for the Neo4j Server.
 */
public class NeoServer implements WrapperListener {
    public static final Logger log = Logger.getLogger(NeoServer.class);

    public static final String WEBSERVER_PORT_PROPERTY_KEY = "org.neo4j.webserver.port";

    public static final String REST_API_SERVICE_NAME = "/db/data";
    public static final String REST_API_PACKAGE = "org.neo4j.server.rest.web";
    
    public static final String NEO_CONFIG_FILE_KEY = "org.neo4j.server.properties";
    public static final String DEFAULT_NEO_CONFIG_DIR = File.separator + "etc" + File.separator + "neo";
    
    public static final String DATABASE_LOCATION_PROPERTY_KEY = "org.neo4j.database.location";
    public static final String WEBADMIN_NAMESPACE_PROPERTY_KEY = "org.neo4j.server.webadmin.";
    

    public static final String WEB_ADMIN_REST_API_SERVICE_NAME = "/db/manage";
    protected static final String WEB_ADMIN_REST_API_PACKAGE = "org.neo4j.server.webadmin.rest";
    protected static final String WEB_ADMIN_PATH = "/webadmin";

    public static final String STATIC_CONTENT_LOCATION = "html";
    public static final int DEFAULT_WEBSERVER_PORT = 7474;

    protected static NeoServer theServer;

    protected Configurator configurator;
    protected Database database;
    protected WebServer webServer;

    protected int webServerPort;
    public static final String EXPORT_BASE_PATH = "org.neo4j.export.basepath";

    /**
     * For test purposes only.
     */
    protected NeoServer(Configurator configurator, Database db, WebServer ws) {
        this.configurator = configurator;
        this.database = db;
        this.webServer = ws;
    }

    /**
     * This only works if the server has been started via NeoServer.main(...)
     * @return
     */
    public static NeoServer getServer_FOR_TESTS_ONLY_KITTENS_DIE_WHEN_YOU_USE_THIS() {
        return theServer;
    }

    protected NeoServer() {
        StartupHealthCheck healthCheck = new StartupHealthCheck(new ConfigFileMustBePresentRule());
        if (!healthCheck.run()) {
            throw new StartupHealthCheckFailedException(healthCheck.failedRule());
        }
    }

    /**
     * Convenience method which calls start with a null argument
     * 
     * @return
     */
    public Integer start() {
        return start(null);
        
    }

    public Integer start(String[] args) {
        
        try {
            this.configurator = new Configurator(new Validator(new DatabaseLocationMustBeSpecifiedRule()), getConfigFile());
            webServerPort = configurator.configuration().getInt(WEBSERVER_PORT_PROPERTY_KEY, DEFAULT_WEBSERVER_PORT);

            this.database = new Database(configurator.configuration().getString(DATABASE_LOCATION_PROPERTY_KEY));

            this.webServer = new Jetty6WebServer( database.db );

            log.info("Starting Neo Server on port [%s]", webServerPort);
            webServer.setPort(webServerPort);

            log.info("Mounting webadmin at [%s]", WEB_ADMIN_PATH);
            webServer.addStaticContent(STATIC_CONTENT_LOCATION, WEB_ADMIN_PATH);

            log.info("Mounting management API at [%s]", WEB_ADMIN_REST_API_SERVICE_NAME);
            webServer.addJAXRSPackages(listFrom(new String[] { WEB_ADMIN_REST_API_PACKAGE }), WEB_ADMIN_REST_API_SERVICE_NAME);

            log.info("Mounting REST API at [%s]", REST_API_SERVICE_NAME);            
            webServer.addJAXRSPackages(listFrom(new String[] { REST_API_PACKAGE }), REST_API_SERVICE_NAME);

           // Temporary coffee shop
            webServer.addJAXRSPackages(listFrom(new String[] {"org.example.coffeeshop"}), "/");
            
            webServer.start();
            
            log.info("Started Neo Server on port [%s]", restApiUri().getPort());
            
            return null; // This is for the service wrapper, and though it looks weird, it's correct
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Failed to start Neo Server on port [%s]", webServerPort);
            e.printStackTrace();
            return 1;
        }
    }
    
    public String baseUri() throws UnknownHostException {
            StringBuilder sb = new StringBuilder();
            sb.append("http");
            if (webServerPort == 443) {
                sb.append("s");

            }
            sb.append("://");
            sb.append(InetAddress.getLocalHost().getCanonicalHostName());

            if (webServerPort != 80) {
                sb.append(":");
                sb.append(webServerPort);
            }
            sb.append("/");

        return sb.toString();
    }
    
    private URI generateUriFor(String serviceName) {
        if(serviceName.startsWith("/")) {
            serviceName = serviceName.substring(1);
        }
        StringBuilder sb = new StringBuilder();
        try {
            sb.append(baseUri());
            sb.append(serviceName);
            sb.append("/");
            
            return new URI(sb.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public URI managementApiUri() {
           return generateUriFor(WEB_ADMIN_REST_API_SERVICE_NAME);
    }

    public URI restApiUri() {
        return generateUriFor(REST_API_SERVICE_NAME);
    }
    
    public URI webadminUri() {
        return generateUriFor(WEB_ADMIN_PATH);
    }

    /**
     * Convenience method which calls stop with a normal return code
     */
    public void stop() {
        stop(0);
    }

    public int stop(int stopArg) {
        String location = "unknown";
        try {

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
    
    public static synchronized void shutdown() {
        if(theServer != null) {
            theServer.stop();
            theServer = null;
        }
    }


    public Database database() {
        return database;
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

    protected static File getConfigFile() {
        return new File(System.getProperty(NEO_CONFIG_FILE_KEY, DEFAULT_NEO_CONFIG_DIR));
    }

    protected List<String> listFrom(String[] strings) {
        ArrayList<String> al = new ArrayList<String>();

        if (strings != null) {
            for (String str : strings) {
                al.add(str);
            }
        }

        return al;
    }

    public static void main(String args[]) {
        theServer = new NeoServer();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Neo Server shutdown initiated by kill signal");
                if(theServer != null) {
                    theServer.stop();
                }
                shutdown();
            }
        });

        theServer.start(args);
    }

    public void reboot() {
        stop();
        start();
    }
}
