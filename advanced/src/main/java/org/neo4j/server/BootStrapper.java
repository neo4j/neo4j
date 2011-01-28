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
package org.neo4j.server;

import java.io.File;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.startup.healthcheck.ConfigFileMustBePresentRule;
import org.neo4j.server.startup.healthcheck.StartupHealthCheck;
import org.neo4j.server.web.Jetty6WebServer;

public class BootStrapper
{

    public static final Logger log = Logger.getLogger(BootStrapper.class);

    public static final Integer OK = 0;
    public static final Integer WEB_SERVER_STARTUP_ERROR_CODE = 1;
    public static final Integer GRAPH_DATABASE_STARTUP_ERROR_CODE = 2;
    public static final String KEY_LOG4J_CONFIG_XML_PATH = "log4j.config.xml.path";

    private NeoServer server;

    public void controlEvent(int arg) {
        // Do nothing, required by the WrapperListener interface
    }

    public Integer start() {
        return start(null);

    }

    public Integer start(String[] args) {
        try {
            StartupHealthCheck startupHealthCheck = new StartupHealthCheck(new ConfigFileMustBePresentRule());
            Jetty6WebServer webServer = new Jetty6WebServer();
            server = new NeoServer(new AddressResolver(), startupHealthCheck,
                    getConfigFile(),
                    webServer);
            server.start();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    log.info("Neo4j Server shutdown initiated by kill signal");
                    if (server != null) {
                        server.stop();
                    }
                }
            });

            return OK;
        } catch (TransactionFailureException tfe) {
            tfe.printStackTrace();
            log.error( String.format( "Failed to start Neo Server on port [%d], because ", server.restApiUri().getPort() ) + tfe
                + ". Another process may be using database location " + server.getDatabase().getLocation() );
            return GRAPH_DATABASE_STARTUP_ERROR_CODE;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Failed to start Neo Server on port [%s]", server.getWebServerPort());
            return WEB_SERVER_STARTUP_ERROR_CODE;
        }
    }

    public void stop() {
        stop(0);
    }

    public int stop(int stopArg) {
        String location = "unknown location";
        try {

            log.info("Successfully shutdown Neo Server on port [%d], database [%s]", server.getWebServerPort(), location);
            return 0;
        } catch (Exception e) {
            log.error("Failed to cleanly shutdown Neo Server on port [%d], database [%s]. Reason [%s] ", server.getWebServerPort(), location, e.getMessage());
            return 1;
        }
    }

    public NeoServer getServer()
    {
        return server;
    }

    protected static File getConfigFile() {
        return new File(System.getProperty(Configurator.NEO_SERVER_CONFIG_FILE_KEY, Configurator.DEFAULT_CONFIG_DIR));
    }

    public static void main(String[] args) {
        configureLogging();
        BootStrapper bootstrapper = new BootStrapper();
        bootstrapper.start(args);
    }

    private static void configureLogging()
    {
        String log4jConfigPath = System.getProperty( KEY_LOG4J_CONFIG_XML_PATH );
        if ( log4jConfigPath != null )
        {
            DOMConfigurator.configure( log4jConfigPath );
            System.out.println( "Configured logging from file: " + log4jConfigPath );
        }
        else
        {
            BasicConfigurator.configure();
            System.out.println( "Configured default logging." );
        }
    }
}
