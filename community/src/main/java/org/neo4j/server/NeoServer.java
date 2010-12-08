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
import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.management.MalformedObjectNameException;

import org.apache.commons.configuration.Configuration;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.validation.DatabaseLocationMustBeSpecifiedRule;
import org.neo4j.server.configuration.validation.Validator;
import org.neo4j.server.database.Database;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.osgi.OSGiContainer;
import org.neo4j.server.rrd.RrdFactory;
import org.neo4j.server.startup.healthcheck.StartupHealthCheck;
import org.neo4j.server.startup.healthcheck.StartupHealthCheckFailedException;
import org.neo4j.server.web.Jetty6WebServer;
import org.neo4j.server.web.WebServer;
import org.osgi.framework.BundleException;
import org.rrd4j.core.RrdDb;

public class NeoServer {

    public static final Logger log = Logger.getLogger(NeoServer.class);

    private File configFile;
    private Configurator configurator;
    private Database database;
    private WebServer webServer;
    private final StartupHealthCheck startupHealthCheck;

    private RoundRobinJobScheduler jobScheduler = new RoundRobinJobScheduler();

    private AddressResolver addressResolver;

    public NeoServer(StartupHealthCheck startupHealthCheck, File configFile, WebServer ws) {
        this.startupHealthCheck = startupHealthCheck;
        this.configFile = configFile;
        this.webServer = ws;
    }

    public NeoServer(AddressResolver addressResolver, StartupHealthCheck startupHealthCheck, File configFile, WebServer webServer) {
        this(startupHealthCheck, configFile, webServer);
        this.addressResolver = addressResolver;
    }

    public void start() {
        startupHealthCheck();
        validateConfiguration();
        startDatabase();
        try {
            startRoundRobinDB();
            startOsgiContainer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        startWebServer();
    }

    private void startupHealthCheck() {
        if (!startupHealthCheck.run()) {
            throw new StartupHealthCheckFailedException(startupHealthCheck.failedRule());
        }
    }

    private void validateConfiguration() {
        this.configurator = new Configurator(new Validator(new DatabaseLocationMustBeSpecifiedRule()), configFile);
    }

    private void startDatabase() {
        String dbLocation = new File(configurator.configuration().getString(Configurator.DATABASE_LOCATION_PROPERTY_KEY)).getAbsolutePath();
        Map<String, String> databaseTuningProperties = configurator.getDatabaseTuningProperties();
        if(databaseTuningProperties != null) {
            this.database = new Database(dbLocation, databaseTuningProperties);
        } else {
            this.database = new Database(dbLocation);
        }
    }

    public Configuration getConfiguration() {
        return configurator.configuration();
    }

    private void startWebServer() {

        int webServerPort = getWebServerPort();
        this.webServer = new Jetty6WebServer();
        this.webServer.setNeoServer(this);

        log.info("Starting Neo Server on port [%s]", webServerPort);
        webServer.setPort(webServerPort);

        log.info("Mounting webadmin at [%s]", Configurator.WEB_ADMIN_PATH);
        webServer.addStaticContent(Configurator.STATIC_WEB_CONTENT_LOCATION, Configurator.WEB_ADMIN_PATH);

        log.info("Mounting management API at [%s]", Configurator.WEB_ADMIN_REST_API_PATH);
        webServer.addJAXRSPackages(listFrom(new String[] { Configurator.WEB_ADMIN_REST_API_PACKAGE }), Configurator.WEB_ADMIN_REST_API_PATH);

        log.info("Mounting REST API at [%s]", Configurator.REST_API_PATH);
        webServer.addJAXRSPackages(listFrom(new String[] { Configurator.REST_API_PACKAGE }), Configurator.REST_API_PATH);

        try {
            webServer.start();
        } catch (Exception e) {
            log.error("Failed to start Neo Server on port [%d], reason [%s]", getWebServerPort(), e.getMessage());
        }
    }

    protected int getWebServerPort() {
        return configurator.configuration().getInt(Configurator.WEBSERVER_PORT_PROPERTY_KEY, Configurator.DEFAULT_WEBSERVER_PORT);
    }

    private void startRoundRobinDB() throws MalformedObjectNameException, IOException {
        RrdDb rrdDb = RrdFactory.createRrdDbAndSampler(database.graph, jobScheduler);
        database.setRrdDb(rrdDb);
    }

    private void startOsgiContainer() throws BundleException {
        // Start embedded OSGi container, maybe
        boolean osgiServerShouldStart = configurator.configuration().getBoolean(Configurator.ENABLE_OSGI_SERVER_PROPERTY_KEY, false);
        if (osgiServerShouldStart) {
            String bundleDirectory = configurator.configuration().getString(Configurator.OSGI_BUNDLE_DIR_PROPERTY_KEY, "../");
            String cacheDirectory = configurator.configuration().getString(Configurator.OSGI_CACHE_DIR_PROPERTY_KEY, "../");
            OSGiContainer container = new OSGiContainer(bundleDirectory, cacheDirectory);
            container.start();
        }
    }

    public void stop() {
        try {
            stopJobs();
            stopDatabase();
            stopWebServer();
            log.info("Successfully shutdown Neo Server on port [%d], database [%s]", getWebServerPort(), getDatabase().getLocation());
        } catch (Exception e) {
            log.warn("Failed to cleanly shutdown Neo Server on port [%d], database [%s]. Reason: %s", 
                    getWebServerPort(), getDatabase().getLocation(),
                    e.getMessage());
        }
    }

    private void stopWebServer() {
        if (webServer != null) {
            webServer.stop();
        }
    }

    private void stopDatabase() {
        if (database != null) {
            database.shutdown();
        }
    }

    private void stopJobs() {
        jobScheduler.stopJobs();
    }

    private List<String> listFrom(String[] strings) {
        ArrayList<String> al = new ArrayList<String>();

        if (strings != null) {
            al.addAll(Arrays.asList(strings));
        }

        return al;
    }

    public Database getDatabase() {
        return database;
    }

    public String baseUri() throws UnknownHostException {
        StringBuilder sb = new StringBuilder();
        sb.append("http");
        int webServerPort = getWebServerPort();
        if (webServerPort == 443) {
            sb.append("s");

        }
        sb.append("://");
        sb.append(addressResolver.getHostname());

        if (webServerPort != 80) {
            sb.append(":");
            sb.append(webServerPort);
        }
        sb.append("/");

        return sb.toString();
    }

    private URI generateUriFor(String serviceName) {
        if (serviceName.startsWith("/")) {
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
        return generateUriFor(Configurator.WEB_ADMIN_REST_API_PATH);
    }

    public URI restApiUri() {
        return generateUriFor(Configurator.REST_API_PATH);
    }

    public URI webadminUri() {
        return generateUriFor(Configurator.WEB_ADMIN_PATH);
    }
}