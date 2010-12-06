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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.server.ServerTestUtils.createTempDir;
import static org.neo4j.server.ServerTestUtils.createTempPropertyFile;
import static org.neo4j.server.ServerTestUtils.writePropertyToFile;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.startup.healthcheck.StartupHealthCheck;
import org.neo4j.server.startup.healthcheck.StartupHealthCheckRule;
import org.neo4j.server.web.Jetty6WebServer;

public class ServerBuilder {

    private String portNo = "7474";
    private String dbDir = "/tmo/neo.db";
    private String rrdbDir = "/tmp/neo.rr.db";
    private String webAdminUri = "http://localhost:7474/db/manage/";
    private String webAdminDataUri = "http://localhost:7474/db/data/";
    private StartupHealthCheck startupHealthCheck;
    private AddressResolver addressResolver = new LocalhostAddressResolver();

    public static ServerBuilder server() {
        return new ServerBuilder();
    }

    public NeoServer build() throws IOException {
        File f = createPropertyFile();
        return new NeoServer(addressResolver, startupHealthCheck, f, new Jetty6WebServer());
    }

    private File createPropertyFile() throws IOException {
        File temporaryConfigFile = createTempPropertyFile();
        writePropertyToFile(Configurator.DATABASE_LOCATION_PROPERTY_KEY, dbDir, temporaryConfigFile);
        if (portNo != null) {
            writePropertyToFile(Configurator.WEBSERVER_PORT_PROPERTY_KEY, portNo, temporaryConfigFile);
        }
        writePropertyToFile(Configurator.WEBADMIN_NAMESPACE_PROPERTY_KEY + ".rrdb.location", rrdbDir, temporaryConfigFile);
        writePropertyToFile(Configurator.WEB_ADMIN_PATH_PROPERTY_KEY, webAdminUri, temporaryConfigFile);
        writePropertyToFile(Configurator.WEB_ADMIN_REST_API_PATH_PROPERTY_KEY, webAdminDataUri, temporaryConfigFile);
        return temporaryConfigFile;
    }

    private ServerBuilder() {
    }

    public ServerBuilder withPassingStartupHealthcheck() {
        startupHealthCheck = mock(StartupHealthCheck.class);
        when(startupHealthCheck.run()).thenReturn(true);
        return this;
    }

    public ServerBuilder onPort(int portNo) {
        this.portNo = String.valueOf(portNo);
        return this;
    }

    public ServerBuilder usingDatabaseDir(String dbDir) {
        this.dbDir = dbDir;
        return this;
    }

    public ServerBuilder withRandomDatabaseDir() throws IOException {
        this.dbDir = createTempDir().getAbsolutePath();
        return this;
    }

    public ServerBuilder usingRoundRobinDatabaseDir(String rrdbDir) {
        this.rrdbDir = rrdbDir;
        return this;
    }

    public ServerBuilder withWebAdminUri(String webAdminUri) {
        this.webAdminUri = webAdminUri;
        return this;
    }

    public ServerBuilder withWebDataAdminUri(String webAdminDataUri) {
        this.webAdminDataUri = webAdminDataUri;
        return this;
    }

    public ServerBuilder withoutWebServerPort() {
        portNo = null;
        return this;
    }
    
    public ServerBuilder withNetworkBoundHostnameResolver() {
        addressResolver = new AddressResolver();
        return this;
    }

    public ServerBuilder withFailingStartupHealthcheck() {
        startupHealthCheck = mock(StartupHealthCheck.class);
        when(startupHealthCheck.run()).thenReturn(false);
        when(startupHealthCheck.failedRule()).thenReturn(new StartupHealthCheckRule() {

            public String getFailureMessage() {
                return "mockFailure";
            }

            public boolean execute(Properties properties) {
                // TODO Auto-generated method stub
                return false;
            }
        });
        return this;
    }
}