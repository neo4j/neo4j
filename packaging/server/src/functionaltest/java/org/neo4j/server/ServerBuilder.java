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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.server.ServerTestUtils.createTempDir;
import static org.neo4j.server.ServerTestUtils.createTempPropertyFile;
import static org.neo4j.server.ServerTestUtils.writePropertiesToFile;
import static org.neo4j.server.ServerTestUtils.writePropertyToFile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.startup.healthcheck.StartupHealthCheck;
import org.neo4j.server.startup.healthcheck.StartupHealthCheckRule;
import org.neo4j.server.web.Jetty6WebServer;

public class ServerBuilder {

    private String portNo = "7474";
    private String dbDir = null;
    private String rrdbDir = "/tmp/neo.rr.db"; // TODO: should we require this
                                               // to be null as well?
    private String webAdminUri = "/db/manage/";
    private String webAdminDataUri = "/db/data/";
    private StartupHealthCheck startupHealthCheck;
    private AddressResolver addressResolver = new LocalhostAddressResolver();
    private final HashMap<String, String> thirdPartyPackages = new HashMap<String, String>();

    private static enum WhatToDo {
        CREATE_GOOD_TUNING_FILE, CREATE_DANGLING_TUNING_FILE_PROPERTY, CREATE_CORRUPT_TUNING_FILE
    };

    private WhatToDo action;
    private List<Class<? extends ServerModule>> serverModules = new ArrayList<Class<? extends ServerModule>>();

    public static ServerBuilder server() {
        return new ServerBuilder();
    }

    public NeoServerWithEmbeddedWebServer build() throws IOException {
        if ( dbDir == null )
        {
            throw new IllegalStateException( "database directory must be configured." );
        }
        File f = createPropertiesFiles();
        
        return new CleaningNeoServer(addressResolver, startupHealthCheck, f, new Jetty6WebServer(), dbDir, (Class<? extends ServerModule>[]) serverModules.toArray(new Class[0]));
    }
    

    public File createPropertiesFiles() throws IOException {
        File temporaryConfigFile = createTempPropertyFile();
        
        createPropertiesFile(temporaryConfigFile);
        createTuningFile(temporaryConfigFile);
        
        return temporaryConfigFile;
    }

    private void createPropertiesFile(File temporaryConfigFile) {
        writePropertyToFile(Configurator.DATABASE_LOCATION_PROPERTY_KEY, dbDir, temporaryConfigFile);
        if (portNo != null) {
            writePropertyToFile(Configurator.WEBSERVER_PORT_PROPERTY_KEY, portNo, temporaryConfigFile);
        }
        writePropertyToFile(Configurator.RRDB_LOCATION_PROPERTY_KEY, rrdbDir, temporaryConfigFile);
        writePropertyToFile(Configurator.MANAGEMENT_PATH_PROPERTY_KEY, webAdminUri, temporaryConfigFile);
        writePropertyToFile(Configurator.DATA_API_PATH_PROPERTY_KEY, webAdminDataUri, temporaryConfigFile);
        
        if (thirdPartyPackages.keySet().size() > 0) {
            writePropertiesToFile(Configurator.THIRD_PARTY_PACKAGES_KEY, thirdPartyPackages, temporaryConfigFile);
        }
    }

    private void createTuningFile(File temporaryConfigFile) throws IOException {
        if (action == WhatToDo.CREATE_GOOD_TUNING_FILE) {
            File databaseTuningPropertyFile = createTempPropertyFile();
            writePropertyToFile("neostore.nodestore.db.mapped_memory", "25M", databaseTuningPropertyFile);
            writePropertyToFile("neostore.relationshipstore.db.mapped_memory", "50M", databaseTuningPropertyFile);
            writePropertyToFile("neostore.propertystore.db.mapped_memory", "90M", databaseTuningPropertyFile);
            writePropertyToFile("neostore.propertystore.db.strings.mapped_memory", "130M", databaseTuningPropertyFile);
            writePropertyToFile("neostore.propertystore.db.arrays.mapped_memory", "130M", databaseTuningPropertyFile);
            writePropertyToFile(Configurator.DB_TUNING_PROPERTY_FILE_KEY, databaseTuningPropertyFile.getAbsolutePath(), temporaryConfigFile);
        } else if (action == WhatToDo.CREATE_DANGLING_TUNING_FILE_PROPERTY) {
            writePropertyToFile(Configurator.DB_TUNING_PROPERTY_FILE_KEY, createTempPropertyFile().getAbsolutePath(), temporaryConfigFile);
        } else if (action == WhatToDo.CREATE_CORRUPT_TUNING_FILE) {
            File corruptTuningFile = trashFile();
            writePropertyToFile(Configurator.DB_TUNING_PROPERTY_FILE_KEY, corruptTuningFile.getAbsolutePath(), temporaryConfigFile);
        }
    }

    private File trashFile() throws IOException {
        File f = createTempPropertyFile();

        FileWriter fstream = new FileWriter(f, true);
        BufferedWriter out = new BufferedWriter(fstream);

        for (int i = 0; i < 100; i++) {
            out.write((int) System.currentTimeMillis());
        }

        out.close();
        return f;
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

    public ServerBuilder withRelativeWebAdminUriPath(String webAdminUri) {
        try {
            URI theUri = new URI(webAdminUri);
            if(theUri.isAbsolute()) {
                this.webAdminUri = theUri.getPath();
            } else {
                this.webAdminUri = theUri.toString();
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public ServerBuilder withRelativeWebDataAdminUriPath(String webAdminDataUri) {
        try {
            URI theUri = new URI(webAdminDataUri);
            if(theUri.isAbsolute()) {
                this.webAdminDataUri = theUri.getPath();
            } else {
                this.webAdminDataUri = theUri.toString();
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
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
                return false;
            }
        });
        return this;
    }

    public ServerBuilder withDefaultDatabaseTuning() throws IOException {
        action = WhatToDo.CREATE_GOOD_TUNING_FILE;
        return this;
    }

    public ServerBuilder withNonResolvableTuningFile() throws IOException {
        action = WhatToDo.CREATE_DANGLING_TUNING_FILE_PROPERTY;
        return this;
    }

    public ServerBuilder withCorruptTuningFile() throws IOException {
        action = WhatToDo.CREATE_CORRUPT_TUNING_FILE;
        return this;
    }

    public ServerBuilder withThirdPartyJaxRsPackage(String packageName, String mountPoint) {
        thirdPartyPackages.put(packageName, mountPoint);
        return this;
    }
    
    public ServerBuilder withSpecificServerModules(Class<? extends ServerModule> ... modules) {
        serverModules = Arrays.asList(modules);
        return this;
    }
}