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

import static org.neo4j.server.ServerTestUtils.*;

public class ServerBuilder {

    private String portNo = "7474";
    private String dbDir = "/tmo/neo.db";
    private String rrdbDir = "/tmp/neo.rr.db";
    private String webAdminUri = "http://localhost:7474/db/manage/";
    private String webAdminDataUri = "http://localhost:7474/db/data/";

    public static ServerBuilder server() {
        return new ServerBuilder();
    }
    
    public NeoServer build() throws IOException {
        File f = createPropertyFile();
        System.setProperty(NeoServer.NEO_CONFIG_FILE_KEY, f.getAbsolutePath());
        return new NeoServer();
    }

    private File createPropertyFile() throws IOException {
        File temporaryConfigFile = createTempPropertyFile();
        writePropertyToFile( "org.neo4j.server.database.location", dbDir, temporaryConfigFile );
        writePropertyToFile( "org.neo4j.server.webserver.port", portNo, temporaryConfigFile );
        writePropertyToFile( NeoServer.WEBADMIN_NAMESPACE_PROPERTY_KEY + "rrdb.location", rrdbDir, temporaryConfigFile );
        writePropertyToFile( "org.neo4j.server.webadmin.management.uri", webAdminUri, temporaryConfigFile );
        writePropertyToFile( "org.neo4j.server.webadmin.data.uri", webAdminDataUri, temporaryConfigFile );
        return temporaryConfigFile;
    }

    private ServerBuilder() {}
    
    public ServerBuilder onPort(int portNo) {
        this.portNo = String.valueOf(portNo);
        return this;
    }
    
    public ServerBuilder usingDatabaseDir(String dbDir) {
        this.dbDir = dbDir;
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
}