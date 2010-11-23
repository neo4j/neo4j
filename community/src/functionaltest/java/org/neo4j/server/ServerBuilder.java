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