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
package org.neo4j.server.configuration;

import static org.neo4j.server.ServerTestUtils.createTempPropertyFile;
import static org.neo4j.server.ServerTestUtils.writePropertyToFile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class PropertyFileBuilder {
    
    private String portNo = "7474";
    private String dbDir = "/tmp/neo.db";
    private String rrdbDir = "/tmp/neo.rr.db";
    private String webAdminUri = "http://localhost:7474/db/manage/";
    private String webAdminDataUri = "http://localhost:7474/db/data/";
    private String dbTuningPropertyFile = null;
    private ArrayList<Tuple> nameValuePairs = new ArrayList<Tuple>();

    private static class Tuple {
        public Tuple(String name, String value) {
            this.name = name;
            this.value = value;
        }
        public String name;
        public String value;
    }
    
    public static PropertyFileBuilder builder() {
        return new PropertyFileBuilder();
    }
    
    private PropertyFileBuilder() {}
    
    public File build() throws IOException {
        File temporaryConfigFile = createTempPropertyFile();
        writePropertyToFile(Configurator.DATABASE_LOCATION_PROPERTY_KEY, dbDir, temporaryConfigFile);
        if (portNo != null) {
            writePropertyToFile(Configurator.WEBSERVER_PORT_PROPERTY_KEY, portNo, temporaryConfigFile);
        }
        writePropertyToFile(Configurator.RRDB_LOCATION_PROPERTY_KEY, rrdbDir, temporaryConfigFile);
        writePropertyToFile(Configurator.MANAGEMENT_PATH_PROPERTY_KEY, webAdminUri, temporaryConfigFile);
        writePropertyToFile(Configurator.DATA_API_PATH_PROPERTY_KEY, webAdminDataUri, temporaryConfigFile);
        if(dbTuningPropertyFile != null) {
            writePropertyToFile(Configurator.DB_TUNING_PROPERTY_FILE_KEY, dbTuningPropertyFile, temporaryConfigFile);
        }
        
        for(Tuple t: nameValuePairs) {
            writePropertyToFile(t.name, t.value, temporaryConfigFile);
        }
        
        
        return temporaryConfigFile;
    }

    public PropertyFileBuilder withDbTuningPropertyFile(File f) {
        dbTuningPropertyFile = f.getAbsolutePath();
        return this;
    }

    public PropertyFileBuilder withNameValue(String name, String value) {
        nameValuePairs.add(new Tuple(name, value));
        return this;
    }

    public PropertyFileBuilder withDbTuningPropertyFile(String propertyFileLocation) {
        dbTuningPropertyFile = propertyFileLocation;
        return this;
    }
}
