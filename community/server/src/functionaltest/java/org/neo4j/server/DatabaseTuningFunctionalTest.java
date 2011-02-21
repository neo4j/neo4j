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

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.junit.Test;
import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.database.Database;
import org.neo4j.server.logging.InMemoryAppender;

public class DatabaseTuningFunctionalTest {

    @Test
    public void shouldLoadAKnownGoodPropertyFile() throws IOException {
        NeoServer server = ServerBuilder.server().withPassingStartupHealthcheck().withRandomDatabaseDir().withDefaultDatabaseTuning().build();
        server.start();
        Map<Object, Object> params = server.getDatabase().graph.getConfig().getParams();
        
        
        assertTrue(propertyAndValuePresentIn("neostore.nodestore.db.mapped_memory", "25M", params));
        assertTrue(propertyAndValuePresentIn("neostore.relationshipstore.db.mapped_memory", "50M", params));
        assertTrue(propertyAndValuePresentIn("neostore.propertystore.db.mapped_memory", "90M", params));
        assertTrue(propertyAndValuePresentIn("neostore.propertystore.db.strings.mapped_memory", "130M", params));
        assertTrue(propertyAndValuePresentIn("neostore.propertystore.db.arrays.mapped_memory", "130M", params));
        
        server.stop();
    }
    
    
    private boolean propertyAndValuePresentIn(String name, String value, Map<Object, Object> params) {
        for(Object o : params.keySet()) {
            if(o.toString().equals(name) && params.get(o).toString().equals(value)) {
                return true;
            }
        }
        
        return false;
    }


    @Test
    public void shouldLogWarningAndContinueIfNoTuningFilePropertyPresent() throws IOException {
        InMemoryAppender appender = new InMemoryAppender(Database.log);
        
        String propertyFilePath = "/tmp/some/path/to/a/server/property/file";
        System.setProperty("org.neo4j.server.properties", propertyFilePath);
        NeoServer server = ServerBuilder.server().withPassingStartupHealthcheck().withRandomDatabaseDir().build();
        server.start();
        
        assertThat(appender.toString(), containsString("No database tuning properties set in the property file, using defaults. Please specify the performance properties file with org.neo4j.server.db.tuning.properties in the server properties file [" + propertyFilePath + "]."));
        
        server.stop();
    }
    
    @Test
    public void shouldLogWarningAndContinueIfTuningFilePropertyDoesNotResolve() throws IOException {
        InMemoryAppender appender = new InMemoryAppender(PropertyFileConfigurator.log);
        
        NeoServer server = ServerBuilder.server().withPassingStartupHealthcheck().withRandomDatabaseDir().withNonResolvableTuningFile().build();
        server.start();
        
        assertThat(appender.toString(), containsString(String.format("The specified file for database performance tuning properties [")));
        assertThat(appender.toString(), containsString(String.format("] does not exist.")));
        
        server.stop();
    } 

}
