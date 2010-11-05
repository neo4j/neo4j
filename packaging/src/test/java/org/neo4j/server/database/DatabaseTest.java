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

package org.neo4j.server.database;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.logging.InMemoryAppender;

public class DatabaseTest {

    private File databaseDirectory;
    private Database theDatabase;

    @Before
    public void setup() throws Exception {
        if(theDatabase != null) {
            theDatabase.shutdown();
        }
        databaseDirectory = ServerTestUtils.createTempDir();
        theDatabase = new Database(databaseDirectory.getAbsolutePath());
    }
    
    @Test
    public void shouldLogOnSuccessfulStartup() {
        InMemoryAppender appender = new InMemoryAppender(Database.log);
        
        theDatabase.startup();
        
        assertThat(appender.toString(), containsString("Successfully started database"));
    }
    

    @Test
    public void shouldShutdownCleanly() {
        InMemoryAppender appender = new InMemoryAppender(Database.log);
        
        theDatabase.startup();
        theDatabase.shutdown();
        
        assertThat(appender.toString(), containsString("Successfully shutdown database"));
    }
}
