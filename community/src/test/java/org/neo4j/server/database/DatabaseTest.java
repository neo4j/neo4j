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
