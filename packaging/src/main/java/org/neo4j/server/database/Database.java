package org.neo4j.server.database;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.index.IndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.server.logging.Logger;

public class Database {
    
    public static Logger log = Logger.getLogger(Database.class);
    
    public GraphDatabaseService db;
    public IndexService indexService;
    public IndexService fulltextIndexService;
    public Map<String, Index<? extends PropertyContainer>> indicies;

    private String databaseStoreDirectory;

    public Database(String databaseStoreDirectory) {
        this(new EmbeddedGraphDatabase(databaseStoreDirectory), null, null, null);
        this.databaseStoreDirectory = databaseStoreDirectory;
    }
    
    public Database(GraphDatabaseService db, IndexService indexService,
            IndexService fulltextIndexService,
            Map<String, Index<? extends PropertyContainer>> indices) {
        this.db = db;
        this.indexService = indexService;
        this.fulltextIndexService = fulltextIndexService;
        indicies = indices;
    }

    public void startup() {
        log.info("Successfully started database");
    }

    public void shutdown() {
        try {
            if(db != null) {
                db.shutdown();
            }
            log.info("Successfully shutdown database");
        } catch(Exception e) {
            log.error("Database did not shut down cleanly. Reason [%s]", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public String getLocation() {
        return databaseStoreDirectory;
    }
}