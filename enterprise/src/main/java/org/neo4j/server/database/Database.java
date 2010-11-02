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

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.helpers.collection.MapUtil;
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
        this(new EmbeddedGraphDatabase(databaseStoreDirectory, MapUtil.stringMap( "enable_remote_shell", "true" )), null, null, null);
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