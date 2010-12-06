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

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneFulltextIndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.server.logging.Logger;
import org.rrd4j.core.RrdDb;

import java.util.HashMap;
import java.util.Map;

public class Database {

    public static Logger log = Logger.getLogger(Database.class);

    public AbstractGraphDatabase graph;
    public IndexService indexService;
    public IndexService fulltextIndexService;
    public Map<String, Index<? extends PropertyContainer>> indicies;

    private String databaseStoreDirectory;
    private RrdDb rrdDb;

    public Database(AbstractGraphDatabase db) {
        this.databaseStoreDirectory = db.getStoreDir();
        graph = db;
        ensureIndexServiceIsAvailable();
    }

    public Database(String databaseStoreDirectory) {
        this(createDatabase(databaseStoreDirectory));
        log.warn("No database tuning properties set in the property file, using defaults. Please specify the performance properties file with org.neo4j.server.db.tuning.properties in the server properties file [%s].", System.getProperty("org.neo4j.server.properties"));
    }

    public Database(String databaseStoreDirectory, Map<String, String> databaseTuningProperties) {
        this(createDatabase(databaseStoreDirectory, databaseTuningProperties));   
    }

    private static EmbeddedGraphDatabase createDatabase(String databaseStoreDirectory) {
        return createDatabase(databaseStoreDirectory, null);
    }
    
    private static EmbeddedGraphDatabase createDatabase(String databaseStoreDirectory, Map<String, String> databaseTuningProperties) {
        log.info("Creating database at " + databaseStoreDirectory);
        
        if(databaseTuningProperties == null) {
            databaseTuningProperties = new HashMap<String, String>();
        }
        
        databaseTuningProperties.put("enable_remote_shell", "true");
        return new EmbeddedGraphDatabase(databaseStoreDirectory, databaseTuningProperties);
    }

    private synchronized void ensureIndexServiceIsAvailable() throws DatabaseBlockedException {
        if (indexService == null) {
            if (graph instanceof AbstractGraphDatabase) {
                indexService = new LuceneIndexService(graph);
                fulltextIndexService = new LuceneFulltextIndexService(graph);
                indicies = instantiateSomeIndicies();
            } else {
                // TODO: Indexing for remote dbs
                throw new UnsupportedOperationException("Indexing is not yet available in neo4j-rest for remote databases.");
            }
        }
    }

    private Map<String, Index<? extends PropertyContainer>> instantiateSomeIndicies() {
        Map<String, Index<? extends PropertyContainer>> map = new HashMap<String, Index<? extends PropertyContainer>>();
        map.put("node", new NodeIndex(indexService));
        map.put("fulltext-node", new NodeIndex(fulltextIndexService));
        return map;
    }

    public void startup() {
        if (graph != null) {
            log.info("Successfully started database");
        } else {
            log.error("Failed to start database. GraphDatabaseService has not been properly initialized.");
        }
    }

    public void shutdown() {
        try {
            if (graph != null) {
                graph.shutdown();
            }
            log.info("Successfully shutdown database");
        } catch (Exception e) {
            log.error("Database did not shut down cleanly. Reason [%s]", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public String getLocation() {
        return databaseStoreDirectory;
    }

    public Index<? extends PropertyContainer> getIndex(String name) {
        Index<? extends PropertyContainer> index = indicies.get(name);
        if (index == null) {
            throw new RuntimeException("No index for [" + name + "]");
        }
        return index;
    }

    public RrdDb rrdDb() {
        return rrdDb;
    }

    public void setRrdDb(RrdDb rrdDb) {
        this.rrdDb = rrdDb;
    }
}