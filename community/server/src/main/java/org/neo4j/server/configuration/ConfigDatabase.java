/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.configuration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.server.NeoServerSettings;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.DatabaseDefinition;
import org.neo4j.server.database.DatabaseRegistry;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_dir;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.helpers.collection.MapUtil.toStringMap;
import static org.neo4j.server.NeoServerSettings.config_db_path;
import static org.neo4j.server.NeoServerSettings.legacy_db_location;
import static org.neo4j.server.NeoServerSettings.legacy_db_mode;
import static org.neo4j.server.database.DatabaseHosting.Mode;
import static org.neo4j.server.database.DatabaseHosting.Mode.MANAGED;

public class ConfigDatabase implements Lifecycle
{

    public static final String CONFIG_DB = "__config__";
    private final DatabaseRegistry databases;
    private final Config config;

    public ConfigDatabase( DatabaseRegistry databases, Config config )
    {
        this.databases = databases;
        this.config = config;
    }

    public DatabaseDefinition newDatabase( final String dbKey, final String provider, final Mode mode, final Config dbConfig )
    {
        databases.visit( CONFIG_DB, new DatabaseRegistry.Visitor()
        {
            @Override
            public void visit( Database db )
            {
                try(Transaction tx = db.getGraph().beginTx())
                {
                    db.executionEngine().execute(
                        "CREATE (db:Database {key:{key}, provider:{provider}, path:{path}, mode:{mode}})-[:CONFIGURED_BY]->({cfg})",
                            map(
                                "key", dbKey,
                                "provider", provider,
                                "path", dbConfig.get( GraphDatabaseSettings.store_dir ).getPath(),
                                "mode", mode.name(),
                                "cfg", dbConfig.getParams()
                            ));
                    tx.success();
                }
            }
        });
        return new DatabaseDefinition( dbKey, provider, mode, dbConfig );
    }

    public DatabaseDefinition dropDatabase( final String dbKey )
    {
        final AtomicReference<DatabaseDefinition> definitionContainer = new AtomicReference<>(  );
        databases.visit( CONFIG_DB, new DatabaseRegistry.Visitor()
        {
            @Override
            public void visit( Database db )
            {
                try(Transaction tx = db.getGraph().beginTx())
                {
                    Map<String, Object> row = single( db.executionEngine().execute(
                            "MATCH (db:Database {key:{key}})-[:CONFIGURED_BY]->(cfg) " +
                            "RETURN db.mode, db.provider, cfg", map( "key", dbKey ) ) );

                    String provider = (String) row.get( "db.provider" );
                    String mode = (String) row.get( "db.mode" );
                    Config config = new Config(toStringMap((Node)row.get("cfg")));

                    definitionContainer.set(new DatabaseDefinition( dbKey, provider, Mode.fromString(mode), config ));

                    db.executionEngine().execute(
                            "MATCH (db:Database {key:{key}})-[:CONFIGURED_BY]->(cfg) " +
                            "OPTIONAL MATCH (db)-[r]-() " +
                            "DELETE db, r, cfg", map( "key", dbKey ));
                    tx.success();
                }
            }
        });
        return definitionContainer.get();
    }

    public Iterable<DatabaseDefinition> listDatabases()
    {
        final List<DatabaseDefinition> dbs = new ArrayList<>();
        databases.visit( CONFIG_DB, new DatabaseRegistry.Visitor()
        {
            @Override
            public void visit( Database db )
            {
                try(Transaction ignore = db.getGraph().beginTx())
                {
                    for ( Map<String, Object> row : db.executionEngine().execute(
                            "MATCH (db:Database)-[:CONFIGURED_BY]->(cfg) RETURN db.key, db.provider, db.mode, cfg" ) )
                    {
                        dbs.add( new DatabaseDefinition(
                                (String)row.get("db.key"),
                                (String)row.get("db.provider"),
                                Mode.fromString((String)row.get("db.mode")),
                                new Config(toStringMap((Node)row.get("cfg"))) ) );
                    }
                }
            }
        });
        return dbs;
    }

    public boolean databaseExists( final String key )
    {
        final AtomicBoolean exists = new AtomicBoolean( false );
        databases.visit( CONFIG_DB, new DatabaseRegistry.Visitor()
        {
            @Override
            public void visit( Database db )
            {
                try(Transaction ignore = db.getGraph().beginTx())
                {
                    for ( Map<String, Object> row : db.executionEngine().execute(
                            "MATCH (db:Database {key:{key}}) RETURN db", map("key", key) ) )
                    {
                        exists.set( true );
                    }
                }
            }
        });
        return exists.get();
    }

    public DatabaseDefinition getDatabase( final String dbKey )
    {
        final AtomicReference<DatabaseDefinition> definitionContainer = new AtomicReference<>(  );
        databases.visit( CONFIG_DB, new DatabaseRegistry.Visitor()
        {
            @Override
            public void visit( Database db )
            {
                try(Transaction ignore = db.getGraph().beginTx())
                {
                    Map<String, Object> row = single( db.executionEngine().execute(
                            "MATCH (db:Database {key:{key}})-[:CONFIGURED_BY]->(cfg) " +
                                    "RETURN db.mode, db.provider, cfg", map( "key", dbKey ) ) );

                    String provider = (String) row.get( "db.provider" );
                    String mode = (String) row.get( "db.mode" );
                    Config config = new Config(toStringMap((Node)row.get("cfg")));

                    definitionContainer.set(new DatabaseDefinition( dbKey, provider, Mode.fromString(mode), config ));
                }
            }
        });
        return definitionContainer.get();
    }

    public void reconfigureDatabase( final String key, final String provider, final Mode mode, final Config cfg )
    {
        dropDatabase( key );
        newDatabase( key, provider, mode, cfg );
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        if(!databases.contains( CONFIG_DB ))
        {
            boolean configDbExisted = config.get( config_db_path ).exists();
            databases.create( new DatabaseDefinition(
                    CONFIG_DB,
                    "single",
                    MANAGED,
                    new Config( stringMap( store_dir.name(), config.get( config_db_path ).getAbsolutePath() ) )));

            configureLegacyDb( configDbExisted );
        }
    }

    private void configureLegacyDb( boolean configDbExisted  ) throws IOException
    {
        // For backwards compatibility, we need to check if we should set up a "db" database that points to
        // the "graph.db" location. We use the config database to figure this out - if there is a config database,
        // assume this check has already been performed, if not, we check for graph.db folder and act accordingly.
        boolean shouldCreateLegacyDb = !configDbExisted;
        if(shouldCreateLegacyDb)
        {
            newDatabase( "db", config.get( legacy_db_mode ).toLowerCase(), Mode.MANAGED, new Config(
                    stringMap(store_dir.name(), config.get( legacy_db_location ).getPath() )));
        }

        File legacyConfig = config.get( NeoServerSettings.legacy_db_config );
        if( databaseExists( "db" ) && legacyConfig.exists())
        {
            Map<String, String> loadedConfig = MapUtil.load( legacyConfig );
            loadedConfig.put( store_dir.name(), config.get( legacy_db_location ).getPath() );
            reconfigureDatabase( "db", config.get( legacy_db_mode ).toLowerCase(), Mode.MANAGED,
                                 new Config(loadedConfig ));
        }
    }

    @Override
    public void stop() throws Throwable
    {
    }

    @Override
    public void shutdown() throws Throwable
    {
    }
}
