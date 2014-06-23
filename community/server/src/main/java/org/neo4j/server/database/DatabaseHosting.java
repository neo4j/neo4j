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
package org.neo4j.server.database;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.server.configuration.ConfigDatabase;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_dir;

public class DatabaseHosting implements Lifecycle
{

    private final DatabaseRegistry registry;
    private final ConfigDatabase configDb;
    private final StringLogger log;

    public enum Mode
    {
        /**
         * A database where the user has explicitly specified a disk location for the database files. Dropping
         * an external database does not remove the actual files from disk.
         */
        EXTERNAL,

        /**
         * A database where the user has asked the server to provide a database and handle the files for it. Dropping
         * a managed database will delete the associated files on disk.
         */
        MANAGED,
        ;

        public static Mode fromString( String mode )
        {
            return valueOf( mode.toUpperCase() );
        }
    }

    public DatabaseHosting( DatabaseRegistry registry, ConfigDatabase configDb, StringLogger log )
    {
        this.registry = registry;
        this.configDb = configDb;
        this.log = log;
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        for ( DatabaseDefinition databaseDefinition : configDb.listDatabases() )
        {
            create( databaseDefinition );
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

    public void newDatabase( String dbKey, String provider, Mode mode, Config dbConfig )
    {
        if(!registry.contains( dbKey ))
        {
            create( configDb.newDatabase( dbKey, provider, mode, dbConfig ) );
        }
    }

    public void reconfigureDatabase( String dbKey, Config newConfig )
    {
        if(registry.contains( dbKey ))
        {
            registry.drop( dbKey );
            DatabaseDefinition definition = configDb.getDatabase( dbKey );

            // Don't allow modifying the database path this way.
            Map<String,String> configMap = newConfig.getParams();
            configMap.put( store_dir.name(), definition.path().getPath() );

            configDb.reconfigureDatabase( dbKey, definition.provider(), definition.mode(), new Config( configMap ) );

            create( configDb.getDatabase( dbKey ) );
        }
    }

    public void changeDatabaseProvider( String dbKey, String provider )
    {
        if(registry.contains( dbKey ))
        {
            registry.drop( dbKey );
            DatabaseDefinition definition = configDb.getDatabase( dbKey );
            configDb.reconfigureDatabase( dbKey, provider, definition.mode(), definition.config() );
            DatabaseDefinition database = configDb.getDatabase( dbKey );
            create( database );
        }
    }

    private void create( DatabaseDefinition database )
    {
        try
        {
            registry.create( database );
        }
        catch ( NoSuchDatabaseProviderException e )
        {
            log.error( String.format( "Unable to start database '%s', because there is no database provider called " +
                    "'%s', which this database has been configured to use.", database.key(), e.provider() ) );
        }
    }

    public void dropDatabase( final String dbKey )
    {
        if(registry.contains( dbKey ))
        {
            DatabaseDefinition definition = configDb.dropDatabase( dbKey );
            registry.drop( dbKey );

            if(definition.mode() == Mode.MANAGED)
            {
                // Sanity check
                File path = definition.path();
                if( path != null && path.getPath().length() > 0)
                {
                    try
                    {
                        FileUtils.deleteRecursively( path );
                    }
                    catch ( IOException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
            }
        }
    }
}
