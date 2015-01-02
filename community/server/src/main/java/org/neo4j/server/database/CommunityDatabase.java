/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.shell.ShellSettings;

import static org.neo4j.server.configuration.Configurator.DATABASE_LOCATION_PROPERTY_KEY;
import static org.neo4j.server.configuration.Configurator.DEFAULT_DATABASE_LOCATION_PROPERTY_KEY;

public class CommunityDatabase implements Database
{
    protected final Configurator configurator;
    protected final Configuration serverConfiguration;
    protected final Logging logging;
    protected final ConsoleLogger log;

    private boolean isRunning = false;

    private AbstractGraphDatabase graph;

    @SuppressWarnings("deprecation")
    public CommunityDatabase( Configurator configurator, Logging logging )
    {
        this.logging = logging;
        this.configurator = configurator;
        this.serverConfiguration = configurator.configuration();
        this.log = logging.getConsoleLog( getClass() );
    }

    protected AbstractGraphDatabase createDb()
    {
        return (AbstractGraphDatabase) new org.neo4j.graphdb.factory.GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( serverConfiguration.getString( DATABASE_LOCATION_PROPERTY_KEY,
                        DEFAULT_DATABASE_LOCATION_PROPERTY_KEY ) )
                .setConfig( getDbTuningPropertiesWithServerDefaults() )
                .newGraphDatabase();
    }

    @Override
    public String getLocation()
    {
        return graph.getStoreDir();
    }

    @Override
    public Logging getLogging()
    {
        return logging;
    }

    @Override
    public org.neo4j.graphdb.index.Index<Relationship> getRelationshipIndex( String name )
    {
        RelationshipIndex index = graph.index().forRelationships( name );
        if ( index == null )
        {
            throw new RuntimeException( "No index for [" + name + "]" );
        }
        return index;
    }

    @Override
    public org.neo4j.graphdb.index.Index<Node> getNodeIndex( String name )
    {
        org.neo4j.graphdb.index.Index<Node> index = graph.index()
                .forNodes( name );
        if ( index == null )
        {
            throw new RuntimeException( "No index for [" + name + "]" );
        }
        return index;
    }

    @Override
    public IndexManager getIndexManager()
    {
        return graph.index();
    }

    @Override
    public GraphDatabaseAPI getGraph()
    {
        return graph;
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        try
        {
            this.graph = createDb();
            isRunning = true;
            log.log( "Successfully started database" );
        }
        catch ( Exception e )
        {
            log.error( "Failed to start database.", e );
            throw e;
        }
    }

    @Override
    public void stop() throws Throwable
    {
        try
        {
            if ( this.graph != null )
            {
                this.graph.shutdown();
                isRunning = false;
                this.graph = null;
                log.log( "Successfully stopped database" );
            }
        }
        catch ( Exception e )
        {
            log.error( String.format("Database did not stop cleanly. Reason [%s]", e.getMessage()) );
            throw e;
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
    }

    @Override
    public boolean isRunning()
    {
        return isRunning;
    }

    protected Map<String, String> getDbTuningPropertiesWithServerDefaults()
    {
        Map<String, String> result = new HashMap<>( configurator.getDatabaseTuningProperties() );
        putIfAbsent( result, ShellSettings.remote_shell_enabled.name(), Settings.TRUE );
        putIfAbsent( result, GraphDatabaseSettings.keep_logical_logs.name(), Settings.TRUE );

        try
        {
            result.put( UdcSettings.udc_source.name(), "server" );
        }
        catch ( NoClassDefFoundError e )
        {
            // UDC is not on classpath, ignore
        }

        return result;
    }

    private void putIfAbsent( Map<String, String> databaseProperties, String configKey, String configValue )
    {
        if ( databaseProperties.get( configKey ) == null )
        {
            databaseProperties.put( configKey, configValue );
        }
    }
}
