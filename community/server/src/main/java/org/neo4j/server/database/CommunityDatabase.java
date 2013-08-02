/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.shell.ShellSettings;

import static org.neo4j.server.configuration.Configurator.DATABASE_LOCATION_PROPERTY_KEY;
import static org.neo4j.server.configuration.Configurator.DEFAULT_DATABASE_LOCATION_PROPERTY_KEY;

public class CommunityDatabase extends/* implements */ Database
{
    protected final Configurator configurator;
    protected final Configuration serverConfiguration;
    private boolean isRunning = false;

    @SuppressWarnings("deprecation")
    public CommunityDatabase( Configurator configurator )
    {
        this.configurator = configurator;
        this.serverConfiguration = configurator.configuration();
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
    @SuppressWarnings("deprecation")
    public void start() throws Throwable
    {
        try
        {
            this.graph = createDb();
            isRunning = true;
            log.info( "Successfully started database" );
        }
        catch ( Exception e )
        {
            log.error( "Failed to start database.", e );
            throw e;
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public void stop() throws Throwable
    {
        try
        {
            if ( this.graph != null )
            {
                this.graph.shutdown();
                isRunning = false;
                this.graph = null;
                log.info( "Successfully stopped database" );
            }
        }
        catch ( Exception e )
        {
            log.error( "Database did not stop cleanly. Reason [%s]", e.getMessage() );
            throw e;
        }
    }

    @Override
    public boolean isRunning()
    {
        return isRunning;
    }

    protected Map<String, String> getDbTuningPropertiesWithServerDefaults()
    {
        Map<String, String> result = new HashMap<String, String>( configurator.getDatabaseTuningProperties() );
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
