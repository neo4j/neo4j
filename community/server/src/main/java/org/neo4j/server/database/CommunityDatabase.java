/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.shell.ShellSettings;

public class CommunityDatabase extends/* implements */ Database {

	protected final Configuration serverConfig;

	@SuppressWarnings("deprecation")
	public CommunityDatabase(Configuration serverConfig) {
		this.serverConfig = serverConfig;
	}
	
	@SuppressWarnings("deprecation")
	public CommunityDatabase(Configuration serverConfig, Map<String,String> neo4jProperties) {
		this.serverConfig = serverConfig;
	}
	
	@Override
	@SuppressWarnings("deprecation")
	public void start() throws Throwable
	{
		try
        {
			this.graph = (AbstractGraphDatabase) new org.neo4j.graphdb.factory.GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder( serverConfig.getString( Configurator.DATABASE_LOCATION_PROPERTY_KEY, Configurator.DEFAULT_DATABASE_LOCATION_PROPERTY_KEY) )
				.setConfig( loadNeo4jProperties() )
				.newGraphDatabase();
            log.info( "Successfully started database" );
        } catch(Exception e)
        {
            log.error( "Failed to start database.", e);
            throw e;
        }
	}

	@Override
	@SuppressWarnings("deprecation")
	public void stop() throws Throwable
	{
		try
        {
			if(this.graph != null) 
			{
				this.graph.shutdown();
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
	
	protected Map<String, String> loadNeo4jProperties() {
		Map<String, String> neo4jProperties;
		try {
			String path = serverConfig.getString(Configurator.DB_TUNING_PROPERTY_FILE_KEY,"");
			neo4jProperties = MapUtil.load(new File(path));
			log.info("Loaded neo4j tuning properties from " + path);
		} catch (IOException e) {
			log.warn("Unable to load database tuning properties, using defaults.", e);
			neo4jProperties = new HashMap<String,String>();
		}
		
		putIfAbsent( neo4jProperties, ShellSettings.remote_shell_enabled.name(), GraphDatabaseSetting.TRUE );
        putIfAbsent( neo4jProperties, GraphDatabaseSettings.keep_logical_logs.name(), GraphDatabaseSetting.TRUE );
        neo4jProperties.put( GraphDatabaseSettings.udc_source.name(), "server" );
        
		return neo4jProperties;
	}

    private void putIfAbsent( Map<String, String> databaseProperties, String configKey, String configValue )
    {
        if ( databaseProperties.get( configKey ) == null )
        {
            databaseProperties.put( configKey, configValue );
        }
    }

}
