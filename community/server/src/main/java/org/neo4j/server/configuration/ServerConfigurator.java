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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.NeoServerSettings;
import org.neo4j.server.WrappingNeoServerBootstrapper;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Used by the {@link WrappingNeoServerBootstrapper}, passing the minimum amount
 * of required configuration on to the neo4j server.
 * <p/>
 * If you want to change configuration for your
 * {@link WrappingNeoServerBootstrapper}, create an instance of this class, and
 * add configuration like so:
 * <p/>
 * <pre>
 * {
 *     &#064;code ServerConfigurator conf = new ServerConfigurator( myDb );
 *     conf.setProperty( ServerSettings.webserver_port.name(), "8080" );
 * }
 * </pre>
 * <p/>
 * See the neo4j manual for information about what configuration directives the
 * server takes, or take a look at the settings in {@link ServerSettings}.
 */
public class ServerConfigurator implements ConfigurationBuilder
{
    private final Map<String, String> configParams = new HashMap();
    private final Config config;

    public ServerConfigurator( GraphDatabaseAPI db )
    {
        configParams.put( NeoServerSettings.legacy_db_location.name(), db.getStoreDir() );
        config = new Config( configParams );
        PropertyFileConfigurator.setServerSettingsClasses( config );
    }

    @Override
    public Config configuration()
    {
        return config;
    }

    @Override
    public Map<String, String> getDatabaseTuningProperties()
    {
        return stringMap();
    }

    /**
     * @param key
     * @param value
     * @return the configurator itself with configuration property updated.
     */
    public ServerConfigurator setProperty( String key, String value )
    {
        config.applyChanges( MapUtil.stringMap( config.getParams(), key, value ) );
        return this;
    }
}
