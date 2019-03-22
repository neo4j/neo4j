/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.dbms.database;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.Nonnull;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigValue;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingChangeListener;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class DatabaseConfig extends Config implements Lifecycle
{
    Config globalConfig;
    private Map<Setting,Collection<SettingChangeListener>> registeredListeners = new ConcurrentHashMap<>();

    public static DatabaseConfig from( Config globalConfig, DatabaseId databaseId )
    {
        if ( Objects.equals( databaseId.name(), GraphDatabaseSettings.SYSTEM_DATABASE_NAME ) )
        {
            Map<String,String> overriddenConfigs = MapUtil.stringMap( GraphDatabaseSettings.record_format.name(), "" );
            return new OverriddenDatabaseConfig( globalConfig, overriddenConfigs );
        }

        return new DatabaseConfig( globalConfig );
    }

    DatabaseConfig( Config globalConfig )
    {
        this.globalConfig = globalConfig;
    }

    @Override
    public <T> T get( Setting<T> setting )
    {
        return globalConfig.get( setting );
    }

    @Override
    public boolean isConfigured( Setting<?> setting )
    {
        return globalConfig.isConfigured( setting );
    }

    @Override
    public Set<String> identifiersFromGroup( Class<?> groupClass )
    {
        return globalConfig.identifiersFromGroup( groupClass );
    }

    @Override
    public void augment( Map<String,String> settings ) throws InvalidSettingException
    {
        globalConfig.augment( settings );
    }

    @Override
    public void augment( String setting, String value ) throws InvalidSettingException
    {
        globalConfig.augment( setting, value );
    }

    @Override
    public void augment( Setting<?> setting, String value )
    {
        globalConfig.augment( setting, value );
    }

    @Override
    public void augment( Config config ) throws InvalidSettingException
    {
        globalConfig.augment( config );
    }

    @Override
    public void augmentDefaults( Setting<?> setting, String value ) throws InvalidSettingException
    {
        globalConfig.augmentDefaults( setting, value );
    }

    @Override
    public Optional<String> getRaw( @Nonnull String key )
    {
        return globalConfig.getRaw( key );
    }

    @Override
    public Map<String,String> getRaw()
    {
        return globalConfig.getRaw();
    }

    @Override
    public Optional<Object> getValue( @Nonnull String key )
    {
        return globalConfig.getValue( key );
    }

    @Override
    public void updateDynamicSetting( String setting, String update, String origin ) throws IllegalArgumentException, InvalidSettingException
    {
        globalConfig.updateDynamicSetting( setting, update, origin );
    }

    @Override
    public <V> void registerDynamicUpdateListener( Setting<V> setting, SettingChangeListener<V> listener )
    {
        registeredListeners.computeIfAbsent( setting, v -> new ConcurrentLinkedQueue<>() ).add( listener );
        globalConfig.registerDynamicUpdateListener( setting, listener );
    }

    @Override
    public <V> void unregisterDynamicUpdateListener( Setting<V> setting, SettingChangeListener<V> externalListener )
    {
        Collection<SettingChangeListener> listeners = registeredListeners.get( setting );
        if ( listeners != null )
        {
            listeners.remove( externalListener );
        }
        globalConfig.unregisterDynamicUpdateListener( setting, externalListener );
    }

    @Override
    public Map<String,ConfigValue> getConfigValues()
    {
        return globalConfig.getConfigValues();
    }

    @Override
    public String obfuscateIfSecret( Map.Entry<String,String> param )
    {
        return globalConfig.obfuscateIfSecret( param );
    }

    @Override
    @Nonnull
    public Set<String> allConnectorIdentifiers()
    {
        return globalConfig.allConnectorIdentifiers();
    }

    @Override
    @Nonnull
    public Set<String> allConnectorIdentifiers( @Nonnull Map<String,String> params )
    {
        return globalConfig.allConnectorIdentifiers( params );
    }

    @Override
    @Nonnull
    public List<BoltConnector> boltConnectors()
    {
        return globalConfig.boltConnectors();
    }

    @Override
    @Nonnull
    public List<BoltConnector> enabledBoltConnectors()
    {
        return globalConfig.enabledBoltConnectors();
    }

    @Override
    @Nonnull
    public List<BoltConnector> enabledBoltConnectors( @Nonnull Map<String,String> params )
    {
        return globalConfig.enabledBoltConnectors( params );
    }

    @Override
    @Nonnull
    public List<HttpConnector> httpConnectors()
    {
        return globalConfig.httpConnectors();
    }

    @Override
    @Nonnull
    public List<HttpConnector> enabledHttpConnectors()
    {
        return globalConfig.enabledHttpConnectors();
    }

    @Override
    public String toString()
    {
        return globalConfig.toString();
    }

    @Override
    public void init()
    {

    }

    @Override
    public void start()
    {

    }

    @Override
    public void stop() throws Exception
    {
        for ( Map.Entry<Setting,Collection<SettingChangeListener>> settingListeners : registeredListeners.entrySet() )
        {
            Setting setting = settingListeners.getKey();
            Collection<SettingChangeListener> listeners = settingListeners.getValue();
            for ( SettingChangeListener listener : listeners )
            {
                globalConfig.unregisterDynamicUpdateListener( setting, listener );
            }
        }
        registeredListeners = new ConcurrentHashMap<>();
    }

    @Override
    public void shutdown()
    {

    }
}
