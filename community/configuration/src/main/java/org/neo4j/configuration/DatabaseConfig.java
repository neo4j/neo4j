/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.configuration;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;

import static java.lang.Boolean.TRUE;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

public class DatabaseConfig extends Config implements Lifecycle
{
    private final Map<Setting<?>,Object> databaseSpecificSettings;
    private final Config globalConfig;
    private final Map<Setting<?>, Object> overriddenSettings;
    private Map<Setting<Object>,Collection<SettingChangeListener<Object>>> registeredListeners = new ConcurrentHashMap<>();

    public DatabaseConfig( Map<Setting<?>,Object> databaseSpecificSettings, Config globalConfig, NamedDatabaseId namedDatabaseId )
    {
        this.databaseSpecificSettings = databaseSpecificSettings;
        this.globalConfig = globalConfig;
        overriddenSettings = !namedDatabaseId.isSystemDatabase() ? null : Map.of(
                record_format, "", //Latest version of the format family it is currently on. Needs to work in rolling upgrade.
                allow_upgrade, TRUE
        );
    }

    @Override
    public <T> T get( Setting<T> setting )
    {
        if ( overriddenSettings != null )
        {
            Object o = overriddenSettings.get( setting );
            if ( o != null )
            {
                //noinspection unchecked
                return (T) o;
            }
        }
        Object dbSpecific = databaseSpecificSettings.get( setting );
        if ( dbSpecific != null )
        {
            return (T) dbSpecific;
        }
        return globalConfig.get( setting );
    }

    @Override
    public <T> void addListener( Setting<T> setting, SettingChangeListener<T> listener )
    {
        registeredListeners.computeIfAbsent( (SettingImpl<Object>) setting, v -> new ConcurrentLinkedQueue<>() ).add(
                (SettingChangeListener<Object>) listener );
        globalConfig.addListener( setting, listener );
    }

    @Override
    public <T> void removeListener( Setting<T> setting, SettingChangeListener<T> listener )
    {
        Collection<SettingChangeListener<Object>> listeners = registeredListeners.get( setting );
        if ( listeners != null )
        {
            listeners.remove( listener );
        }
        globalConfig.removeListener( setting, listener );
    }

    @Override
    public void setLogger( Log internalLog )
    {
        globalConfig.setLogger( internalLog );
    }

    @Override
    public <T extends GroupSetting> Map<String,T> getGroups( Class<T> group )
    {
        return globalConfig.getGroups( group );
    }

    @Override
    public <T extends GroupSetting, U extends T> Map<Class<U>,Map<String,U>> getGroupsFromInheritance( Class<T> parentClass )
    {
        return globalConfig.getGroupsFromInheritance( parentClass );
    }

    @Override
    public <T> SettingObserver<T> getObserver( Setting<T> setting )
    {
        return globalConfig.getObserver( setting );
    }

    @Override
    public <T> void setDynamic( Setting<T> setting, T value, String scope )
    {
        globalConfig.setDynamic( setting, value, scope );
    }

    @Override
    public <T> void set( Setting<T> setting, T value )
    {
        globalConfig.set( setting, value );
    }

    @Override
    public <T> void setIfNotSet( Setting<T> setting, T value )
    {
        globalConfig.setIfNotSet( setting, value );
    }

    @Override
    public boolean isExplicitlySet( Setting<?> setting )
    {
        return globalConfig.isExplicitlySet( setting );
    }

    @Override
    public String toString()
    {
        return globalConfig.toString();
    }

    @Override
    public Map<Setting<Object>,Object> getValues()
    {
        return globalConfig.getValues();
    }

    @Override
    public Map<String,Setting<Object>> getDeclaredSettings()
    {
        return globalConfig.getDeclaredSettings();
    }

    @Override
    public String toString( boolean includeNullValues )
    {
        return globalConfig.toString( includeNullValues );
    }

    @Override
    public Setting<Object> getSetting( String name )
    {
        return globalConfig.getSetting( name );
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
    }

    @Override
    public void shutdown()
    {
        for ( var settingListeners : registeredListeners.entrySet() )
        {
            Setting<Object> setting = settingListeners.getKey();
            Collection<SettingChangeListener<Object>> listeners = settingListeners.getValue();
            for ( SettingChangeListener<Object> listener : listeners )
            {
                globalConfig.removeListener( setting, listener );
            }
        }
        registeredListeners = new ConcurrentHashMap<>();
    }

    Config getGlobalConfig()
    {
        return globalConfig;
    }
}
