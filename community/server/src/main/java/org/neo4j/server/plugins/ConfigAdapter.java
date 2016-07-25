/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.plugins;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.AbstractConfiguration;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.configuration.AnnotatedFieldHarvester;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ConfigAdapter extends AbstractConfiguration
{
    private final Config config;

    public ConfigAdapter( Config config )
    {
        this.config = config;
    }

    @Override
    public boolean isEmpty()
    {
        // non-null config is always non-empty as some properties have default values
        return config == null;
    }

    @Override
    public boolean containsKey( String key )
    {
        return getProperty( key ) != null;
    }

    @Override
    public Object getProperty( String key )
    {
        return config.get( getSettingForKey( key ) );
    }

    @Override
    public Iterator<String> getKeys()
    {
        Set<String> propertyKeys = new HashSet<>( config.getParams().keySet() );
        // only keep the properties which have been assigned some values
        for ( String registeredSettingName : getRegisteredSettings().keySet() )
        {
            if ( containsKey( registeredSettingName ) )
            {
                propertyKeys.add( registeredSettingName );
            }
        }
        return propertyKeys.iterator();
    }

    @Override
    protected void addPropertyDirect( String key, Object value )
    {
        config.augment( stringMap( key, value.toString() ) );
    }

    private Setting<?> getSettingForKey( String key )
    {
        Setting<?> setting = getRegisteredSettings().get( key );
        if ( setting != null )
        {
            return setting;
        }
        return Settings.setting( key, Settings.STRING, Settings.NO_DEFAULT );
    }

    private Map<String,Setting<?>> getRegisteredSettings()
    {
        Iterable<Class<?>> settingsClasses = config.getSettingsClasses();
        AnnotatedFieldHarvester fieldHarvester = new AnnotatedFieldHarvester();
        Map<String,Setting<?>> settings = new HashMap<>();
        for ( Class<?> clazz : settingsClasses )
        {
            for ( Pair<Field,Setting> field : fieldHarvester.findStatic( clazz, Setting.class ) )
            {
                settings.put( field.other().name(), field.other() );
            }
        }
        return settings;
    }
}
