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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import javax.annotation.Nonnull;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigValue;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;

/**
 * Instance of database specific config where you may override specific config values with a local, database specific scope.
 * Currently, when a config value is augmented, we augment the underlying global config (as in {@link DatabaseConfig}) and
 * drop the overridden value stored here, returning the new global value in future.
 */
final class OverriddenDatabaseConfig extends DatabaseConfig
{
    private final CopyOnWriteArraySet<String> overriddenSettings;
    private final Config overriddenConfig;

    OverriddenDatabaseConfig( Config globalConfig, Map<String,String> rawValueOverrides )
    {
        super( globalConfig );

        this.overriddenSettings = new CopyOnWriteArraySet<>( rawValueOverrides.keySet() );
        this.overriddenConfig = createOverriddenConfig( globalConfig, rawValueOverrides );
    }

    @Override
    public <T> T get( Setting<T> setting )
    {
        //We do not simply get from the overriddenConfig in order to pick up augmented changes to underlying config
        if ( overriddenSettings.contains( setting.name() ) )
        {
            return overriddenConfig.get( setting );
        }
        return super.get( setting );
    }

    @Override
    public boolean isConfigured( Setting<?> setting )
    {
        //Do not simply check overridden in order to pick up augmented changes
        if ( overriddenSettings.contains( setting.name() ) )
        {
            return overriddenConfig.isConfigured( setting );
        }
       return super.isConfigured( setting );
    }

    @Override
    public void augment( Map<String,String> settings ) throws InvalidSettingException
    {
        for ( Map.Entry<String,String> entry : settings.entrySet() )
        {
            augment( entry.getKey(), entry.getValue() );
        }
    }

    @Override
    public void augment( String setting, String value ) throws InvalidSettingException
    {
        if ( overriddenSettings.contains( setting ) )
        {
            overriddenConfig.augment( setting, value );
        }
        else
        {
            super.augment( setting, value );
        }
    }

    @Override
    public void augment( Setting<?> setting, String value )
    {
        if ( overriddenSettings.contains( setting.name() ) )
        {
            overriddenConfig.augment( setting, value );
        }
        else
        {
            super.augment( setting, value );
        }
    }

    @Override
    public void augment( Config config ) throws InvalidSettingException
    {
        augment( config.getRaw() );
    }

    @Override
    public Optional<String> getRaw( @Nonnull String key )
    {
        if ( overriddenSettings.contains( key ) )
        {
            return overriddenConfig.getRaw( key );
        }
        return super.getRaw( key );
    }

    @Override
    public Map<String,String> getRaw()
    {
        Map<String,String> combinedRaw = new HashMap<>( super.getRaw() );
        combinedRaw.putAll( overriddenConfig.getRaw() );
        return combinedRaw;
    }

    @Override
    public Optional<Object> getValue( @Nonnull String key )
    {
        if ( overriddenSettings.contains( key ) )
        {
            return overriddenConfig.getValue( key );
        }
        return super.getValue( key );
    }

    @Override
    public void updateDynamicSetting( String setting, String update, String origin ) throws IllegalArgumentException, InvalidSettingException
    {
        super.updateDynamicSetting( setting, update, origin );
        overriddenSettings.remove( setting );
    }

    @Override
    public Map<String,ConfigValue> getConfigValues()
    {
        return createOverriddenConfig( super.globalConfig, overriddenConfig.getRaw() ).getConfigValues();
    }

    @Override
    public Set<String> identifiersFromGroup( Class<?> groupClass )
    {
        return createOverriddenConfig( super.globalConfig, overriddenConfig.getRaw() ).identifiersFromGroup( groupClass );
    }

    private static Config createOverriddenConfig( Config original, Map<String,String> overrides )
    {
        return Config.builder()
                .withSettings( original.getRaw() )
                .withSettings( overrides )
                .build();
    }
}
