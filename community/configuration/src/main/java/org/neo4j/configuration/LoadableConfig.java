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

package org.neo4j.configuration;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.neo4j.graphdb.config.SettingGroup;

/**
 * Every class which contains settings should implement this interface to allow the configuration to find the
 * settings via service loading.
 */
public interface LoadableConfig
{
    /**
     * Collects settings from implementors which may or may not have descriptions attached to them.
     *
     * @return a list of the implementing class's ConfigOptions
     */
    default List<ConfigOptions> getConfigOptions()
    {
        ArrayList<ConfigOptions> configOptions = new ArrayList<>();
        for ( Field f : getClass().getDeclaredFields() )
        {
            try
            {
                Object publicSetting = f.get( this );
                if ( publicSetting instanceof SettingGroup )
                {

                    final Description documentation = f.getAnnotation( Description.class );
                    final Optional<String> description;
                    if ( documentation == null )
                    {
                        description = Optional.empty();
                    }
                    else
                    {
                        description = Optional.of( documentation.value() );
                    }

                    configOptions.add( new ConfigOptions( (SettingGroup) publicSetting, description ) );
                }
            }
            catch ( IllegalAccessException ignored )
            {
                // Field is private, ignore it
                continue;
            }
        }
        return configOptions;
    }

    /**
     * @return instances of all classes with loadable configuration options
     */
    static List<LoadableConfig> allConfigClasses()
    {
        return StreamSupport.stream( ServiceLoader.load( LoadableConfig.class ).spliterator(), false )
                .collect( Collectors.toList() );
    }

    /**
     * Collects and returns settings of all known implementors.
     * @return all ConfigOptions known at runtime.
     */
    static List<ConfigOptions> loadAllAvailableConfigOptions()
    {
        return StreamSupport.stream( ServiceLoader.load( LoadableConfig.class ).spliterator(), false )
                .map( LoadableConfig::getConfigOptions )
                .flatMap( List::stream )
                .collect( Collectors.toList() );
    }
}
