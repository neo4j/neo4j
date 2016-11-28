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
package org.neo4j.kernel.configuration;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.SettingValidator;
import org.neo4j.logging.Log;

/**
 * Validates individual settings by delegating to the settings themselves without taking other aspects into
 * consideration.
 */
public class IndividualSettingsValidator implements ConfigurationValidator
{
    @Override
    @Nonnull
    public Map<String,String> validate( @Nonnull Collection<SettingValidator> settingValidators,
            @Nonnull Map<String,String> rawConfig,
            @Nonnull Log log ) throws InvalidSettingException
    {
        Map<String,String> validConfig = settingValidators.stream()
                .map( it -> it.validate( rawConfig ) )
                .flatMap( map -> map.entrySet().stream() )
                .collect( Collectors.toMap( Entry::getKey, Entry::getValue ) );

        rawConfig.forEach( ( key, value ) ->
        {
            if ( !validConfig.containsKey( key ) )
            {
                log.warn( "Unknown config option: %s", key );
            }
        } );

        return validConfig;
    }
}
