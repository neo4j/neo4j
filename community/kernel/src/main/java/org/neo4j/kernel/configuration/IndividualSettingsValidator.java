/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.SettingValidator;
import org.neo4j.logging.Log;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.strict_config_validation;

/**
 * Validates individual settings by delegating to the settings themselves without taking other aspects into
 * consideration.
 */
public class IndividualSettingsValidator implements ConfigurationValidator
{
    private static final List<String> reservedPrefixes =
            Arrays.asList( "dbms.", "metrics.", "ha.", "causal_clustering.", "browser.", "tools.", "unsupported." );
    private final boolean warnOnUnknownSettings;

    /**
     *
     * @param warnOnUnknownSettings if unknown options should be logged when strict validation is disabled
     */
    public IndividualSettingsValidator( boolean warnOnUnknownSettings )
    {
        this.warnOnUnknownSettings = warnOnUnknownSettings;
    }

    @Override
    @Nonnull
    public Map<String,String> validate( @Nonnull Collection<SettingValidator> settingValidators,
            @Nonnull Map<String,String> rawConfig,
            @Nonnull Log log, boolean parsingFile ) throws InvalidSettingException
    {
        Map<String,String> validConfig = settingValidators.stream()
                .map( it -> it.validate( rawConfig, msg ->
                {
                    if ( parsingFile )
                    {
                        log.warn( msg );
                    }
                } ) )
                .flatMap( map -> map.entrySet().stream() )
                .collect( Collectors.toMap( Entry::getKey, Entry::getValue ) );

        final boolean strictValidation = strict_config_validation.apply( validConfig::get );

        rawConfig.forEach( ( key, value ) ->
        {
            if ( !validConfig.containsKey( key ) )
            {
                // Plugins rely on custom config options being present.
                // As a compromise, we only warn (and discard) for settings in our own "namespace"
                if ( reservedPrefixes.stream().anyMatch( key::startsWith ) )
                {
                    if ( warnOnUnknownSettings )
                    {
                        log.warn( "Unknown config option: %s", key );
                    }

                    if ( strictValidation )
                    {
                        throw new InvalidSettingException( String.format(
                                "Unknown config option '%s'. To resolve either remove it from your configuration " +
                                        "or set '%s' to false.", key, strict_config_validation.name() ) );
                    }
                    else
                    {
                        validConfig.put( key, value );
                    }
                }
                else
                {
                    validConfig.put( key, value );
                }
            }
        } );

        return validConfig;
    }
}
