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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.SettingValidator;
import org.neo4j.logging.Log;

import static org.neo4j.kernel.configuration.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.HTTP;


public class ServerConfigurationValidator implements ConfigurationValidator
{
    /**
     * Verifies that at least one http connector is specified and enabled.
     */
    @Override
    @Nonnull
    public Map<String,String> validate( @Nonnull Collection<SettingValidator> settingValidators,
            @Nonnull Map<String,String> rawConfig,
            @Nonnull Log log, boolean parsingFile ) throws InvalidSettingException
    {
        Pattern pattern = Pattern.compile(
                Pattern.quote( "dbms.connector." ) + "([^\\.]+)\\.(.+)" );

        List<Connector> connectors = rawConfig.keySet().stream()
                .map( pattern::matcher )
                .filter( Matcher::matches )
                .map( match -> match.group( 1 ) )
                .distinct()
                .map( Connector::new )
                .collect( Collectors.toList() );

        Map<String,String> validSettings = new HashMap<>( rawConfig );

        // Add missing type info -- validation has succeeded so we can do this with confidence
        connectors.stream()
                .filter( connector -> connector.type.apply( rawConfig::get ) == null )
                .forEach( connector ->
                {
                    if ( "http".equalsIgnoreCase( connector.group.groupKey ) ||
                            "https".equalsIgnoreCase( connector.group.groupKey ) )
                    {
                        validSettings.put( connector.type.name(), HTTP.name() );
                    }
                    else
                    {
                        validSettings.put( connector.type.name(), BOLT.name() );
                    }
                } );

        if ( connectors.stream()
                .filter( connector -> connector.type.apply( validSettings::get ).equals( HTTP ) )
                .noneMatch( connector -> connector.enabled.apply( validSettings::get ) ) )
        {
            throw new InvalidSettingException(
                    String.format( "Missing mandatory enabled connector of type '%s'", HTTP ) );
        }

        return validSettings;
    }
}
