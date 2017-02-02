/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.configuration;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.ClusterSettings.Mode;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.SettingValidator;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.ConfigurationValidator;
import org.neo4j.logging.Log;

import static org.neo4j.cluster.ClusterSettings.initial_hosts;
import static org.neo4j.cluster.ClusterSettings.server_id;

public class HaConfigurationValidator implements ConfigurationValidator
{
    @Override
    @Nonnull
    public Map<String,String> validate( @Nonnull Collection<SettingValidator> settingValidators,
            @Nonnull Map<String,String> rawConfig, @Nonnull Log log, boolean parsingFile ) throws InvalidSettingException
    {
        // Make sure mode is HA
        Mode mode = ClusterSettings.mode.apply( rawConfig::get );
        if ( !mode.equals( Mode.HA ) && !mode.equals( Mode.ARBITER ) )
        {
            // Nothing to validate
            return rawConfig;
        }

        validateServerId( rawConfig::get );

        validateInitialHosts( rawConfig::get );

        return rawConfig;
    }

    private static void validateServerId( Function<String,String> provider )
    {
        if ( server_id.apply( provider ) == null )
        {
            throw new InvalidSettingException( String.format( "Missing mandatory value for '%s'", server_id.name() ) );
        }
    }

    private static void validateInitialHosts( Function<String,String> provider )
    {
        List<HostnamePort> hosts = initial_hosts.apply( provider );
        if ( hosts == null || hosts.isEmpty() )
        {
            throw new InvalidSettingException(
                    String.format( "Missing mandatory non-empty value for '%s'", initial_hosts.name() ) );
        }
    }
}
