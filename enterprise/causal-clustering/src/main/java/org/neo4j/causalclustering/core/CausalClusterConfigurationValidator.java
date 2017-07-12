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
package org.neo4j.causalclustering.core;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;

import org.neo4j.causalclustering.load_balancing.LoadBalancingPluginLoader;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.ClusterSettings.Mode;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.SettingValidator;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationValidator;
import org.neo4j.logging.Log;

import static org.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;

public class CausalClusterConfigurationValidator implements ConfigurationValidator
{
    @Override
    @Nonnull
    public Map<String,String> validate( @Nonnull Collection<SettingValidator> settingValidators,
            @Nonnull Map<String,String> rawConfig, @Nonnull Log log, boolean parsingFile ) throws InvalidSettingException
    {
        // Make sure mode is CC
        Mode mode = ClusterSettings.mode.apply( rawConfig::get );
        if ( !mode.equals( Mode.CORE ) && !mode.equals( Mode.READ_REPLICA ) )
        {
            // Nothing to validate
            return rawConfig;
        }

        validateInitialDiscoveryMembers( rawConfig::get );
        validateBoltConnector( rawConfig );
        validateLoadBalancing( rawConfig, log );

        return rawConfig;
    }

    private static void validateLoadBalancing( Map<String,String> rawConfig, Log log )
    {
        LoadBalancingPluginLoader.validate( Config.defaults().augment( rawConfig ), log );
    }

    private static void validateBoltConnector( Map<String,String> rawConfig )
    {
        if ( Config.enabledBoltConnectors( rawConfig ).isEmpty() )
        {
            throw new InvalidSettingException( "A Bolt connector must be configured to run a cluster" );
        }
    }

    private static void validateInitialDiscoveryMembers( Function<String,String> provider )
    {
        if ( initial_discovery_members.apply( provider ) == null )
        {
            throw new InvalidSettingException(
                    String.format( "Missing mandatory non-empty value for '%s'", initial_discovery_members.name() ) );
        }
    }
}
