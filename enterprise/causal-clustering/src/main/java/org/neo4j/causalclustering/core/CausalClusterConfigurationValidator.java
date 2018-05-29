/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
