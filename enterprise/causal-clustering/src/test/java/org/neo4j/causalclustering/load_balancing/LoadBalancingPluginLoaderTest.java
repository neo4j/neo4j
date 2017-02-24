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
package org.neo4j.causalclustering.load_balancing;

import org.junit.Test;

import java.util.Map;
import java.util.Optional;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.load_balancing.plugins.ServerShufflingProcessor;
import org.neo4j.causalclustering.load_balancing.plugins.server_policies.ServerPoliciesPlugin;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class LoadBalancingPluginLoaderTest
{
    private static final String DUMMY_PLUGIN_NAME = "dummy";
    private static final String DOES_NOT_EXIST = "does_not_exist";

    @Test
    public void shouldReturnSelectedPlugin() throws Throwable
    {
        // given
        Config config = Config.empty();
        config.augment( stringMap( CausalClusteringSettings.load_balancing_plugin.name(), DUMMY_PLUGIN_NAME ) );
        config.augment( stringMap( CausalClusteringSettings.load_balancing_shuffle.name(), "false" ) );

        // when
        LoadBalancingProcessor plugin = LoadBalancingPluginLoader.load(
                mock( TopologyService.class ),
                mock( LeaderLocator.class ),
                NullLogProvider.getInstance(),
                config );

        // then
        assertTrue( plugin instanceof DummyLoadBalancingPlugin );
        assertEquals( DUMMY_PLUGIN_NAME, ((DummyLoadBalancingPlugin) plugin).pluginName() );
        assertTrue( ((DummyLoadBalancingPlugin) plugin).wasInitialized );
    }

    @Test
    public void shouldEnableShufflingOfDelegate() throws Throwable
    {
        // given
        Config config = Config.empty();
        config.augment( stringMap( CausalClusteringSettings.load_balancing_plugin.name(), DUMMY_PLUGIN_NAME ) );
        config.augment( stringMap( CausalClusteringSettings.load_balancing_shuffle.name(), "true" ) );

        // when
        LoadBalancingProcessor plugin = LoadBalancingPluginLoader.load(
                mock( TopologyService.class ),
                mock( LeaderLocator.class ),
                NullLogProvider.getInstance(),
                config );

        // then
        assertTrue( plugin instanceof ServerShufflingProcessor );
        assertTrue( ((ServerShufflingProcessor) plugin).delegate() instanceof DummyLoadBalancingPlugin );
    }

    @Test
    public void shouldFindServerPoliciesPlugin() throws Throwable
    {
        // given
        Config config = Config.empty();
        config.augment( stringMap( CausalClusteringSettings.load_balancing_plugin.name(), ServerPoliciesPlugin.PLUGIN_NAME ) );
        config.augment( stringMap( CausalClusteringSettings.load_balancing_shuffle.name(), "false" ) );

        // when
        LoadBalancingProcessor plugin = LoadBalancingPluginLoader.load(
                mock( TopologyService.class ),
                mock( LeaderLocator.class ),
                NullLogProvider.getInstance(),
                config );

        // then
        assertTrue( plugin instanceof ServerPoliciesPlugin );
        assertEquals( ServerPoliciesPlugin.PLUGIN_NAME, ((ServerPoliciesPlugin) plugin).pluginName() );
    }

    @Test
    public void shouldThrowOnInvalidPlugin()
    {
        // given
        Config config = Config.empty();
        config.augment( stringMap( CausalClusteringSettings.load_balancing_plugin.name(), DOES_NOT_EXIST ) );

        try
        {
            // when
            LoadBalancingPluginLoader.validate( config, mock( Log.class ) );
            fail();
        }
        catch ( InvalidSettingException ignored )
        {
            // then
        }
    }

    @Test
    public void shouldNotAcceptInvalidSetting()
    {
        // given
        Config config = Config.empty().augment( stringMap(
                settingFor( DUMMY_PLUGIN_NAME, DummyLoadBalancingPlugin.DO_NOT_USE_THIS_CONFIG ), "true" ) );
        config.augment( stringMap( CausalClusteringSettings.load_balancing_plugin.name(), DUMMY_PLUGIN_NAME ) );

        try
        {
            // when
            LoadBalancingPluginLoader.validate( config, mock( Log.class ) );
            fail();
        }
        catch ( InvalidSettingException ignored )
        {
            // then
        }
    }

    private static String settingFor( String pluginName, String settingName )
    {
        return String.format( "%s.%s.%s", CausalClusteringSettings.load_balancing_config.name(), pluginName, settingName );
    }

    @Service.Implementation( LoadBalancingPlugin.class )
    public static class DummyLoadBalancingPlugin implements LoadBalancingPlugin
    {
        static final String DO_NOT_USE_THIS_CONFIG = "do_not_use";
        boolean wasInitialized;

        public DummyLoadBalancingPlugin()
        {

        }

        @Override
        public void validate( Config config, Log log ) throws InvalidSettingException
        {
            Optional<String> invalidSetting = config.getRaw( settingFor( DUMMY_PLUGIN_NAME, DO_NOT_USE_THIS_CONFIG ) );
            if ( invalidSetting.isPresent() )
            {
                throw new InvalidSettingException( "Do not use this setting" );
            }
        }

        @Override
        public void init( TopologyService topologyService, LeaderLocator leaderLocator, LogProvider logProvider, Config config ) throws Throwable
        {
            wasInitialized = true;
        }

        @Override
        public String pluginName()
        {
            return DUMMY_PLUGIN_NAME;
        }

        @Override
        public Result run( Map<String,String> context )
        {
            return null;
        }
    }
}
