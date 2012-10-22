/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import static org.neo4j.graphdb.factory.GraphDatabaseSetting.ANY;
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.FALSE;
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.HostnamePortSetting;
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.TRUE;
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.TimeSpanSetting;

import org.neo4j.graphdb.factory.Default;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.BooleanSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.IntegerSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.OptionsSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.StringSetting;
import org.neo4j.kernel.configuration.ConfigurationMigrator;
import org.neo4j.kernel.configuration.Migrator;
import org.neo4j.kernel.impl.cache.GCResistantCacheProvider;

/**
 * Settings for High Availability mode
 */
public class HaSettings
{
    @Migrator
    public static final ConfigurationMigrator migrator = new EnterpriseConfigurationMigrator();

    @Default("20s")
    public static final TimeSpanSetting read_timeout = new TimeSpanSetting( "ha.read_timeout" );

    @Default("20")
    public static final TimeSpanSetting state_switch_timeout = new TimeSpanSetting( "ha.state_switch_timeout" );

    public static final TimeSpanSetting lock_read_timeout = new TimeSpanSetting( "ha.lock_read_timeout" );

    @Default("20")
    public static final GraphDatabaseSetting.IntegerSetting max_concurrent_channels_per_slave = new
            GraphDatabaseSetting.IntegerSetting( "ha.max_concurrent_channels_per_slave",
            "Must be valid timeout in seconds", 1, null );

    public static final IntegerSetting server_id = new GraphDatabaseSetting.IntegerSetting( "ha.server_id",
            "Must be a valid server id" );

    @Default(":6361")
    public static final HostnamePortSetting ha_server = new HostnamePortSetting( "ha.server" );

    @Default(FALSE)
    public static final BooleanSetting cluster_discovery_enabled = new BooleanSetting( "ha.discovery.enabled" );

    public static final StringSetting cluster_discovery_url = new StringSetting( "ha.discovery.url", ANY,
            "Must be a valid URL" );

    @Default("")
    public static final GraphDatabaseSetting.StringSetting initial_hosts = new GraphDatabaseSetting.StringSetting(
            "ha.initial_hosts", GraphDatabaseSetting.ANY, "Must be a valid list of hosts" );

    @Default(":5001-5099")
    public static final GraphDatabaseSetting.HostnamePortSetting cluster_server = new GraphDatabaseSetting
            .HostnamePortSetting(
            "ha.cluster_server" );

    @Default(TRUE)
    public static final BooleanSetting allow_init_cluster = new BooleanSetting( "ha.allow_init_cluster" );

    @Default("keep_all")
    public static final BranchedDataPolicySetting branched_data_policy = new BranchedDataPolicySetting();

    @Default("0")
    public static final TimeSpanSetting pull_interval = new TimeSpanSetting( "ha.pull_interval" );

    @Description("The amount of slaves the master will ask to replicate a committed transaction. " +
            "The master will not throw an exception on commit if the replication failed.")
    @Default("1")
    public static final IntegerSetting tx_push_factor = new IntegerSetting( "ha.tx_push_factor",
            "Must be a valid replication factor", 0, null );

    @Description("Push strategy of a transaction to a slave during commit. " +
            " Round robin (\"round_robin\")  " +
            " or fixed (\"fixed\") selecting the slave with highest machine id first")
    @Default("fixed")
    public static final OptionsSetting tx_push_strategy = new TxPushStrategySetting();

    public static final GraphDatabaseSetting gcr_node_cache_size = GCResistantCacheProvider.Configuration
            .node_cache_size;
    public static final GraphDatabaseSetting gcr_relationship_cache_size = GCResistantCacheProvider.Configuration
            .relationship_cache_size;
    public static final GraphDatabaseSetting gcr_node_cache_array_fraction = GCResistantCacheProvider.Configuration
            .node_cache_array_fraction;
    public static final GraphDatabaseSetting gcr_relationship_cache_array_fraction = GCResistantCacheProvider
            .Configuration.relationship_cache_array_fraction;
    public static final GraphDatabaseSetting gcr_log_interval = GCResistantCacheProvider.Configuration.log_interval;

    public static class TxPushStrategySetting
            extends OptionsSetting
    {
        @Description("Round robin")
        public static final String roundRobin = "round_robin";

        @Description("Fixed")
        public static final String fixed = "fixed";

        public TxPushStrategySetting()
        {
            super( "ha.tx_push_strategy", roundRobin, fixed );
        }
    }

    public static final class BranchedDataPolicySetting
            extends GraphDatabaseSetting.EnumerableSetting<BranchedDataPolicy>
    {
        public BranchedDataPolicySetting()
        {
            super( "ha.branched_data_policy", BranchedDataPolicy.class );
        }
    }
}
