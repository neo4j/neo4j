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
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.TRUE;

import org.neo4j.graphdb.config.Setting;
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
 * Settings for high availability mode
 */
public class HaSettings
{
	@Migrator
	public static final ConfigurationMigrator migrator = new EnterpriseConfigurationMigrator();

    public static final GraphDatabaseSetting.StringSetting coordinators = new GraphDatabaseSetting.StringSetting( "ha.coordinators", ANY, "Must be valid list of host names" );

    @Default("20")
    public static final GraphDatabaseSetting.IntegerSetting read_timeout = new GraphDatabaseSetting.IntegerSetting( "ha.read_timeout", "Must be valid timeout in seconds",5,null );

    public static final GraphDatabaseSetting.IntegerSetting lock_read_timeout = new GraphDatabaseSetting.IntegerSetting( "ha.lock_read_timeout", "Must be valid timeout in seconds",1,null );

    @Default("20")
    public static final GraphDatabaseSetting.IntegerSetting max_concurrent_channels_per_slave = new GraphDatabaseSetting.IntegerSetting( "ha.max_concurrent_channels_per_slave", "Must be valid timeout in seconds",1,null );

    public static final IntegerSetting server_id = new GraphDatabaseSetting.IntegerSetting( "ha.server_id", "Must be a valid server id" );

    public static final StringSetting server = new StringSetting( "ha.server", ANY, "Must be a valid IP and port to bind to as master" );

    @Default( SlaveUpdateModeSetting.async )
    public static final SlaveUpdateModeSetting slave_coordinator_update_mode = new SlaveUpdateModeSetting();

    @Default( "neo4j.ha" )
    public static final GraphDatabaseSetting.StringSetting cluster_name = new GraphDatabaseSetting.StringSetting( "ha.cluster_name", ANY, "Must be a valid cluster name" );

    @Default( TRUE )
    public static final BooleanSetting allow_init_cluster = new BooleanSetting( "ha.allow_init_cluster" );

    @Default("5000")
    public static final IntegerSetting zk_session_timeout = new IntegerSetting( "ha.zk_session_timeout", "Must be a valid timeout in milliseconds", 1, null );

    @Default("500")
    public static final IntegerSetting coordinator_fetch_info_timeout = new IntegerSetting( "ha.coordinator_fetch_info_timeout", "Must be a valid timeout in seconds", 1, null );

    @Default( BranchedDataPolicySetting.keep_all )
    public static final BranchedDataPolicySetting branched_data_policy = new BranchedDataPolicySetting();

    @Default( "0" )
    public static final StringSetting pull_interval = new StringSetting( "ha.pull_interval", ANY, "Must be valid interval setting" );

    @Description(   "The amount of slaves the master will ask to replicate a committed transaction. " +
                    "The master will not throw an exception on commit if the replication failed." )
    @Default( "1" )
    public static final IntegerSetting tx_push_factor = new IntegerSetting( "ha.tx_push_factor", "Must be a valid replication factor", 0, null );


    @Description(   "Push strategy of a transaction to a slave during commit. " +
                    " Round robin (\"round_robin\")  " +
                    " or fixed (\"fixed\") selecting the slave with highest machine id first" )
    @Default( "fixed" )
    public static final OptionsSetting tx_push_strategy = new TxPushStrategySetting();
    
    @Description( "Max size of the data chunks that flows between master and slaves in HA. Bigger size may increase throughput," +
    		"but may be more sensitive to variations in bandwidth, whereas lower size increases tolerance for bandwidth variations. " +
            "Examples: 500k or 3M. Must be within 1k-16M" )
    @Default( "2M" )
    public static final GraphDatabaseSetting<Integer> com_chunk_size = new GraphDatabaseSetting.IntegerRangeNumberOfBytesSetting( "ha.com_chunk_size", 1 * 1024 );

    public static final Setting gcr_node_cache_size = GCResistantCacheProvider.Configuration.node_cache_size;
    public static final Setting gcr_relationship_cache_size = GCResistantCacheProvider.Configuration.relationship_cache_size;
    public static final Setting gcr_node_cache_array_fraction = GCResistantCacheProvider.Configuration.node_cache_array_fraction;
    public static final Setting gcr_relationship_cache_array_fraction = GCResistantCacheProvider.Configuration.relationship_cache_array_fraction;

    public static final Setting gcr_log_interval = GCResistantCacheProvider.Configuration.log_interval;

    public static class TxPushStrategySetting
        extends OptionsSetting
    {
        @Description( "Round robin" )
        public static final String roundRobin = "round_robin";

        @Description( "Fixed" )
        public static final String fixed = "fixed";

        public TxPushStrategySetting( )
        {
            super( "ha.tx_push_strategy", roundRobin, fixed );
        }
    }

    public static final class SlaveUpdateModeSetting
        extends GraphDatabaseSetting.OptionsSetting
    {
        @Description( "Update mode 'sync'" )
        public static final String sync = "sync";

        @Description( "Update mode 'async'" )
        public static final String async = "async";

        @Description( "Update mode 'none'" )
        public static final String none = "none";

        public SlaveUpdateModeSetting(  )
        {
            super( "ha.slave_coordinator_update_mode", sync, async, none );
        }
    }

    public static final class BranchedDataPolicySetting
        extends GraphDatabaseSetting.OptionsSetting
    {
        @Description( "Update mode 'keep_all'" )
        public static final String keep_all = "keep_all";

        @Description( "Update mode 'keep_last'" )
        public static final String keep_last = "keep_last";

        @Description( "Update mode 'keep_none'" )
        public static final String keep_none = "keep_none";

        @Description( "Update mode 'shutdown'" )
        public static final String shutdown = "shutdown";

        public BranchedDataPolicySetting(  )
        {
            super( "ha.branched_data_policy", keep_all, keep_last, keep_none, shutdown );
        }
    }
}
