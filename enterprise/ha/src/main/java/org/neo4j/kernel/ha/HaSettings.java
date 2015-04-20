/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.configuration.ConfigurationMigrator;
import org.neo4j.kernel.configuration.Migrator;

import static org.neo4j.helpers.Settings.BOOLEAN;
import static org.neo4j.helpers.Settings.BYTES;
import static org.neo4j.helpers.Settings.DURATION;
import static org.neo4j.helpers.Settings.HOSTNAME_PORT;
import static org.neo4j.helpers.Settings.INTEGER;
import static org.neo4j.helpers.Settings.min;
import static org.neo4j.helpers.Settings.options;
import static org.neo4j.helpers.Settings.setting;

/**
 * Settings for High Availability mode
 */
@Description( "High Availability configuration settings" )
public class HaSettings
{
    @Migrator
    public static final ConfigurationMigrator migrator = new EnterpriseConfigurationMigrator();

    @Description( "How long a slave will wait for response from master before giving up." )
    public static final Setting<Long> read_timeout = setting( "ha.read_timeout", DURATION, "20s" );

    @Description( "Timeout for waiting for instance to become master or slave." )
    public static final Setting<Long> state_switch_timeout = setting( "ha.state_switch_timeout", DURATION, "120s" );

    @Description( "Timeout for taking remote (write) locks on slaves. Defaults to ha.read_timeout." )
    public static final Setting<Long> lock_read_timeout = setting( "ha.lock_read_timeout", DURATION, read_timeout );

    @Description( "Maximum number of connections a slave can have to the master." )
    public static final Setting<Integer> max_concurrent_channels_per_slave =
            setting( "ha.max_concurrent_channels_per_slave", INTEGER, "20", min( 1 ) );

    @Description( "Hostname and port to bind the HA server." )
    public static final Setting<HostnamePort> ha_server = setting( "ha.server", HOSTNAME_PORT, "0.0.0.0:6001-6011" );

    @Description("Whether this instance should only participate as slave in cluster. "
            + "If set to `true`, it will never be elected as master.")
    public static final Setting<Boolean> slave_only = setting( "ha.slave_only", BOOLEAN, Settings.FALSE );

    @Description( "Policy for how to handle branched data." )
    public static final Setting<BranchedDataPolicy> branched_data_policy = setting( "ha.branched_data_policy",
            options( BranchedDataPolicy.class ), "keep_all" );

    @Description( "Max size of the data chunks that flows between master and slaves in HA. Bigger size may increase " +
            "throughput, but may also be more sensitive to variations in bandwidth, whereas lower size increases tolerance" +
            " for bandwidth variations." )
    public static final Setting<Long> com_chunk_size =
            setting( "ha.com_chunk_size", BYTES, "2M", min( 1024L ) );

    @Description( "Interval of pulling updates from master." )
    public static final Setting<Long> pull_interval = setting( "ha.pull_interval", DURATION, "0s" );

    @Description( "The amount of slaves the master will ask to replicate a committed transaction. " )
    public static final Setting<Integer> tx_push_factor = setting( "ha.tx_push_factor", INTEGER, "1", min( 0 ) );

    @Description( "Push strategy of a transaction to a slave during commit." )
    public static final Setting<TxPushStrategy> tx_push_strategy = setting( "ha.tx_push_strategy", options(
            TxPushStrategy.class ), "fixed" );

    public static enum TxPushStrategy
    {
        @Description("Round robin")
        round_robin,

        @Description("Fixed")
        fixed
    }
}
