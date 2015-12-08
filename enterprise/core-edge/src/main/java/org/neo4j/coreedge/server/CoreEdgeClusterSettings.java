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
package org.neo4j.coreedge.server;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.Function;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;

import static org.neo4j.helpers.Settings.BOOLEAN;
import static org.neo4j.helpers.Settings.DURATION;
import static org.neo4j.helpers.Settings.INTEGER;
import static org.neo4j.helpers.Settings.MANDATORY;
import static org.neo4j.helpers.Settings.TRUE;
import static org.neo4j.helpers.Settings.list;
import static org.neo4j.helpers.Settings.setting;

@Description( "Settings for Core-Edge Clusters" )
public class CoreEdgeClusterSettings
{
    public static final Function<String, ListenSocketAddress> LISTEN_SOCKET_ADDRESS = new Function<String, ListenSocketAddress>()
    {
        @Override
        public ListenSocketAddress apply( String value )
        {
            String[] split = value.split( ":" );
            return new ListenSocketAddress(new InetSocketAddress( split[0], Integer.valueOf( split[1] ) ));
        }

        @Override
        public String toString()
        {
            return "a socket address";
        }
    };

    public static final Function<String, AdvertisedSocketAddress> ADVERTISED_SOCKET_ADDRESS = new Function<String, AdvertisedSocketAddress>()
    {
        @Override
        public AdvertisedSocketAddress apply( String value )
        {
            String[] split = value.split( ":" );
            return new AdvertisedSocketAddress(new InetSocketAddress( split[0], Integer.valueOf( split[1] ) ));
        }

        @Override
        public String toString()
        {
            return "a socket address";
        }
    };

    @Description( "Time out for a new member to catch up" )
    public static final Setting<Long> join_catch_up_timeout =
            setting( "core_edge.join_catch_up_timeout", DURATION, "10m" );

    @Description( "Leader election timeout" )
    public static final Setting<Long> leader_election_timeout =
            setting( "core_edge.leader_election_timeout", DURATION, "500ms" );

    @Description( "Leader wait timeout" )
    public static final Setting<Long> leader_wait_timeout =
            setting( "core_edge.leader_wait_timeout", DURATION, "30s" );

    @Description( "The maximum batch size when catching up (in unit of entries)" )
    public static final Setting<Integer> catchup_batch_size =
            setting( "core_edge.catchup_batch_size", INTEGER, "64" );

    @Description( "The maximum lag allowed before log shipping pauses (in unit of entries)" )
    public static final Setting<Integer> log_shipping_max_lag  =
            setting( "core_edge.log_shipping_max_lag", INTEGER, "256" );

    @Description( "The time between successive retries of replicating a transaction" )
    public static final Setting<Long> tx_replication_retry_interval  =
            setting( "core_edge.tx_replication_retry_interval", DURATION, "1s" );

    @Description( "The maximum time for trying to replicate a transaction and receive a successful response. " +
                  "Note that the transaction might still have been committed in the cluster." )
    public static final Setting<Long> tx_replication_timeout  =
            setting( "core_edge.tx_replication_timeout", DURATION, "30s" );

    @Description( "Expected size of core cluster" )
    public static final Setting<Integer> expected_core_cluster_size =
            setting( "core_edge.expected_core_cluster_size", INTEGER, "3" );

    @Description( "Timeout for taking remote (write) locks on slaves. Defaults to ha.read_timeout." )
    public static final Setting<Long> lock_read_timeout =
            setting( "core_edge.lock_read_timeout", DURATION, "20s" );

    @Description( "Network interface and port for the RAFT server to listen on." )
    public static final Setting<ListenSocketAddress> transaction_listen_address =
            setting( "core_edge.transaction_listen_address", LISTEN_SOCKET_ADDRESS, "0.0.0.0:6001" );

    @Description( "Hostname/IP address and port that other RAFT servers can use to communicate with us." )
    public static final Setting<AdvertisedSocketAddress> transaction_advertised_address =
            setting( "core_edge.transaction_advertised_address", ADVERTISED_SOCKET_ADDRESS, "localhost:6001" );

    @Description( "Network interface and port for the RAFT server to listen on." )
    public static final Setting<ListenSocketAddress> raft_listen_address =
            setting( "core_edge.raft_listen_address", LISTEN_SOCKET_ADDRESS, "0.0.0.0:7400" );

    @Description( "Hostname/IP address and port that other RAFT servers can use to communicate with us." )
    public static final Setting<AdvertisedSocketAddress> raft_advertised_address =
            setting( "core_edge.raft_advertised_address", ADVERTISED_SOCKET_ADDRESS, "localhost:7400" );

    @Description( "Host and port to bind the cluster management communication." )
    public static final Setting<ListenSocketAddress> cluster_listen_address =
            setting( "core_edge.cluster_listen_address", LISTEN_SOCKET_ADDRESS, "0.0.0.0:5001" );

    @Description( "A comma-separated list of other members of the cluster to join." )
    public static final Setting<List<AdvertisedSocketAddress>> initial_core_cluster_members =
            setting( "core_edge.initial_core_cluster_members", list( ",", ADVERTISED_SOCKET_ADDRESS ), MANDATORY );

    @Description( "Prevents the network middleware from dumping its own logs. Defaults to true." )
    public static final Setting<Boolean> disable_middleware_logging =
            setting( "core_edge.disable_middleware_logging", BOOLEAN, TRUE );
}
