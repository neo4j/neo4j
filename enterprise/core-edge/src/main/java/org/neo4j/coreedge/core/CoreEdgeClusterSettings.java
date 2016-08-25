/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.core;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.Function;

import org.neo4j.coreedge.messaging.address.AdvertisedSocketAddress;
import org.neo4j.coreedge.messaging.address.ListenSocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.kernel.configuration.Internal;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.BYTES;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.MANDATORY;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.list;
import static org.neo4j.kernel.configuration.Settings.min;
import static org.neo4j.kernel.configuration.Settings.setting;

@Description("Settings for Core-Edge Clusters")
public class CoreEdgeClusterSettings
{
    private static final Function<String, ListenSocketAddress> LISTEN_SOCKET_ADDRESS = new Function<String,
            ListenSocketAddress>()
    {
        @Override
        public ListenSocketAddress apply( String value )
        {
            String[] split = value.split( ":" );
            return new ListenSocketAddress( new InetSocketAddress( split[0], Integer.valueOf( split[1] ) ) );
        }

        @Override
        public String toString()
        {
            return "a socket address";
        }
    };

    private static final Function<String, AdvertisedSocketAddress> ADVERTISED_SOCKET_ADDRESS = new Function<String,
            AdvertisedSocketAddress>()
    {
        @Override
        public AdvertisedSocketAddress apply( String value )
        {
            return new AdvertisedSocketAddress( value );
        }

        @Override
        public String toString()
        {
            return "a socket address";
        }
    };

    @Description("Time out for a new member to catch up")
    public static final Setting<Long> join_catch_up_timeout =
            setting( "core_edge.join_catch_up_timeout", DURATION, "10m" );

    @Description("The time limit which a new leader election will occur if no messages are received.")
    public static final Setting<Long> leader_election_timeout =
            setting( "core_edge.leader_election_timeout", DURATION, "500ms" );

    @Description("The maximum batch size when catching up (in unit of entries)")
    public static final Setting<Integer> catchup_batch_size =
            setting( "core_edge.catchup_batch_size", INTEGER, "64" );

    @Description("The maximum lag allowed before log shipping pauses (in unit of entries)")
    public static final Setting<Integer> log_shipping_max_lag =
            setting( "core_edge.log_shipping_max_lag", INTEGER, "256" );

    @Description("Size of the RAFT in queue")
    @Internal
    public static final Setting<Integer> raft_in_queue_size =
            setting( "core_edge.raft_in_queue_size", INTEGER, "64" );

    @Description("Largest batch processed by RAFT")
    @Internal
    public static final Setting<Integer> raft_in_queue_max_batch =
            setting( "core_edge.raft_in_queue_max_batch", INTEGER, "64" );

    @Description("Time out for a token to be replicated")
    public static final Setting<Long> token_creation_timeout =
            setting( "core_edge.token_creation_timeout", DURATION, "1s" );

    @Description("Time out waiting for the leader locking token")
    public static final Setting<Long> leader_lock_token_timeout =
            setting( "core_edge.leader_lock_token_timeout", DURATION, "1s" );

    @Description("Expected number of Core machines in the cluster")
    public static final Setting<Integer> expected_core_cluster_size =
            setting( "core_edge.expected_core_cluster_size", INTEGER, "3" );

    @Description("Network interface and port for the transaction shipping server to listen on.")
    public static final Setting<ListenSocketAddress> transaction_listen_address =
            setting( "core_edge.transaction_listen_address", LISTEN_SOCKET_ADDRESS, "0.0.0.0:6000" );

    @Description("Hostname/IP address and port for the transaction shipping server to listen on.")
    public static final Setting<AdvertisedSocketAddress> transaction_advertised_address =
            setting( "core_edge.transaction_advertised_address", ADVERTISED_SOCKET_ADDRESS, "localhost:6000" );

    @Description("Network interface and port for the RAFT server to listen on.")
    public static final Setting<ListenSocketAddress> raft_listen_address =
            setting( "core_edge.raft_listen_address", LISTEN_SOCKET_ADDRESS, "0.0.0.0:7000" );

    @Description("Hostname/IP address and port for the RAFT server to listen on.")
    public static final Setting<AdvertisedSocketAddress> raft_advertised_address =
            setting( "core_edge.raft_advertised_address", ADVERTISED_SOCKET_ADDRESS, "localhost:7000" );

    @Description("Host and port to bind the cluster member discovery management communication.")
    public static final Setting<ListenSocketAddress> discovery_listen_address =
            setting( "core_edge.discovery_listen_address", LISTEN_SOCKET_ADDRESS, "0.0.0.0:5000" );

    @Description("A comma-separated list of other members of the cluster to join.")
    public static final Setting<List<AdvertisedSocketAddress>> initial_discovery_members =
            setting( "core_edge.initial_discovery_members", list( ",", ADVERTISED_SOCKET_ADDRESS ), MANDATORY );

    @Description("Prevents the network middleware from dumping its own logs. Defaults to true.")
    public static final Setting<Boolean> disable_middleware_logging =
            setting( "core_edge.disable_middleware_logging", BOOLEAN, TRUE );

    @Description("The maximum file size before the storage file is rotated (in unit of entries)")
    public static final Setting<Integer> last_flushed_state_size =
            setting( "core_edge.last_applied_state_size", INTEGER, "1000" );

    @Description("The maximum file size before the id allocation file is rotated (in unit of entries)")
    public static final Setting<Integer> id_alloc_state_size =
            setting( "core_edge.id_alloc_state_size", INTEGER, "1000" );

    @Description("The maximum file size before the membership state file is rotated (in unit of entries)")
    public static final Setting<Integer> raft_membership_state_size =
            setting( "core_edge.raft_membership_state_size", INTEGER, "1000" );

    @Description("The maximum file size before the vote state file is rotated (in unit of entries)")
    public static final Setting<Integer> vote_state_size = setting( "core_edge.raft_vote_state_size", INTEGER, "1000" );

    @Description("The maximum file size before the term state file is rotated (in unit of entries)")
    public static final Setting<Integer> term_state_size = setting( "core_edge.raft_term_state_size", INTEGER, "1000" );

    @Description("The maximum file size before the global session tracker state file is rotated (in unit of entries)")
    public static final Setting<Integer> global_session_tracker_state_size =
            setting( "core_edge.global_session_tracker_state_size", INTEGER, "1000" );

    @Description("The maximum file size before the replicated lock token state file is rotated (in unit of entries)")
    public static final Setting<Integer> replicated_lock_token_state_size =
            setting( "core_edge.replicated_lock_token_state_size", INTEGER, "1000" );

    @Description("The number of messages waiting to be sent to other servers in the cluster")
    public static final Setting<Integer> outgoing_queue_size =
            setting( "core_edge.outgoing_queue_size", INTEGER, "64" );

    @Description("The number of operations to be processed before the state machines flush to disk")
    public static final Setting<Integer> state_machine_flush_window_size =
            setting( "core_edge.state_machine_flush_window_size", INTEGER, "100" );

    @Description("The maximum number of operations to be batched during applications of operations in the state machines")
    public static Setting<Integer> state_machine_apply_max_batch_size =
            setting( "core_edge.state_machine_apply_max_batch_size", INTEGER, "16" );

    @Description( "RAFT log pruning strategy" )
    public static final Setting<String> raft_log_pruning_strategy =
            setting( "core_edge.raft_log_prune_strategy", STRING, "keep_all" );

    @Description( "RAFT log implementation" )
    public static final Setting<String> raft_log_implementation =
            setting( "core_edge.raft_log_implementation", STRING, "SEGMENTED" );

    @Description( "RAFT log rotation size" )
    public static final Setting<Long> raft_log_rotation_size =
            setting( "core_edge.raft_log_rotation_size", BYTES, "1M", min( 1024L ) );

    @Description( "RAFT log reader pool size" )
    public static final Setting<Integer> raft_log_reader_pool_size =
            setting( "core_edge.raft_log_reader_pool_size", INTEGER, "8" );

    @Description( "RAFT log pruning frequency" )
    public static final Setting<Long> raft_log_pruning_frequency =
            setting( "core_edge.raft_log_pruning_frequency", DURATION, "10m" );

    @Description("Enable or disable the dump of all network messages pertaining to the RAFT protocol")
    public static final Setting<Boolean> raft_messages_log_enable =
            setting( "core_edge.raft_messages_log_enable", BOOLEAN, "true");

    @Description( "Interval of pulling updates from cores." )
    public static final Setting<Long> pull_interval = setting( "core_edge.pull_interval", DURATION, "1s" );

    @Description("Throttle limit for logging unknown cluster member address")
    public static final Setting<Long> unknown_address_logging_throttle =
            setting( "core_edge.unknown_address_logging_throttle", DURATION, "10000ms" );

    @Description( "Maximum transaction batch size for edge servers when applying transactions pulled from core servers." )
    @Internal
    public static Setting<Integer> edge_transaction_applier_batch_size =
            setting( "core_edge.edge_transaction_applier_batch_size", INTEGER, "16" );

    @Description( "Time To Live before edge server is considered unavailable" )
    public static final Setting<Long> edge_time_to_live =
            setting( "core_edge.edge_time_to_live", DURATION, "1m", min(60_000L) );

    @Description( "Edge server 'call home' frequency"  )
    public static final Setting<Long> edge_refresh_rate =
            setting( "core_edge.edge_refresh_rate", DURATION, "5s", min(5_000L) );

    @Description( "NODE ID Allocation Space Size" )
    public static final Setting<Integer> node_id_allocation_size =
            setting( "core_edge.node_id_allocation_size", INTEGER, "1024" );

    @Description( "RELATIONSHIP ID Allocation Space Size" )
    public static final Setting<Integer> relationship_id_allocation_size =
            setting( "core_edge.relationship_id_allocation_size", INTEGER, "1024" );

    @Description( "PROPERTY ID Allocation Space Size" )
    public static final Setting<Integer> property_id_allocation_size =
            setting( "core_edge.property_id_allocation_size", INTEGER, "1024" );

    @Description( "STRING_BLOCK ID Allocation Space Size" )
    public static final Setting<Integer> string_block_id_allocation_size =
            setting( "core_edge.string_block_id_allocation_size", INTEGER, "1024" );

    @Description( "ARRAY_BLOCK ID Allocation Space Size" )
    public static final Setting<Integer> array_block_id_allocation_size =
            setting( "core_edge.array_block_id_allocation_size", INTEGER, "1024" );

    @Description( "PROPERTY_KEY_TOKEN ID Allocation Space Size" )
    public static final Setting<Integer> property_key_token_id_allocation_size =
            setting( "core_edge.property_key_token_id_allocation_size", INTEGER, "32" );

    @Description( "PROPERTY_KEY_TOKEN_NAME ID Allocation Space Size" )
    public static final Setting<Integer> property_key_token_name_id_allocation_size =
            setting( "core_edge.property_key_token_name_id_allocation_size", INTEGER, "1024" );

    @Description( "RELATIONSHIP_TYPE_TOKEN ID Allocation Space Size" )
    public static final Setting<Integer> relationship_type_token_id_allocation_size =
            setting( "core_edge.relationship_type_token_id_allocation_size", INTEGER, "32" );

    @Description( "RELATIONSHIP_TYPE_TOKEN_NAME ID Allocation Space Size" )
    public static final Setting<Integer> relationship_type_token_name_id_allocation_size =
            setting( "core_edge.relationship_type_token_name_id_allocation_size", INTEGER, "1024" );

    @Description( "LABEL_TOKEN ID Allocation Space Size" )
    public static final Setting<Integer> label_token_id_allocation_size =
            setting( "core_edge.label_token_id_allocation_size", INTEGER, "32" );

    @Description( "LABEL_TOKEN_NAME ID Allocation Space Size" )
    public static final Setting<Integer> label_token_name_id_allocation_size =
            setting( "core_edge.label_token_name_id_allocation_size", INTEGER, "1024" );

    @Description( "NEOSTORE_BLOCK ID Allocation Space Size" )
    public static final Setting<Integer> neostore_block_id_allocation_size =
            setting( "core_edge.neostore_block_id_allocation_size", INTEGER, "1024" );

    @Description( "SCHEMA ID Allocation Space Size" )
    public static final Setting<Integer> schema_id_allocation_size =
            setting( "core_edge.schema_id_allocation_size", INTEGER, "1024" );

    @Description( "NODE_LABELS ID Allocation Space Size" )
    public static final Setting<Integer> node_labels_id_allocation_size =
            setting( "core_edge.node_labels_id_allocation_size", INTEGER, "1024" );

    @Description( "RELATIONSHIP_GROUP ID Allocation Space Size" )
    public static final Setting<Integer> relationship_group_id_allocation_size =
            setting( "core_edge.relationship_group_id_allocation_size", INTEGER, "1024" );
}
