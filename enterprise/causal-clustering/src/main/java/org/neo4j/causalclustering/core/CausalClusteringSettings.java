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

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.BaseSetting;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Settings;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logs_directory;
import static org.neo4j.kernel.configuration.Settings.ADVERTISED_SOCKET_ADDRESS;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.BYTES;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.PATH;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.advertisedAddress;
import static org.neo4j.kernel.configuration.Settings.buildSetting;
import static org.neo4j.kernel.configuration.Settings.derivedSetting;
import static org.neo4j.kernel.configuration.Settings.determineDefaultLookup;
import static org.neo4j.kernel.configuration.Settings.list;
import static org.neo4j.kernel.configuration.Settings.listenAddress;
import static org.neo4j.kernel.configuration.Settings.min;
import static org.neo4j.kernel.configuration.Settings.options;
import static org.neo4j.kernel.configuration.Settings.setting;

@Description( "Settings for Causal Clustering" )
public class CausalClusteringSettings implements LoadableConfig
{
    @Description( "Time out for a new member to catch up" )
    public static final Setting<Duration> join_catch_up_timeout =
            setting( "causal_clustering.join_catch_up_timeout", DURATION, "10m" );

    @Description( "The time limit within which a new leader election will occur if no messages are received." )
    public static final Setting<Duration> leader_election_timeout =
            setting( "causal_clustering.leader_election_timeout", DURATION, "7s" );

    @Description( "Prevents the current instance from volunteering to become Raft leader. Defaults to false, and " +
            "should only be used in exceptional circumstances by expert users. Using this can result in reduced " +
            "availability for the cluster." )
    public static final Setting<Boolean> refuse_to_be_leader =
            setting( "causal_clustering.refuse_to_be_leader", BOOLEAN, FALSE );

    @Description( "The maximum batch size when catching up (in unit of entries)" )
    public static final Setting<Integer> catchup_batch_size =
            setting( "causal_clustering.catchup_batch_size", INTEGER, "64" );

    @Description( "The maximum lag allowed before log shipping pauses (in unit of entries)" )
    public static final Setting<Integer> log_shipping_max_lag =
            setting( "causal_clustering.log_shipping_max_lag", INTEGER, "256" );

    @Description( "Size of the RAFT in queue" )
    @Internal
    public static final Setting<Integer> raft_in_queue_size =
            setting( "causal_clustering.raft_in_queue_size", INTEGER, "64" );

    @Description( "Largest batch processed by RAFT" )
    @Internal
    public static final Setting<Integer> raft_in_queue_max_batch =
            setting( "causal_clustering.raft_in_queue_max_batch", INTEGER, "64" );

    @Description( "Expected number of Core machines in the cluster" )
    public static final Setting<Integer> expected_core_cluster_size =
            setting( "causal_clustering.expected_core_cluster_size", INTEGER, "3" );

    @Description( "Network interface and port for the transaction shipping server to listen on." )
    public static final Setting<ListenSocketAddress> transaction_listen_address =
            listenAddress( "causal_clustering.transaction_listen_address", 6000 );

    @Description( "Advertised hostname/IP address and port for the transaction shipping server." )
    public static final Setting<AdvertisedSocketAddress> transaction_advertised_address =
            advertisedAddress( "causal_clustering.transaction_advertised_address", transaction_listen_address );

    @Description( "Network interface and port for the RAFT server to listen on." )
    public static final Setting<ListenSocketAddress> raft_listen_address =
            listenAddress( "causal_clustering.raft_listen_address", 7000 );

    @Description( "Advertised hostname/IP address and port for the RAFT server." )
    public static final Setting<AdvertisedSocketAddress> raft_advertised_address =
            advertisedAddress( "causal_clustering.raft_advertised_address", raft_listen_address );

    @Description( "Host and port to bind the cluster member discovery management communication." )
    public static final Setting<ListenSocketAddress> discovery_listen_address =
            listenAddress( "causal_clustering.discovery_listen_address", 5000 );

    @Description( "Advertised cluster member discovery management communication." )
    public static final Setting<AdvertisedSocketAddress> discovery_advertised_address =
            advertisedAddress( "causal_clustering.discovery_advertised_address", discovery_listen_address );

    @Description( "A comma-separated list of other members of the cluster to join." )
    public static final Setting<List<AdvertisedSocketAddress>> initial_discovery_members =
            setting( "causal_clustering.initial_discovery_members", list( ",", ADVERTISED_SOCKET_ADDRESS ),
                    NO_DEFAULT );

    public enum DiscoveryType
    {
        DNS,
        LIST
    }

    @Description( "Configure the discovery type used for cluster name resolution" )
    public static final Setting<DiscoveryType> discovery_type =
            setting( "causal_clustering.discovery_type", options( DiscoveryType.class ), DiscoveryType.LIST.name() );

    @Description( "Prevents the network middleware from dumping its own logs. Defaults to true." )
    public static final Setting<Boolean> disable_middleware_logging =
            setting( "causal_clustering.disable_middleware_logging", BOOLEAN, TRUE );

    @Description( "Logging level of middleware logging" )
    public static final Setting<Integer> middleware_logging_level =
            setting( "causal_clustering.middleware_logging.level", INTEGER, Integer.toString( Level.FINE.intValue() ) );

    @Internal // not supported yet
    @Description( "Hazelcast license key" )
    public static final Setting<String> hazelcast_license_key =
            setting( "hazelcast.license_key", STRING, NO_DEFAULT );

    @Description( "The maximum file size before the storage file is rotated (in unit of entries)" )
    public static final Setting<Integer> last_flushed_state_size =
            setting( "causal_clustering.last_applied_state_size", INTEGER, "1000" );

    @Description( "The maximum file size before the ID allocation file is rotated (in unit of entries)" )
    public static final Setting<Integer> id_alloc_state_size =
            setting( "causal_clustering.id_alloc_state_size", INTEGER, "1000" );

    @Description( "The maximum file size before the membership state file is rotated (in unit of entries)" )
    public static final Setting<Integer> raft_membership_state_size =
            setting( "causal_clustering.raft_membership_state_size", INTEGER, "1000" );

    @Description( "The maximum file size before the vote state file is rotated (in unit of entries)" )
    public static final Setting<Integer> vote_state_size =
            setting( "causal_clustering.raft_vote_state_size", INTEGER, "1000" );

    @Description( "The maximum file size before the term state file is rotated (in unit of entries)" )
    public static final Setting<Integer> term_state_size =
            setting( "causal_clustering.raft_term_state_size", INTEGER, "1000" );

    @Description( "The maximum file size before the global session tracker state file is rotated (in unit of entries)" )
    public static final Setting<Integer> global_session_tracker_state_size =
            setting( "causal_clustering.global_session_tracker_state_size", INTEGER, "1000" );

    @Description( "The maximum file size before the replicated lock token state file is rotated (in unit of entries)" )
    public static final Setting<Integer> replicated_lock_token_state_size =
            setting( "causal_clustering.replicated_lock_token_state_size", INTEGER, "1000" );

    @Description( "The maximum amount of data which can be in the replication stage concurrently." )
    public static final Setting<Long> replication_total_size_limit =
            setting( "causal_clustering.replication_total_size_limit", BYTES, "128M" );

    @Description( "The initial timeout until replication is retried. The timeout will increase exponentially." )
    public static final Setting<Duration> replication_retry_timeout_base =
            setting( "causal_clustering.replication_retry_timeout_base", DURATION, "10s" );

    @Description( "The upper limit for the exponentially incremented retry timeout." )
    public static final Setting<Duration> replication_retry_timeout_limit =
            setting( "causal_clustering.replication_retry_timeout_limit", DURATION, "60s" );

    @Description( "The number of operations to be processed before the state machines flush to disk" )
    public static final Setting<Integer> state_machine_flush_window_size =
            setting( "causal_clustering.state_machine_flush_window_size", INTEGER, "4096" );

    @Description( "The maximum number of operations to be batched during applications of operations in the state machines" )
    public static final Setting<Integer> state_machine_apply_max_batch_size =
            setting( "causal_clustering.state_machine_apply_max_batch_size", INTEGER, "16" );

    @Description( "RAFT log pruning strategy" )
    public static final Setting<String> raft_log_pruning_strategy =
            setting( "causal_clustering.raft_log_prune_strategy", STRING, "1g size" );

    @Description( "RAFT log implementation" )
    public static final Setting<String> raft_log_implementation =
            setting( "causal_clustering.raft_log_implementation", STRING, "SEGMENTED" );

    @Description( "RAFT log rotation size" )
    public static final Setting<Long> raft_log_rotation_size =
            buildSetting( "causal_clustering.raft_log_rotation_size", BYTES, "250M" ).constraint( min( 1024L ) ).build();

    @Description( "RAFT log reader pool size" )
    public static final Setting<Integer> raft_log_reader_pool_size =
            setting( "causal_clustering.raft_log_reader_pool_size", INTEGER, "8" );

    @Description( "RAFT log pruning frequency" )
    public static final Setting<Duration> raft_log_pruning_frequency =
            setting( "causal_clustering.raft_log_pruning_frequency", DURATION, "10m" );

    @Description( "Enable or disable the dump of all network messages pertaining to the RAFT protocol" )
    @Internal
    public static final Setting<Boolean> raft_messages_log_enable =
            setting( "causal_clustering.raft_messages_log_enable", BOOLEAN, FALSE );

    @Description( "Path to RAFT messages log." )
    @Internal
    public static final Setting<File> raft_messages_log_path =
            derivedSetting( "causal_clustering.raft_messages_log_path", logs_directory,
                    logs -> new File( logs, "raft-messages.log" ), PATH );

    @Description( "Interval of pulling updates from cores." )
    public static final Setting<Duration> pull_interval = setting( "causal_clustering.pull_interval", DURATION, "1s" );

    @Description( "The catch up protocol times out if the given duration elapses with not network activity. " +
            "Every message received by the client from the server extends the time out duration." )
    @Internal
    public static final Setting<Duration> catch_up_client_inactivity_timeout =
            setting( "causal_clustering.catch_up_client_inactivity_timeout", DURATION, "5s" );

    @Description( "Throttle limit for logging unknown cluster member address" )
    public static final Setting<Duration> unknown_address_logging_throttle =
            setting( "causal_clustering.unknown_address_logging_throttle", DURATION, "10000ms" );

    @Description( "Maximum transaction batch size for read replicas when applying transactions pulled from core " +
            "servers." )
    @Internal
    public static Setting<Integer> read_replica_transaction_applier_batch_size =
            setting( "causal_clustering.read_replica_transaction_applier_batch_size", INTEGER, "64" );

    @Description( "Time To Live before read replica is considered unavailable" )
    public static final Setting<Duration> read_replica_time_to_live =
            buildSetting( "causal_clustering.read_replica_time_to_live", DURATION, "1m" ).constraint( min( Duration.ofSeconds( 60 ) ) ).build();

    @Description( "How long drivers should cache the data from the `dbms.cluster.routing.getServers()` procedure." )
    public static final Setting<Duration> cluster_routing_ttl =
            buildSetting( "causal_clustering.cluster_routing_ttl", DURATION, "300s" ).constraint( min( Duration.ofSeconds( 1 ) ) ).build();

    @Description( "Configure if the `dbms.cluster.routing.getServers()` procedure should include followers as read " +
            "endpoints or return only read replicas. Note: if there are no read replicas in the cluster, followers " +
            "are returned as read end points regardless the value of this setting. Defaults to true so that followers" +
            "are available for read-only queries in a typical heterogeneous setup." )
    public static final Setting<Boolean> cluster_allow_reads_on_followers =
            setting( "causal_clustering.cluster_allow_reads_on_followers", BOOLEAN, TRUE );

    @Description( "The size of the ID allocation requests Core servers will make when they run out of NODE IDs. " +
            "Larger values mean less frequent requests but also result in more unused IDs (and unused disk space) " +
            "in the event of a crash." )
    public static final Setting<Integer> node_id_allocation_size =
            setting( "causal_clustering.node_id_allocation_size", INTEGER, "1024" );

    @Description( "The size of the ID allocation requests Core servers will make when they run out " +
            "of RELATIONSHIP IDs. Larger values mean less frequent requests but also result in more unused IDs " +
            "(and unused disk space) in the event of a crash." )
    public static final Setting<Integer> relationship_id_allocation_size =
            setting( "causal_clustering.relationship_id_allocation_size", INTEGER, "1024" );

    @Description( "The size of the ID allocation requests Core servers will make when they run out " +
            "of PROPERTY IDs. Larger values mean less frequent requests but also result in more unused IDs " +
            "(and unused disk space) in the event of a crash." )
    public static final Setting<Integer> property_id_allocation_size =
            setting( "causal_clustering.property_id_allocation_size", INTEGER, "1024" );

    @Description( "The size of the ID allocation requests Core servers will make when they run out " +
            "of STRING_BLOCK IDs. Larger values mean less frequent requests but also result in more unused IDs " +
            "(and unused disk space) in the event of a crash." )
    public static final Setting<Integer> string_block_id_allocation_size =
            setting( "causal_clustering.string_block_id_allocation_size", INTEGER, "1024" );

    @Description( "The size of the ID allocation requests Core servers will make when they run out " +
            "of ARRAY_BLOCK IDs. Larger values mean less frequent requests but also result in more unused IDs " +
            "(and unused disk space) in the event of a crash." )
    public static final Setting<Integer> array_block_id_allocation_size =
            setting( "causal_clustering.array_block_id_allocation_size", INTEGER, "1024" );

    @Description( "The size of the ID allocation requests Core servers will make when they run out " +
            "of PROPERTY_KEY_TOKEN IDs. Larger values mean less frequent requests but also result in more unused IDs " +
            "(and unused disk space) in the event of a crash." )
    public static final Setting<Integer> property_key_token_id_allocation_size =
            setting( "causal_clustering.property_key_token_id_allocation_size", INTEGER, "32" );

    @Description( "The size of the ID allocation requests Core servers will make when they run out " +
            "of PROPERTY_KEY_TOKEN_NAME IDs. Larger values mean less frequent requests but also result in more " +
            "unused IDs (and unused disk space) in the event of a crash." )
    public static final Setting<Integer> property_key_token_name_id_allocation_size =
            setting( "causal_clustering.property_key_token_name_id_allocation_size", INTEGER, "1024" );

    @Description( "The size of the ID allocation requests Core servers will make when they run out " +
            "of RELATIONSHIP_TYPE_TOKEN IDs. Larger values mean less frequent requests but also result in more " +
            "unused IDs (and unused disk space) in the event of a crash." )
    public static final Setting<Integer> relationship_type_token_id_allocation_size =
            setting( "causal_clustering.relationship_type_token_id_allocation_size", INTEGER, "32" );

    @Description( "The size of the ID allocation requests Core servers will make when they run out " +
            "of RELATIONSHIP_TYPE_TOKEN_NAME IDs. Larger values mean less frequent requests but also result in more " +
            "unused IDs (and unused disk space) in the event of a crash." )
    public static final Setting<Integer> relationship_type_token_name_id_allocation_size =
            setting( "causal_clustering.relationship_type_token_name_id_allocation_size", INTEGER, "1024" );

    @Description( "The size of the ID allocation requests Core servers will make when they run out " +
            "of LABEL_TOKEN IDs. Larger values mean less frequent requests but also result in more " +
            "unused IDs (and unused disk space) in the event of a crash." )
    public static final Setting<Integer> label_token_id_allocation_size =
            setting( "causal_clustering.label_token_id_allocation_size", INTEGER, "32" );

    @Description( "The size of the ID allocation requests Core servers will make when they run out " +
            "of LABEL_TOKEN_NAME IDs. Larger values mean less frequent requests but also result in more " +
            "unused IDs (and unused disk space) in the event of a crash." )
    public static final Setting<Integer> label_token_name_id_allocation_size =
            setting( "causal_clustering.label_token_name_id_allocation_size", INTEGER, "1024" );

    @Description( "The size of the ID allocation requests Core servers will make when they run out " +
            "of NEOSTORE_BLOCK IDs. Larger values mean less frequent requests but also result in more " +
            "unused IDs (and unused disk space) in the event of a crash." )
    public static final Setting<Integer> neostore_block_id_allocation_size =
            setting( "causal_clustering.neostore_block_id_allocation_size", INTEGER, "1024" );

    @Description( "The size of the ID allocation requests Core servers will make when they run out " +
            "of SCHEMA IDs. Larger values mean less frequent requests but also result in more " +
            "unused IDs (and unused disk space) in the event of a crash." )
    public static final Setting<Integer> schema_id_allocation_size =
            setting( "causal_clustering.schema_id_allocation_size", INTEGER, "1024" );

    @Description( "The size of the ID allocation requests Core servers will make when they run out " +
            "of NODE_LABELS IDs. Larger values mean less frequent requests but also result in more " +
            "unused IDs (and unused disk space) in the event of a crash." )
    public static final Setting<Integer> node_labels_id_allocation_size =
            setting( "causal_clustering.node_labels_id_allocation_size", INTEGER, "1024" );

    @Description( "The size of the ID allocation requests Core servers will make when they run out " +
            "of RELATIONSHIP_GROUP IDs. Larger values mean less frequent requests but also result in more " +
            "unused IDs (and unused disk space) in the event of a crash." )
    public static final Setting<Integer> relationship_group_id_allocation_size =
            setting( "causal_clustering.relationship_group_id_allocation_size", INTEGER, "1024" );

    @Description( "Time between scanning the cluster to refresh current server's view of topology" )
    public static final Setting<Duration> cluster_topology_refresh =
            buildSetting( "causal_clustering.cluster_topology_refresh", DURATION, "5s" ).constraint( min( Duration.ofSeconds( 1 ) ) ).build();

    @Description( "An ordered list in descending preference of the strategy which read replicas use to choose " +
            "the upstream server from which to pull transactional updates." )
    public static final Setting<List<String>> upstream_selection_strategy =
            setting( "causal_clustering.upstream_selection_strategy", list( ",", STRING ), "default" );

    @Description( "Configuration of a user-defined upstream selection strategy. " +
            "The user-defined strategy is used if the list of strategies (`causal_clustering.upstream_selection_strategy`) " +
            "includes the value `user_defined`. " )
    public static final Setting<String> user_defined_upstream_selection_strategy =
            setting( "causal_clustering.user_defined_upstream_strategy", STRING, "" );

    @Description( "Comma separated list of groups to be used by the connect-randomly-to-server-group selection strategy. " +
            "The connect-randomly-to-server-group strategy is used if the list of strategies (`causal_clustering.upstream_selection_strategy`) " +
            "includes the value `connect-randomly-to-server-group`. " )
    public static final Setting<List<String>> connect_randomly_to_server_group_strategy =
            setting( "causal_clustering.connect-randomly-to-server-group", list( ",", STRING ), "" );

    @Description( "A list of group names for the server used when configuring load balancing and replication policies." )
    public static final Setting<List<String>> server_groups =
            setting( "causal_clustering.server_groups", list( ",", STRING ), "" );

    @Description( "The load balancing plugin to use." )
    public static final Setting<String> load_balancing_plugin =
            setting( "causal_clustering.load_balancing.plugin", STRING, "server_policies" );

    static BaseSetting<String> prefixSetting( final String name, final Function<String, String> parser,
                                              final String defaultValue )
    {
        BiFunction<String, Function<String, String>, String> valueLookup = ( n, settings ) -> settings.apply( n );
        BiFunction<String, Function<String, String>, String> defaultLookup = determineDefaultLookup( defaultValue,
                valueLookup );

        return new Settings.DefaultSetting<String>( name, parser, valueLookup, defaultLookup, Collections.emptyList() )
        {
            @Override
            public Map<String, String> validate( Map<String, String> rawConfig, Consumer<String> warningConsumer )
                    throws InvalidSettingException
            {
                // Validate setting, if present or default value otherwise
                try
                {
                    apply( rawConfig::get );
                    // only return if it was present though

                    Map<String, String> validConfig = new HashMap<>();

                    rawConfig.keySet().stream()
                            .filter( key -> key.startsWith( name() ) )
                            .forEach( key -> validConfig.put( key, rawConfig.get( key ) ) );

                    return validConfig;
                }
                catch ( RuntimeException e )
                {
                    throw new InvalidSettingException( e.getMessage(), e );
                }
            }
        };
    }

    @Description( "The configuration must be valid for the configured plugin and usually exists" +
            "under matching subkeys, e.g. ..config.server_policies.*" +
            "This is just a top-level placeholder for the plugin-specific configuration." )
    public static final Setting<String> load_balancing_config =
            prefixSetting( "causal_clustering.load_balancing.config", STRING, "" );

    @Description( "Enables shuffling of the returned load balancing result." )
    public static final Setting<Boolean> load_balancing_shuffle =
            setting( "causal_clustering.load_balancing.shuffle", BOOLEAN, TRUE );

    @Description( "Require authorization for access to the Causal Clustering status endpoints." )
    public static final Setting<Boolean> status_auth_enabled =
            setting( "dbms.security.causal_clustering_status_auth_enabled", BOOLEAN, TRUE );

    @Description( "Enable multi-data center features. Requires appropriate licensing." )
    public static final Setting<Boolean> multi_dc_license =
            setting( "causal_clustering.multi_dc_license", BOOLEAN, FALSE );

    @Description( "Name of the SSL policy to be used by the clustering, as defined under the dbms.ssl.policy.* settings." +
                  " If no policy is configured then the communication will not be secured." )
    public static final Setting<String> ssl_policy =
            prefixSetting( "causal_clustering.ssl_policy", STRING, NO_DEFAULT );
}
