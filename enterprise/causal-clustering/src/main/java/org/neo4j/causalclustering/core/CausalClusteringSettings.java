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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.logging.Level;

import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCacheFactory;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactorySelector;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.configuration.ReplacedBy;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;

import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols.Implementations.GZIP;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols.Implementations.LZ4;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols.Implementations.LZ4_HIGH_COMPRESSION;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols.Implementations.LZ4_HIGH_COMPRESSION_VALIDATING;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols.Implementations.LZ_VALIDATING;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols.Implementations.SNAPPY;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols.Implementations.SNAPPY_VALIDATING;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logs_directory;
import static org.neo4j.kernel.configuration.Settings.ADVERTISED_SOCKET_ADDRESS;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.BYTES;
import static org.neo4j.kernel.configuration.Settings.DOUBLE;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.PATH;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.STRING_LIST;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.advertisedAddress;
import static org.neo4j.kernel.configuration.Settings.buildSetting;
import static org.neo4j.kernel.configuration.Settings.derivedSetting;
import static org.neo4j.kernel.configuration.Settings.list;
import static org.neo4j.kernel.configuration.Settings.listenAddress;
import static org.neo4j.kernel.configuration.Settings.min;
import static org.neo4j.kernel.configuration.Settings.optionsIgnoreCase;
import static org.neo4j.kernel.configuration.Settings.prefixSetting;
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

    @Internal
    @Description( "Configures the time after which we give up trying to bind to a cluster formed of the other initial discovery members." )
    public static final Setting<Duration> cluster_binding_timeout = setting( "causal_clustering.cluster_binding_timeout", DURATION, "5m" );

    @Description( "Prevents the current instance from volunteering to become Raft leader. Defaults to false, and " +
            "should only be used in exceptional circumstances by expert users. Using this can result in reduced " +
            "availability for the cluster." )
    public static final Setting<Boolean> refuse_to_be_leader =
            setting( "causal_clustering.refuse_to_be_leader", BOOLEAN, FALSE );

    @Description( "The name of the database being hosted by this server instance. This configuration setting may be safely ignored " +
            "unless deploying a multicluster. Instances may be allocated to distinct sub-clusters by assigning them distinct database " +
            "names using this setting. For instance if you had 6 instances you could form 2 sub-clusters by assigning half " +
            "the database name \"foo\", half the name \"bar\". The setting value must match exactly between members of the same sub-cluster. " +
            "This setting is a one-off: once an instance is configured with a database name it may not be changed in future without using " +
            "neo4j-admin unbind." )
    public static final Setting<String> database =
            setting( "causal_clustering.database", STRING, "default" );

    @Description( "Enable pre-voting extension to the Raft protocol (this is breaking and must match between the core cluster members)" )
    public static final Setting<Boolean> enable_pre_voting =
            setting( "causal_clustering.enable_pre_voting", BOOLEAN, FALSE );

    @Description( "The maximum batch size when catching up (in unit of entries)" )
    public static final Setting<Integer> catchup_batch_size =
            setting( "causal_clustering.catchup_batch_size", INTEGER, "64" );

    @Description( "The maximum lag allowed before log shipping pauses (in unit of entries)" )
    public static final Setting<Integer> log_shipping_max_lag =
            setting( "causal_clustering.log_shipping_max_lag", INTEGER, "256" );

    @Internal
    @Description( "Maximum number of entries in the RAFT in-queue" )
    public static final Setting<Integer> raft_in_queue_size =
            setting( "causal_clustering.raft_in_queue_size", INTEGER, "1024" );

    @Description( "Maximum number of bytes in the RAFT in-queue" )
    public static final Setting<Long> raft_in_queue_max_bytes =
            setting( "causal_clustering.raft_in_queue_max_bytes", BYTES, "2G" );

    @Internal
    @Description( "Largest batch processed by RAFT in number of entries" )
    public static final Setting<Integer> raft_in_queue_max_batch =
            setting( "causal_clustering.raft_in_queue_max_batch", INTEGER, "128" );

    @Description( "Largest batch processed by RAFT in bytes" )
    public static final Setting<Long> raft_in_queue_max_batch_bytes =
            setting( "causal_clustering.raft_in_queue_max_batch_bytes", BYTES, "8M" );

    @Description( "Expected number of Core machines in the cluster before startup" )
    @Deprecated
    @ReplacedBy( "causal_clustering.minimum_core_cluster_size_at_formation and causal_clustering.minimum_core_cluster_size_at_runtime" )
    public static final Setting<Integer> expected_core_cluster_size =
            setting( "causal_clustering.expected_core_cluster_size", INTEGER, "3" );

    @Description( "Minimum number of Core machines in the cluster at formation. The expected_core_cluster size setting is used when bootstrapping the " +
            "cluster on first formation. A cluster will not form without the configured amount of cores and this should in general be configured to the" +
            " full and fixed amount. When using multi-clustering (configuring multiple distinct database names across core hosts), this setting is used " +
            "to define the minimum size of *each* sub-cluster at formation." )
    public static final Setting<Integer> minimum_core_cluster_size_at_formation =
            buildSetting( "causal_clustering.minimum_core_cluster_size_at_formation", INTEGER, expected_core_cluster_size.getDefaultValue() )
                    .constraint( min( 2 ) ).build();

    @Description( "Minimum number of Core machines required to be available at runtime. The consensus group size (core machines successfully voted into the " +
            "Raft) can shrink and grow dynamically but bounded on the lower end at this number. The intention is in almost all cases for users to leave this " +
            "setting alone. If you have 5 machines then you can survive failures down to 3 remaining, e.g. with 2 dead members. The three remaining can " +
            "still vote another replacement member in successfully up to a total of 6 (2 of which are still dead) and then after this, one of the " +
            "superfluous dead members will be immediately and automatically voted out (so you are left with 5 members in the consensus group, 1 of which " +
            "is currently dead). Operationally you can now bring the last machine up by bringing in another replacement or repairing the dead one. " +
            "When using multi-clustering (configuring multiple distinct database names across core hosts), this setting is used to define the minimum size " +
            "of *each* sub-cluster at runtime." )
    public static final Setting<Integer> minimum_core_cluster_size_at_runtime =
            buildSetting( "causal_clustering.minimum_core_cluster_size_at_runtime", INTEGER, "3" ).constraint( min( 2 ) ).build();

    @Description( "Network interface and port for the transaction shipping server to listen on. Please note that it is also possible to run the backup " +
            "client against this port so always limit access to it via the firewall and configure an ssl policy." )
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

    @Description( "Type of in-flight cache." )
    public static final Setting<InFlightCacheFactory.Type> in_flight_cache_type =
            setting( "causal_clustering.in_flight_cache.type", optionsIgnoreCase( InFlightCacheFactory.Type.class ),
                    InFlightCacheFactory.Type.CONSECUTIVE.name() );

    @Description( "The maximum number of entries in the in-flight cache." )
    public static final Setting<Integer> in_flight_cache_max_entries =
            setting( "causal_clustering.in_flight_cache.max_entries", INTEGER, "1024" );

    @Description( "The maximum number of bytes in the in-flight cache." )
    public static final Setting<Long> in_flight_cache_max_bytes =
            setting( "causal_clustering.in_flight_cache.max_bytes", BYTES, "2G" );

    @Description( "Address for Kubernetes API" )
    public static final Setting<AdvertisedSocketAddress> kubernetes_address =
            setting( "causal_clustering.kubernetes.address", ADVERTISED_SOCKET_ADDRESS, "kubernetes.default.svc:443" );

    @Description( "File location of token for Kubernetes API" )
    public static final Setting<File> kubernetes_token =
            pathUnixAbsolute( "causal_clustering.kubernetes.token", "/var/run/secrets/kubernetes.io/serviceaccount/token" );

    @Description( "File location of namespace for Kubernetes API" )
    public static final Setting<File> kubernetes_namespace =
            pathUnixAbsolute( "causal_clustering.kubernetes.namespace", "/var/run/secrets/kubernetes.io/serviceaccount/namespace" );

    @Description( "File location of CA certificate for Kubernetes API" )
    public static final Setting<File> kubernetes_ca_crt =
            pathUnixAbsolute( "causal_clustering.kubernetes.ca_crt", "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt" );

    /**
     * Creates absolute path on the first filesystem root. This will be `/` on Unix but arbitrary on Windows.
     * If filesystem roots cannot be listed then `//` will be used - this will be resolved to `/` on Unix and `\\` (a UNC network path) on Windows.
     * An absolute path is always needed for validation, even though we only care about a path on Linux.
     */
    private static Setting<File> pathUnixAbsolute( String name, String path )
    {
        File[] roots = File.listRoots();
        Path root = roots.length > 0 ? roots[0].toPath() : Paths.get( "//" );
        return setting( name, PATH, root.resolve( path ).toString() );
    }

    @Description( "LabelSelector for Kubernetes API" )
    public static final Setting<String> kubernetes_label_selector =
            setting( "causal_clustering.kubernetes.label_selector", STRING, NO_DEFAULT );

    @Description( "Service port name for discovery for Kubernetes API" )
    public static final Setting<String> kubernetes_service_port_name =
            setting( "causal_clustering.kubernetes.service_port_name", STRING, NO_DEFAULT );

    @Internal
    @Description( "The polling interval when attempting to resolve initial_discovery_members from DNS and SRV records." )
    public static final Setting<Duration> discovery_resolution_retry_interval =
            setting( "causal_clustering.discovery_resolution_retry_interval", DURATION, "5s" );

    @Internal
    @Description( "Configures the time after which we give up trying to resolve a DNS/SRV record into a list of initial discovery members." )
    public static final Setting<Duration> discovery_resolution_timeout =
            setting( "causal_clustering.discovery_resolution_timeout", DURATION, "5m" );

    @Description( "Configure the discovery type used for cluster name resolution" )
    public static final Setting<DiscoveryType> discovery_type =
            setting( "causal_clustering.discovery_type", optionsIgnoreCase( DiscoveryType.class ), DiscoveryType.LIST.name() );

    @Internal
    @Description( "Select the middleware used for cluster topology discovery" )
    public static final Setting<DiscoveryServiceFactorySelector.DiscoveryImplementation> discovery_implementation =
            setting( "causal_clustering.discovery_implementation", optionsIgnoreCase( DiscoveryServiceFactorySelector.DiscoveryImplementation.class ),
                    DiscoveryServiceFactorySelector.DEFAULT.name() );

    @Description( "Prevents the network middleware from dumping its own logs. Defaults to true." )
    public static final Setting<Boolean> disable_middleware_logging =
            setting( "causal_clustering.disable_middleware_logging", BOOLEAN, TRUE );

    @Description( "The level of middleware logging" )
    public static final Setting<Integer> middleware_logging_level =
            setting( "causal_clustering.middleware_logging.level", INTEGER, Integer.toString( Level.FINE.intValue() ) );

    @Internal // not supported yet
    @Description( "Hazelcast license key" )
    public static final Setting<String> hazelcast_license_key =
            setting( "hazelcast.license_key", STRING, NO_DEFAULT );

    @Internal
    @Description( "Parallelism level of default dispatcher used by Akka based cluster topology discovery, including cluster, replicator, and discovery actors" )
    public static final Setting<Integer> middleware_akka_default_parallelism_level =
            setting( "causal_clustering.middleware.akka.default-parallelism", INTEGER, Integer.toString( 4 ) );

    @Internal
    @Description( "Parallelism level of dispatcher used for communication from Akka based cluster topology discovery " )
    public static final Setting<Integer> middleware_akka_sink_parallelism_level =
            setting( "causal_clustering.middleware.akka.sink-parallelism", INTEGER, Integer.toString( 2 ) );

    /*
        Begin akka failure detector
        setting descriptions copied from reference.conf in akka-cluster
     */
    @Internal
    @Description( "Akka cluster phi accrual failure detector. " +
            "How often keep-alive heartbeat messages should be sent to each connection." )
    public static final Setting<Duration> akka_failure_detector_heartbeat_interval =
            setting( "causal_clustering.middleware.akka.failure_detector.heartbeat_interval", DURATION, "1s" );

    @Internal
    @Description( "Akka cluster phi accrual failure detector. " +
            "Defines the failure detector threshold. " +
            "A low threshold is prone to generate many wrong suspicions but ensures " +
            "a quick detection in the event of a real crash. Conversely, a high " +
            "threshold generates fewer mistakes but needs more time to detect actual crashes." )
    public static final Setting<Double> akka_failure_detector_threshold =
            setting( "causal_clustering.middleware.akka.failure_detector.threshold", DOUBLE, "10.0" );

    @Internal
    @Description( "Akka cluster phi accrual failure detector. " +
            "Number of the samples of inter-heartbeat arrival times to adaptively " +
            "calculate the failure timeout for connections." )
    public static final Setting<Integer> akka_failure_detector_max_sample_size =
            setting( "causal_clustering.middleware.akka.failure_detector.max_sample_size", INTEGER, "1000" );

    @Internal
    @Description( "Akka cluster phi accrual failure detector. " +
            "Minimum standard deviation to use for the normal distribution in " +
            "AccrualFailureDetector. Too low standard deviation might result in " +
            "too much sensitivity for sudden, but normal, deviations in heartbeat inter arrival times." )
    public static final Setting<Duration> akka_failure_detector_min_std_deviation =
            setting( "causal_clustering.middleware.akka.failure_detector.min_std_deviation", DURATION, "100ms" );

    @Internal
    @Description( "Akka cluster phi accrual failure detector. " +
            "Number of potentially lost/delayed heartbeats that will be " +
            "accepted before considering it to be an anomaly. " +
            "This margin is important to be able to survive sudden, occasional, " +
            "pauses in heartbeat arrivals, due to for example garbage collect or network drop." )
    public static final Setting<Duration> akka_failure_detector_acceptable_heartbeat_pause =
            setting( "causal_clustering.middleware.akka.failure_detector.acceptable_heartbeat_pause", DURATION, "4s" );

    @Internal
    @Description( "Akka cluster phi accrual failure detector. " +
            "Number of member nodes that each member will send heartbeat messages to, " +
            "i.e. each node will be monitored by this number of other nodes." )
    public static final Setting<Integer> akka_failure_detector_monitored_by_nr_of_members =
            setting( "causal_clustering.middleware.akka.failure_detector.monitored_by_nr_of_members", INTEGER, "5" );

    @Internal
    @Description( "Akka cluster phi accrual failure detector. " +
            "After the heartbeat request has been sent the first failure detection " +
            "will start after this period, even though no heartbeat message has been received." )
    public static final Setting<Duration> akka_failure_detector_expected_response_after =
            setting( "causal_clustering.middleware.akka.failure_detector.expected_response_after", DURATION, "1s" );
    /*
        End akka failure detector
     */

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

    @Description( "The catch up protocol times out if the given duration elapses with no network activity. " +
            "Every message received by the client from the server extends the time out duration." )
    public static final Setting<Duration> catch_up_client_inactivity_timeout =
            setting( "causal_clustering.catch_up_client_inactivity_timeout", DURATION, "10m" );

    @Description( "Maximum retry time per request during store copy. Regular store files and indexes are downloaded in separate requests during store copy." +
            " This configures the maximum time failed requests are allowed to resend. " )
    public static final Setting<Duration> store_copy_max_retry_time_per_request =
            setting( "causal_clustering.store_copy_max_retry_time_per_request", DURATION, "20m" );

    @Description( "Maximum backoff timeout for store copy requests" )
    @Internal
    public static final Setting<Duration> store_copy_backoff_max_wait = setting( "causal_clustering.store_copy_backoff_max_wait", DURATION, "5s" );

    @Description( "Throttle limit for logging unknown cluster member address" )
    public static final Setting<Duration> unknown_address_logging_throttle =
            setting( "causal_clustering.unknown_address_logging_throttle", DURATION, "10000ms" );

    @Description( "Maximum transaction batch size for read replicas when applying transactions pulled from core " +
            "servers." )
    @Internal
    public static final Setting<Integer> read_replica_transaction_applier_batch_size =
            setting( "causal_clustering.read_replica_transaction_applier_batch_size", INTEGER, "64" );

    @Description( "Time To Live before read replica is considered unavailable" )
    public static final Setting<Duration> read_replica_time_to_live =
            buildSetting( "causal_clustering.read_replica_time_to_live", DURATION, "1m" ).constraint( min( Duration.ofSeconds( 60 ) ) ).build();

    @Description( "How long drivers should cache the data from the `dbms.cluster.routing.getServers()` procedure." )
    public static final Setting<Duration> cluster_routing_ttl =
            buildSetting( "causal_clustering.cluster_routing_ttl", DURATION, "300s" ).constraint( min( Duration.ofSeconds( 1 ) ) ).build();

    @Description( "Configure if the `dbms.cluster.routing.getServers()` procedure should include followers as read " +
            "endpoints or return only read replicas. Note: if there are no read replicas in the cluster, followers " +
            "are returned as read end points regardless the value of this setting. Defaults to true so that followers " +
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

    @Description( "Time out for protocol negotiation handshake" )
    public static final Setting<Duration> handshake_timeout = setting( "causal_clustering.handshake_timeout", DURATION, "20s" );

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

    @Description( "Raft protocol implementation versions that this instance will allow in negotiation as a comma-separated list." +
            " Order is not relevant: the greatest value will be preferred. An empty list will allow all supported versions" )
    public static final Setting<List<Integer>> raft_implementations =
            setting( "causal_clustering.protocol_implementations.raft", list( ",", INTEGER ), "" );

    @Description( "Catchup protocol implementation versions that this instance will allow in negotiation as a comma-separated list." +
            " Order is not relevant: the greatest value will be preferred. An empty list will allow all supported versions" )
    public static final Setting<List<Integer>> catchup_implementations =
            setting( "causal_clustering.protocol_implementations.catchup", list( ",", INTEGER ), "" );

    @Description( "Network compression algorithms that this instance will allow in negotiation as a comma-separated list." +
            " Listed in descending order of preference for incoming connections. An empty list implies no compression." +
            " For outgoing connections this merely specifies the allowed set of algorithms and the preference of the " +
            " remote peer will be used for making the decision." +
            " Allowable values: [" + GZIP + "," + SNAPPY + "," + SNAPPY_VALIDATING + "," +
            LZ4 + "," + LZ4_HIGH_COMPRESSION + "," + LZ_VALIDATING + "," + LZ4_HIGH_COMPRESSION_VALIDATING + "]" )
    public static final Setting<List<String>> compression_implementations =
            setting( "causal_clustering.protocol_implementations.compression", STRING_LIST, "");
}
