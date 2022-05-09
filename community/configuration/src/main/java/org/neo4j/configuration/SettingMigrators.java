/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.configuration;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.neo4j.configuration.BootloaderSettings.lib_directory;
import static org.neo4j.configuration.BootloaderSettings.run_directory;
import static org.neo4j.configuration.BootloaderSettings.windows_service_name;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_time;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_tx;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_volume;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_iops_limit;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_policy;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.database_dumps_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.licenses_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.load_csv_file_url_root;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_database_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_prefetch_allowlist;
import static org.neo4j.configuration.GraphDatabaseSettings.plugin_dir;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_allowlist;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.GraphDatabaseSettings.script_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_max_off_heap_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_off_heap_block_cache_size;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_off_heap_max_cacheable_block_size;
import static org.neo4j.configuration.connectors.BoltConnectorInternalSettings.thread_pool_shutdown_wait_time;
import static org.neo4j.configuration.connectors.BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_max_inbound_bytes;
import static org.neo4j.configuration.connectors.BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_timeout;
import static org.neo4j.configuration.connectors.BoltConnectorInternalSettings.unsupported_thread_pool_queue_size;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.logging.FormattedLogFormat;
import org.neo4j.logging.InternalLog;

public final class SettingMigrators {

    private SettingMigrators() {}

    @ServiceProvider
    public static class Neo4j5_0SettingMigrator implements SettingMigrator {

        private static final String OLD_PREFIX = "dbms.connector";
        private static final Pattern SUPPORTED_CONNECTOR_PATTERN = Pattern.compile("(.+)\\.(bolt|http|https)\\.(.+)");
        private static final List<String> REMOVED_SETTINGS = List.of(
                "causal_clustering.delete_store_before_store_copy",
                "causal_clustering.multi_dc_license",
                "causal_clustering.store_copy_chunk_size",
                "dbms.allow_upgrade",
                "dbms.record_format",
                "dbms.backup.incremental.strategy",
                "dbms.directories.tx_log",
                "dbms.index.default_schema_provider",
                "dbms.index_searcher_cache_size",
                "dbms.logs.debug.rotation.delay",
                "dbms.logs.user.rotation.delay",
                "dbms.memory.pagecache.swapper",
                "dbms.routing.driver.api",
                "dbms.security.ldap.authentication.use_samaccountname",
                "dbms.security.procedures.default_allowed",
                "dbms.security.procedures.roles",
                "dbms.security.property_level.blacklist",
                "dbms.security.property_level.enabled",
                "fabric.driver.api",
                "unsupported.cypher.parser",
                "unsupported.dbms.block_remote_alias",
                "unsupported.dbms.memory.pagecache.warmup.legacy_profile_loader",
                "unsupported.dbms.recovery.ignore_store_id_validation",
                "unsupported.dbms.tokenscan.log.enabled",
                "unsupported.dbms.tokenscan.log.prune_threshold",
                "unsupported.dbms.tokenscan.log.rotation_threshold",
                "unsupported.dbms.topology_graph.enable");
        private static final Collection<Mapping> LEGACY_UNSUPPORTED_SETTINGS_MAPPING = List.of(
                new Mapping(
                        "causal_clustering.akka_actor_system_restarter.initial_delay",
                        "internal.cluster.akka_actor_system_restarter.initial_delay"),
                new Mapping(
                        "causal_clustering.akka_actor_system_restarter.max_acceptable_failures",
                        "internal.cluster.akka_actor_system_restarter.max_acceptable_failures"),
                new Mapping(
                        "causal_clustering.akka_actor_system_restarter.max_delay",
                        "internal.cluster.akka_actor_system_restarter.max_delay"),
                new Mapping(
                        "causal_clustering.cluster_binding_retry_timeout",
                        "internal.cluster.cluster_binding_retry_timeout"),
                new Mapping(
                        "causal_clustering.cluster_id_publish_timeout", "internal.cluster.cluster_id_publish_timeout"),
                new Mapping(
                        "causal_clustering.cluster_info_polling_max_wait",
                        "internal.cluster.cluster_info_polling_max_wait"),
                new Mapping(
                        "causal_clustering.discovery_resolution_retry_interval",
                        "internal.cluster.discovery_resolution_retry_interval"),
                new Mapping(
                        "causal_clustering.discovery_resolution_timeout",
                        "internal.cluster.discovery_resolution_timeout"),
                new Mapping("causal_clustering.enable_seed_validation", "internal.cluster.enable_seed_validation"),
                new Mapping("causal_clustering.leader_transfer_interval", "internal.cluster.leader_transfer_interval"),
                new Mapping(
                        "causal_clustering.leader_transfer_member_backoff",
                        "internal.cluster.leader_transfer_member_backoff"),
                new Mapping("causal_clustering.leader_transfer_timeout", "internal.cluster.leader_transfer_timeout"),
                new Mapping(
                        "causal_clustering.max_commits_delay_id_reuse", "internal.cluster.max_commits_delay_id_reuse"),
                new Mapping("causal_clustering.max_time_delay_id_reuse", "internal.cluster.max_time_delay_id_reuse"),
                new Mapping(
                        "causal_clustering.middleware.akka.allow_any_core_to_bootstrap",
                        "internal.cluster.middleware.akka.allow_any_core_to_bootstrap"),
                new Mapping(
                        "causal_clustering.middleware.akka.bind_timeout",
                        "internal.cluster.middleware.akka.bind_timeout"),
                new Mapping(
                        "causal_clustering.middleware.akka.cluster.min_nr_of_members",
                        "internal.cluster.middleware.akka.cluster.min_nr_of_members"),
                new Mapping(
                        "causal_clustering.middleware.akka.cluster.seed_node_timeout",
                        "internal.cluster.middleware.akka.cluster.seed_node_timeout"),
                new Mapping(
                        "causal_clustering.middleware.akka.cluster.seed_node_timeout_on_first_start",
                        "internal.cluster.middleware.akka.cluster.seed_node_timeout_on_first_start"),
                new Mapping(
                        "causal_clustering.middleware.akka.connection_timeout",
                        "internal.cluster.middleware.akka.connection_timeout"),
                new Mapping(
                        "causal_clustering.middleware.akka.default_parallelism",
                        "internal.cluster.middleware.akka.default_parallelism"),
                new Mapping(
                        "causal_clustering.middleware.akka.down_unreachable_on_new_joiner",
                        "internal.cluster.middleware.akka.down_unreachable_on_new_joiner"),
                new Mapping(
                        "causal_clustering.middleware.akka.external_config",
                        "internal.cluster.middleware.akka.external_config"),
                new Mapping(
                        "causal_clustering.middleware.akka.failure_detector.acceptable_heartbeat_pause",
                        "internal.cluster.middleware.akka.failure_detector.acceptable_heartbeat_pause"),
                new Mapping(
                        "causal_clustering.middleware.akka.failure_detector.expected_response_after",
                        "internal.cluster.middleware.akka.failure_detector.expected_response_after"),
                new Mapping(
                        "causal_clustering.middleware.akka.failure_detector.heartbeat_interval",
                        "internal.cluster.middleware.akka.failure_detector.heartbeat_interval"),
                new Mapping(
                        "causal_clustering.middleware.akka.failure_detector.max_sample_size",
                        "internal.cluster.middleware.akka.failure_detector.max_sample_size"),
                new Mapping(
                        "causal_clustering.middleware.akka.failure_detector.min_std_deviation",
                        "internal.cluster.middleware.akka.failure_detector.min_std_deviation"),
                new Mapping(
                        "causal_clustering.middleware.akka.failure_detector.monitored_by_nr_of_members",
                        "internal.cluster.middleware.akka.failure_detector.monitored_by_nr_of_members"),
                new Mapping(
                        "causal_clustering.middleware.akka.failure_detector.threshold",
                        "internal.cluster.middleware.akka.failure_detector.threshold"),
                new Mapping(
                        "causal_clustering.middleware.akka.handshake_timeout",
                        "internal.cluster.middleware.akka.handshake_timeout"),
                new Mapping(
                        "causal_clustering.middleware.akka.shutdown_timeout",
                        "internal.cluster.middleware.akka.shutdown_timeout"),
                new Mapping(
                        "causal_clustering.middleware.akka.sink_parallelism",
                        "internal.cluster.middleware.akka.sink_parallelism"),
                new Mapping("causal_clustering.min_time_delay_id_reuse", "internal.cluster.min_time_delay_id_reuse"),
                new Mapping(
                        "causal_clustering.raft_group_graveyard_state_size",
                        "internal.cluster.raft_group_graveyard_state_size"),
                new Mapping("causal_clustering.raft_in_queue_max_batch", "internal.cluster.raft_in_queue_max_batch"),
                new Mapping("causal_clustering.raft_in_queue_size", "internal.cluster.raft_in_queue_size"),
                new Mapping("causal_clustering.raft_messages_log_enable", "internal.cluster.raft_messages_log_enable"),
                new Mapping("causal_clustering.raft_messages_log_path", "internal.cluster.raft_messages_log_path"),
                new Mapping(
                        "causal_clustering.read_replica_transaction_applier_batch_size",
                        "internal.cluster.read_replica_transaction_applier_batch_size"),
                new Mapping(
                        "causal_clustering.read_replica_transaction_applier_max_queue_size",
                        "internal.cluster.read_replica_transaction_applier_max_queue_size"),
                new Mapping("causal_clustering.seed_validation_timeout", "internal.cluster.seed_validation_timeout"),
                new Mapping(
                        "causal_clustering.store_copy_backoff_max_wait",
                        "internal.cluster.store_copy_backoff_max_wait"),
                new Mapping(
                        "causal_clustering.store_size_service_cache_timeout",
                        "internal.cluster.store_size_service_cache_timeout"),
                new Mapping(
                        "causal_clustering.temporary_database.extension_package_names",
                        "internal.cluster.temporary_database.extension_package_names"),
                new Mapping(
                        "causal_clustering.topology_graph.default_num_primaries",
                        "internal.cluster.topology_graph.default_num_primaries"),
                new Mapping(
                        "causal_clustering.topology_graph.default_num_secondaries",
                        "internal.cluster.topology_graph.default_num_secondaries"),
                new Mapping("dbms.capabilities.blocked", "internal.dbms.capabilities.blocked"),
                new Mapping("dbms.connector.bolt.tcp_keep_alive", "internal.server.bolt.tcp_keep_alive"),
                new Mapping("dbms.init_file", "internal.dbms.init_file"),
                new Mapping("dbms.log_inconsistent_data_deletion", "internal.dbms.log_inconsistent_data_deletion"),
                new Mapping("dbms.routing.driver.event_loop_count", "internal.dbms.routing.driver.event_loop_count"),
                new Mapping(
                        "dbms.routing.driver.idle_check_interval", "internal.dbms.routing.driver.idle_check_interval"),
                new Mapping(
                        "dbms.routing.driver.logging.leaked_sessions",
                        "internal.dbms.routing.driver.logging.leaked_sessions"),
                new Mapping("dbms.routing.driver.timeout", "internal.dbms.routing.driver.timeout"),
                new Mapping("fabric.driver.event_loop_count", "internal.fabric.driver.event_loop_count"),
                new Mapping("fabric.driver.idle_check_interval", "internal.fabric.driver.idle_check_interval"),
                new Mapping("fabric.driver.logging.leaked_sessions", "internal.fabric.driver.logging.leaked_sessions"),
                new Mapping("fabric.driver.timeout", "internal.fabric.driver.timeout"),
                new Mapping("fabric.enabled_by_default", "internal.fabric.enabled_by_default"),
                new Mapping("fabric.stream.batch_size", "internal.fabric.stream.batch_size"),
                new Mapping("unsupported.bootloader.auto.configuration", "internal.bootloader.auto.configuration"),
                new Mapping(
                        "unsupported.bootloader.auto.configuration.codecache.max",
                        "internal.bootloader.auto.configuration.codecache.max"),
                new Mapping(
                        "unsupported.bootloader.auto.configuration.codecache.min",
                        "internal.bootloader.auto.configuration.codecache.min"),
                new Mapping(
                        "unsupported.bootloader.auto.configuration.dataset.file",
                        "internal.bootloader.auto.configuration.dataset.file"),
                new Mapping(
                        "unsupported.bootloader.auto.configuration.execution_plan_cache.max",
                        "internal.bootloader.auto.configuration.execution_plan_cache.max"),
                new Mapping(
                        "unsupported.bootloader.auto.configuration.execution_plan_cache.min",
                        "internal.bootloader.auto.configuration.execution_plan_cache.min"),
                new Mapping(
                        "unsupported.bootloader.auto.configuration.heap.max",
                        "internal.bootloader.auto.configuration.heap.max"),
                new Mapping(
                        "unsupported.bootloader.auto.configuration.heap.min",
                        "internal.bootloader.auto.configuration.heap.min"),
                new Mapping(
                        "unsupported.bootloader.auto.configuration.memory.limit",
                        "internal.bootloader.auto.configuration.memory.limit"),
                new Mapping(
                        "unsupported.bootloader.auto.configuration.pagecache.max",
                        "internal.bootloader.auto.configuration.pagecache.max"),
                new Mapping(
                        "unsupported.bootloader.auto.configuration.pagecache.min",
                        "internal.bootloader.auto.configuration.pagecache.min"),
                new Mapping(
                        "unsupported.bootloader.auto.configuration.query_cache.max",
                        "internal.bootloader.auto.configuration.query_cache.max"),
                new Mapping(
                        "unsupported.bootloader.auto.configuration.query_cache.min",
                        "internal.bootloader.auto.configuration.query_cache.min"),
                new Mapping(
                        "unsupported.bootloader.auto.configuration.search.radius",
                        "internal.bootloader.auto.configuration.search.radius"),
                new Mapping(
                        "unsupported.bootloader.auto.configuration.tx_limit.max",
                        "internal.bootloader.auto.configuration.tx_limit.max"),
                new Mapping(
                        "unsupported.bootloader.auto.configuration.tx_limit.min",
                        "internal.bootloader.auto.configuration.tx_limit.min"),
                new Mapping(
                        "unsupported.causal_clustering.cluster_status_request_maximum_wait",
                        "internal.cluster.cluster_status_request_maximum_wait"),
                new Mapping(
                        "unsupported.causal_clustering.experimental_catchup_protocol_enabled",
                        "internal.cluster.experimental_catchup_protocol_enabled"),
                new Mapping(
                        "unsupported.causal_clustering.experimental_consensus_protocol_enabled",
                        "internal.cluster.experimental_consensus_protocol_enabled"),
                new Mapping(
                        "unsupported.causal_clustering.experimental_dbms_protocol_enabled",
                        "internal.cluster.experimental_dbms_protocol_enabled"),
                new Mapping(
                        "unsupported.causal_clustering.experimental_raft_protocol_enabled",
                        "internal.cluster.experimental_raft_protocol_enabled"),
                new Mapping(
                        "unsupported.causal_clustering.inbound_connection_initialization_logging_enabled",
                        "internal.cluster.inbound_connection_initialization_logging_enabled"),
                new Mapping(
                        "unsupported.consistency_checker.fail_fast_threshold",
                        "internal.consistency_checker.fail_fast_threshold"),
                new Mapping(
                        "unsupported.consistency_checker.memory_limit_factor",
                        "internal.consistency_checker.memory_limit_factor"),
                new Mapping("unsupported.cypher.compiler_tracing", "internal.cypher.compiler_tracing"),
                new Mapping(
                        "unsupported.cypher.enable_extra_semantic_features",
                        "internal.cypher.enable_extra_semantic_features"),
                new Mapping("unsupported.cypher.enable_runtime_monitors", "internal.cypher.enable_runtime_monitors"),
                new Mapping("unsupported.cypher.expression_engine", "internal.cypher.expression_engine"),
                new Mapping(
                        "unsupported.cypher.expression_recompilation_limit",
                        "internal.cypher.expression_recompilation_limit"),
                new Mapping(
                        "unsupported.cypher.idp_solver_duration_threshold",
                        "internal.cypher.idp_solver_duration_threshold"),
                new Mapping(
                        "unsupported.cypher.idp_solver_table_threshold", "internal.cypher.idp_solver_table_threshold"),
                new Mapping(
                        "unsupported.cypher.non_indexed_label_warning_threshold",
                        "internal.cypher.non_indexed_label_warning_threshold"),
                new Mapping("unsupported.cypher.number_of_workers", "internal.cypher.number_of_workers"),
                new Mapping("unsupported.cypher.pipelined.batch_size_big", "internal.cypher.pipelined.batch_size_big"),
                new Mapping(
                        "unsupported.cypher.pipelined.batch_size_small", "internal.cypher.pipelined.batch_size_small"),
                new Mapping(
                        "unsupported.cypher.pipelined.enable_runtime_trace",
                        "internal.cypher.pipelined.enable_runtime_trace"),
                new Mapping(
                        "unsupported.cypher.pipelined.operator_engine", "internal.cypher.pipelined.operator_engine"),
                new Mapping(
                        "unsupported.cypher.pipelined.operator_fusion_over_pipeline_limit",
                        "internal.cypher.pipelined.operator_fusion_over_pipeline_limit"),
                new Mapping(
                        "unsupported.cypher.pipelined.runtime_trace_path",
                        "internal.cypher.pipelined.runtime_trace_path"),
                new Mapping(
                        "unsupported.cypher.pipelined_interpreted_pipes_fallback",
                        "internal.cypher.pipelined_interpreted_pipes_fallback"),
                new Mapping(
                        "unsupported.cypher.planning_point_indexes_enabled",
                        "internal.cypher.planning_point_indexes_enabled"),
                new Mapping(
                        "unsupported.cypher.planning_range_indexes_enabled",
                        "internal.cypher.planning_range_indexes_enabled"),
                new Mapping(
                        "unsupported.cypher.planning_text_indexes_enabled",
                        "internal.cypher.planning_text_indexes_enabled"),
                new Mapping("unsupported.cypher.replan_algorithm", "internal.cypher.replan_algorithm"),
                new Mapping("unsupported.cypher.runtime", "internal.cypher.runtime"),
                new Mapping("unsupported.cypher.splitting_top_behavior", "internal.cypher.splitting_top_behavior"),
                new Mapping(
                        "unsupported.cypher.statistics_divergence_target",
                        "internal.cypher.statistics_divergence_target"),
                new Mapping("unsupported.cypher.target_replan_interval", "internal.cypher.target_replan_interval"),
                new Mapping(
                        "unsupported.cypher.var_expand_relationship_id_set_threshold",
                        "internal.cypher.var_expand_relationship_id_set_threshold"),
                new Mapping(
                        "unsupported.datacollector.max_query_text_size", "internal.datacollector.max_query_text_size"),
                new Mapping(
                        "unsupported.datacollector.max_recent_query_count",
                        "internal.datacollector.max_recent_query_count"),
                new Mapping("unsupported.dbms.block_alter_database", "internal.dbms.block_alter_database"),
                new Mapping("unsupported.dbms.block_create_drop_database", "internal.dbms.block_create_drop_database"),
                new Mapping(
                        "unsupported.dbms.block_size.array_properties", "internal.dbms.block_size.array_properties"),
                new Mapping("unsupported.dbms.block_size.labels", "internal.dbms.block_size.labels"),
                new Mapping("unsupported.dbms.block_size.strings", "internal.dbms.block_size.strings"),
                new Mapping("unsupported.dbms.block_start_stop_database", "internal.dbms.block_start_stop_database"),
                new Mapping(
                        "unsupported.dbms.bolt.inbound_message_throttle.high_watermark",
                        "internal.dbms.bolt.inbound_message_throttle.high_watermark"),
                new Mapping(
                        "unsupported.dbms.bolt.inbound_message_throttle.low_watermark",
                        "internal.dbms.bolt.inbound_message_throttle.low_watermark"),
                new Mapping(
                        "unsupported.dbms.bolt.netty_message_merge_cumulator",
                        "internal.dbms.bolt.netty_message_merge_cumulator"),
                new Mapping(
                        "unsupported.dbms.bolt.netty_server_shutdown_quiet_period",
                        "internal.dbms.bolt.netty_server_shutdown_quiet_period"),
                new Mapping(
                        "unsupported.dbms.bolt.netty_server_shutdown_timeout",
                        "internal.dbms.bolt.netty_server_shutdown_timeout"),
                new Mapping(
                        "unsupported.dbms.bolt.netty_server_use_epoll", "internal.dbms.bolt.netty_server_use_epoll"),
                new Mapping(
                        "unsupported.dbms.bolt.outbound_buffer_throttle",
                        "internal.dbms.bolt.outbound_buffer_throttle"),
                new Mapping(
                        "unsupported.dbms.bolt.outbound_buffer_throttle.high_watermark",
                        "internal.dbms.bolt.outbound_buffer_throttle.high_watermark"),
                new Mapping(
                        "unsupported.dbms.bolt.outbound_buffer_throttle.low_watermark",
                        "internal.dbms.bolt.outbound_buffer_throttle.low_watermark"),
                new Mapping(
                        "unsupported.dbms.bolt.outbound_buffer_throttle.max_duration",
                        "internal.dbms.bolt.outbound_buffer_throttle.max_duration"),
                new Mapping(
                        "unsupported.dbms.checkpoint_log.rotation.keep.files",
                        "internal.db.checkpoint_log.rotation.keep.files"),
                new Mapping(
                        "unsupported.dbms.checkpoint_log.rotation.size", "internal.db.checkpoint_log.rotation.size"),
                new Mapping(
                        "unsupported.dbms.config.command_evaluation_timeout",
                        "internal.dbms.config.command_evaluation_timeout"),
                new Mapping(
                        "unsupported.dbms.counts_store_rotation_timeout",
                        "internal.dbms.counts_store_rotation_timeout"),
                new Mapping("unsupported.dbms.cypher_ip_blocklist", "internal.dbms.cypher_ip_blocklist"),
                new Mapping(
                        "unsupported.dbms.debug.page_cache_tracer_speed_reporting_threshold",
                        "internal.dbms.debug.page_cache_tracer_speed_reporting_threshold"),
                new Mapping(
                        "unsupported.dbms.debug.print_page_buffer_allocation_trace",
                        "internal.dbms.debug.print_page_buffer_allocation_trace"),
                new Mapping("unsupported.dbms.debug.trace_cursors", "internal.dbms.debug.trace_cursors"),
                new Mapping("unsupported.dbms.debug.trace_tx_statement", "internal.dbms.debug.trace_tx_statement"),
                new Mapping("unsupported.dbms.debug.track_cursor_close", "internal.dbms.debug.track_cursor_close"),
                new Mapping(
                        "unsupported.dbms.debug.track_tx_statement_close",
                        "internal.dbms.debug.track_tx_statement_close"),
                new Mapping("unsupported.dbms.directories.auth", "internal.server.directories.auth"),
                new Mapping(
                        "unsupported.dbms.directories.databases.root", "internal.server.directories.databases.root"),
                new Mapping("unsupported.dbms.directories.pid_file", "internal.server.directories.pid_file"),
                new Mapping("unsupported.dbms.directories.scripts", "internal.server.directories.scripts"),
                new Mapping("unsupported.dbms.directories.windows_tools", "internal.server.directories.windows_tools"),
                new Mapping("unsupported.dbms.discoverable_bolt_address", "internal.dbms.discoverable_bolt_address"),
                new Mapping(
                        "unsupported.dbms.discoverable_bolt_routing_address",
                        "internal.dbms.discoverable_bolt_routing_address"),
                new Mapping("unsupported.dbms.dump_diagnostics", "internal.dbms.dump_diagnostics"),
                new Mapping(
                        "unsupported.dbms.enable_transaction_heap_allocation_tracking",
                        "internal.dbms.enable_transaction_heap_allocation_tracking"),
                new Mapping("unsupported.dbms.executiontime_limit.time", "internal.dbms.executiontime_limit.time"),
                new Mapping("unsupported.dbms.extra_lock_verification", "internal.dbms.extra_lock_verification"),
                new Mapping("unsupported.dbms.force_small_id_cache", "internal.dbms.force_small_id_cache"),
                new Mapping("unsupported.dbms.http_paths_blacklist", "internal.dbms.http_paths_blacklist"),
                new Mapping(
                        "unsupported.dbms.id_buffering.offload_to_disk", "internal.dbms.id_buffering.offload_to_disk"),
                new Mapping("unsupported.dbms.idgenerator.log.enabled", "internal.dbms.idgenerator.log.enabled"),
                new Mapping(
                        "unsupported.dbms.idgenerator.log.prune_threshold",
                        "internal.dbms.idgenerator.log.prune_threshold"),
                new Mapping(
                        "unsupported.dbms.idgenerator.log.rotation_threshold",
                        "internal.dbms.idgenerator.log.rotation_threshold"),
                new Mapping(
                        "unsupported.dbms.include_dev_record_format_versions",
                        "internal.dbms.include_dev_record_format_versions"),
                new Mapping("unsupported.dbms.index.archive_failed", "internal.dbms.index.archive_failed"),
                new Mapping(
                        "unsupported.dbms.index.default_fulltext_provider",
                        "internal.dbms.index.default_fulltext_provider"),
                new Mapping("unsupported.dbms.index.lucene.merge_factor", "internal.dbms.index.lucene.merge_factor"),
                new Mapping("unsupported.dbms.index.lucene.min_merge", "internal.dbms.index.lucene.min_merge"),
                new Mapping("unsupported.dbms.index.lucene.nocfs.ratio", "internal.dbms.index.lucene.nocfs.ratio"),
                new Mapping(
                        "unsupported.dbms.index.lucene.population_max_buffered_docs",
                        "internal.dbms.index.lucene.population_max_buffered_docs"),
                new Mapping(
                        "unsupported.dbms.index.lucene.population_ram_buffer_size",
                        "internal.dbms.index.lucene.population_ram_buffer_size"),
                new Mapping(
                        "unsupported.dbms.index.lucene.population_serial_merge_scheduler",
                        "internal.dbms.index.lucene.population_serial_merge_scheduler"),
                new Mapping(
                        "unsupported.dbms.index.lucene.standard_ram_buffer_size",
                        "internal.dbms.index.lucene.standard_ram_buffer_size"),
                new Mapping(
                        "unsupported.dbms.index.lucene.writer_max_buffered_docs",
                        "internal.dbms.index.lucene.writer_max_buffered_docs"),
                new Mapping(
                        "unsupported.dbms.index.population_batch_max_byte_size",
                        "internal.dbms.index.population_batch_max_byte_size"),
                new Mapping(
                        "unsupported.dbms.index.population_print_debug", "internal.dbms.index.population_print_debug"),
                new Mapping(
                        "unsupported.dbms.index.population_queue_threshold",
                        "internal.dbms.index.population_queue_threshold"),
                new Mapping("unsupported.dbms.index.populator_block_size", "internal.dbms.index.populator_block_size"),
                new Mapping(
                        "unsupported.dbms.index.populator_merge_factor", "internal.dbms.index.populator_merge_factor"),
                new Mapping(
                        "unsupported.dbms.index.sampling.async_recovery",
                        "internal.dbms.index.sampling.async_recovery"),
                new Mapping(
                        "unsupported.dbms.index.sampling.async_recovery_wait",
                        "internal.dbms.index.sampling.async_recovery_wait"),
                new Mapping(
                        "unsupported.dbms.index.sampling.log_recovered_samples",
                        "internal.dbms.index.sampling.log_recovered_samples"),
                new Mapping(
                        "unsupported.dbms.index.skip_default_indexes_on_creation",
                        "internal.dbms.index.skip_default_indexes_on_creation"),
                new Mapping(
                        "unsupported.dbms.index.spatial.curve.bottom_threshold",
                        "internal.dbms.index.spatial.curve.bottom_threshold"),
                new Mapping(
                        "unsupported.dbms.index.spatial.curve.extra_levels",
                        "internal.dbms.index.spatial.curve.extra_levels"),
                new Mapping(
                        "unsupported.dbms.index.spatial.curve.top_threshold",
                        "internal.dbms.index.spatial.curve.top_threshold"),
                new Mapping(
                        "unsupported.dbms.index_population.parallelism", "internal.dbms.index_population.parallelism"),
                new Mapping("unsupported.dbms.index_population.workers", "internal.dbms.index_population.workers"),
                new Mapping("unsupported.dbms.index_sampling.parallelism", "internal.dbms.index_sampling.parallelism"),
                new Mapping(
                        "unsupported.dbms.initial_transaction_heap_grab_size",
                        "internal.dbms.initial_transaction_heap_grab_size"),
                new Mapping(
                        "unsupported.dbms.io.controller.consider.external.enabled",
                        "internal.dbms.io.controller.consider.external.enabled"),
                new Mapping("unsupported.dbms.kernel_id", "internal.dbms.kernel_id"),
                new Mapping("unsupported.dbms.large_cluster.enable", "internal.dbms.large_cluster.enable"),
                new Mapping("unsupported.dbms.lock_manager", "internal.dbms.lock_manager"),
                new Mapping(
                        "unsupported.dbms.lock_manager.verbose_deadlocks",
                        "internal.dbms.lock_manager.verbose_deadlocks"),
                new Mapping(
                        "unsupported.dbms.logs.query.heap_dump_enabled", "internal.dbms.logs.query.heap_dump_enabled"),
                new Mapping("unsupported.dbms.loopback_delete", "internal.dbms.loopback_delete"),
                new Mapping("unsupported.dbms.loopback_enabled", "internal.dbms.loopback_enabled"),
                new Mapping("unsupported.dbms.loopback_file", "internal.dbms.loopback_file"),
                new Mapping("unsupported.dbms.lucene.ephemeral", "internal.dbms.lucene.ephemeral"),
                new Mapping("unsupported.dbms.lucene.max_partition_size", "internal.dbms.lucene.max_partition_size"),
                new Mapping(
                        "unsupported.dbms.max_http_request_header_size", "internal.dbms.max_http_request_header_size"),
                new Mapping(
                        "unsupported.dbms.max_http_response_header_size",
                        "internal.dbms.max_http_response_header_size"),
                new Mapping(
                        "unsupported.dbms.memory.counts_store_max_cached_entries",
                        "internal.dbms.memory.counts_store_max_cached_entries"),
                new Mapping(
                        "unsupported.dbms.memory.managed_network_buffers",
                        "internal.dbms.memory.managed_network_buffers"),
                new Mapping("unsupported.dbms.multiversioned.store", "internal.dbms.multiversioned.store"),
                new Mapping("unsupported.dbms.page.file.tracer", "internal.dbms.page.file.tracer"),
                new Mapping(
                        "unsupported.dbms.parallel_index_updates_apply", "internal.dbms.parallel_index_updates_apply"),
                new Mapping("unsupported.dbms.query.snapshot", "internal.dbms.query.snapshot"),
                new Mapping("unsupported.dbms.query.snapshot.retries", "internal.dbms.query.snapshot.retries"),
                new Mapping(
                        "unsupported.dbms.query_execution_plan_cache_size",
                        "internal.dbms.query_execution_plan_cache_size"),
                new Mapping("unsupported.dbms.readonly.failover", "internal.dbms.readonly.failover"),
                new Mapping(
                        "unsupported.dbms.recovery.enable_parallelism", "internal.dbms.recovery.enable_parallelism"),
                new Mapping("unsupported.dbms.report_configuration", "internal.dbms.report_configuration"),
                new Mapping("unsupported.dbms.reserved.page.header.bytes", "internal.dbms.reserved.page.header.bytes"),
                new Mapping(
                        "unsupported.dbms.security.ldap.authorization.connection_pooling",
                        "internal.dbms.security.ldap.authorization.connection_pooling"),
                new Mapping(
                        "unsupported.dbms.select_specfic_record_format", "internal.dbms.select_specfic_record_format"),
                new Mapping(
                        "unsupported.dbms.ssl.system.ignore_dot_files", "internal.dbms.ssl.system.ignore_dot_files"),
                new Mapping(
                        "unsupported.dbms.storage.consistency_check_on_apply",
                        "internal.dbms.storage.consistency_check_on_apply"),
                new Mapping("unsupported.dbms.storage_engine", "internal.dbms.storage_engine"),
                new Mapping(
                        "unsupported.dbms.strictly_prioritize_id_freelist",
                        "internal.dbms.strictly_prioritize_id_freelist"),
                new Mapping(
                        "unsupported.dbms.topology_graph_updater.enable",
                        "internal.dbms.topology_graph_updater.enable"),
                new Mapping("unsupported.dbms.tracer", "internal.dbms.tracer"),
                new Mapping("unsupported.dbms.transaction_start_timeout", "internal.dbms.transaction_start_timeout"),
                new Mapping("unsupported.dbms.tx.logs.dedicated.appender", "internal.dbms.tx.logs.dedicated.appender"),
                new Mapping(
                        "unsupported.dbms.tx_log.fail_on_corrupted_log_files",
                        "internal.dbms.tx_log.fail_on_corrupted_log_files"),
                new Mapping("unsupported.dbms.tx_log.presketch", "internal.dbms.tx_log.presketch"),
                new Mapping(
                        "unsupported.dbms.upgrade_restriction_enabled", "internal.dbms.upgrade_restriction_enabled"),
                new Mapping("unsupported.dbms.uris.browser", "internal.dbms.uris.browser"),
                new Mapping("unsupported.dbms.uris.db", "internal.dbms.uris.db"),
                new Mapping("unsupported.dbms.uris.dbms", "internal.dbms.uris.dbms"),
                new Mapping("unsupported.dbms.uris.management", "internal.dbms.uris.management"),
                new Mapping("unsupported.dbms.uris.rest", "internal.dbms.uris.rest"),
                new Mapping(
                        "unsupported.dbms.use_old_token_index_location", "internal.dbms.use_old_token_index_location"),
                new Mapping("unsupported.dbms.wadl_generation_enabled", "internal.dbms.wadl_generation_enabled"),
                new Mapping(
                        "unsupported.metrics.cypher.cache.entries.enabled",
                        "internal.metrics.cypher.cache.entries.enabled"),
                new Mapping("unsupported.tools.batch_inserter.batch_size", "internal.tools.batch_inserter.batch_size"),
                new Mapping(
                        "unsupported.vm_pause_monitor.measurement_duration",
                        "internal.vm_pause_monitor.measurement_duration"),
                new Mapping(
                        "unsupported.vm_pause_monitor.stall_alert_threshold",
                        "internal.vm_pause_monitor.stall_alert_threshold"),
                new Mapping("dbms.config.strict_validation", GraphDatabaseSettings.strict_config_validation.name()));

        @Override
        public void migrate(Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            cleanupRemovedSettings(values, defaultValues, log);

            migrateUnsupportedSettingsToInternal(values, defaultValues, log);

            migrateDirectoriesChanges(values, defaultValues, log);
            migrateLogFormat(values, defaultValues, log);
            migrateConnectors(values, defaultValues, log);
            migrateDatabaseMemorySettings(values, defaultValues, log);
            migrateWhitelistSettings(values, defaultValues, log);
            migrateWindowsServiceName(values, defaultValues, log);
            migrateGroupSpatialSettings(values, defaultValues, log);
            migrateCheckpointSettings(values, defaultValues, log);
            migrateKeepAliveSetting(values, defaultValues, log);
            migrateRefuseToBeALeader(values, defaultValues, log);
            migrateReadOnlySetting(values, defaultValues, log);
            migrateDatabaseMaxSize(values, defaultValues, log);
        }

        private static void migrateUnsupportedSettingsToInternal(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            LEGACY_UNSUPPORTED_SETTINGS_MAPPING.forEach(mapping ->
                    migrateSettingNameChange(values, log, mapping.oldSettingName(), mapping.newSettingName()));
        }

        private record Mapping(String oldSettingName, String newSettingName) {}

        private static void cleanupRemovedSettings(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            REMOVED_SETTINGS.forEach(
                    setting -> migrateSettingRemoval(values, log, setting, "It no longer has any effect"));
        }

        private static void migrateDirectoriesChanges(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.directories.neo4j_home", neo4j_home);
            migrateSettingNameChange(values, log, "dbms.directories.data", data_directory);
            migrateSettingNameChange(values, log, "dbms.directories.transaction.logs.root", transaction_logs_root_path);
            migrateSettingNameChange(values, log, "dbms.directories.script.root", script_root_path);
            migrateSettingNameChange(values, log, "dbms.directories.dumps.root", database_dumps_root_path);
            migrateSettingNameChange(values, log, "dbms.directories.import", load_csv_file_url_root);
            migrateSettingNameChange(values, log, "dbms.directories.plugins", plugin_dir);
            migrateSettingNameChange(values, log, "dbms.directories.logs", logs_directory);
            migrateSettingNameChange(values, log, "dbms.directories.licenses", licenses_directory);

            migrateSettingNameChange(values, log, "dbms.directories.run", run_directory);
            migrateSettingNameChange(values, log, "dbms.directories.lib", lib_directory);
        }

        private static void migrateLogFormat(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            String value = values.remove("unsupported.dbms.logs.format");
            if (isNotBlank(value)) {
                log.warn("Use of deprecated setting 'unsupported.dbms.logs.format'.");
                if ("STANDARD_FORMAT".equals(value)) {
                    values.put(GraphDatabaseSettings.default_log_format.name(), FormattedLogFormat.PLAIN.name());
                } else if ("JSON_FORMAT".equals(value)) {
                    values.put(GraphDatabaseSettings.default_log_format.name(), FormattedLogFormat.JSON.name());
                } else {
                    log.warn(
                            "Unrecognized value for 'unsupported.dbms.logs.format'. Was %s but expected STANDARD_FORMAT or JSON_FORMAT.",
                            value);
                }
            }
        }

        private static void migrateConnectors(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            List<String> connectorSettings = values.keySet().stream()
                    .filter(key -> key.startsWith(OLD_PREFIX))
                    .filter(key -> SUPPORTED_CONNECTOR_PATTERN.matcher(key).matches())
                    .toList();
            for (String connectorSetting : connectorSettings) {
                if (connectorSetting.endsWith(".type")) {
                    values.remove(connectorSetting);
                } else if (connectorSetting.equals("dbms.connector.bolt.unsupported_thread_pool_shutdown_wait_time")) {
                    migrateSettingNameChange(values, log, connectorSetting, thread_pool_shutdown_wait_time);
                } else if (connectorSetting.equals("dbms.connector.bolt.unsupported_thread_pool_queue_size")) {
                    migrateSettingNameChange(values, log, connectorSetting, unsupported_thread_pool_queue_size);
                } else if (connectorSetting.equals("dbms.connector.bolt.unsupported_unauth_connection_timeout")) {
                    migrateSettingNameChange(values, log, connectorSetting, unsupported_bolt_unauth_connection_timeout);
                } else if (connectorSetting.equals("dbms.connector.bolt.unsupported_unauth_max_inbound_bytes")) {
                    migrateSettingNameChange(
                            values, log, connectorSetting, unsupported_bolt_unauth_connection_max_inbound_bytes);
                } else {
                    var newName = connectorSetting.replace("dbms.connector", "server");
                    migrateSettingNameChange(values, log, connectorSetting, newName);
                }
            }
        }

        private static void migrateDatabaseMemorySettings(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.tx_state.max_off_heap_memory", tx_state_max_off_heap_memory);
            migrateSettingNameChange(
                    values,
                    log,
                    "dbms.tx_state.off_heap.max_cacheable_block_size",
                    tx_state_off_heap_max_cacheable_block_size);
            migrateSettingNameChange(
                    values, log, "dbms.tx_state.off_heap.block_cache_size", tx_state_off_heap_block_cache_size);

            // Migrate cypher.query_max_allocations to new setting, if new settings is not configured
            String maxAllocations = values.remove("cypher.query_max_allocations");
            if (isNotBlank(maxAllocations)) {
                if (!values.containsKey(memory_transaction_max_size.name())) {
                    log.warn(
                            "The setting cypher.query_max_allocations is removed and replaced by %s.",
                            memory_transaction_max_size.name());
                    values.put(memory_transaction_max_size.name(), maxAllocations);
                } else {
                    log.warn(
                            "The setting cypher.query_max_allocations is removed and replaced by %s. Since both are set, %s will take "
                                    + "precedence and the value of cypher.query_max_allocations, %s, will be ignored.",
                            memory_transaction_max_size.name(), memory_transaction_max_size.name(), maxAllocations);
                }
            }
        }

        private static void migrateWhitelistSettings(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(
                    values, log, "dbms.memory.pagecache.warmup.preload.whitelist", pagecache_warmup_prefetch_allowlist);
            migrateSettingNameChange(values, log, "dbms.security.procedures.whitelist", procedure_allowlist);
        }

        private static void migrateWindowsServiceName(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.windows_service_name", windows_service_name);
        }

        private static void migrateGroupSpatialSettings(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateGroupSettingPrefixChange(
                    values, log, "unsupported.dbms.db.spatial.crs", "internal.dbms.db.spatial.crs");
        }

        private static void migrateCheckpointSettings(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.checkpoint", check_point_policy);
            migrateSettingNameChange(values, log, "dbms.checkpoint.interval.time", check_point_interval_time);
            migrateSettingNameChange(values, log, "dbms.checkpoint.interval.tx", check_point_interval_tx);
            migrateSettingNameChange(values, log, "dbms.checkpoint.interval.volume", check_point_interval_volume);
            migrateSettingNameChange(values, log, "dbms.checkpoint.iops.limit", check_point_iops_limit);
        }

        private static void migrateKeepAliveSetting(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(
                    values,
                    log,
                    "dbms.connector.bolt.connection_keep_alive_scheduling_interval",
                    BoltConnector.connection_keep_alive_streaming_scheduling_interval);
        }

        private static void migrateRefuseToBeALeader(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            final var refuseToBeLeader = "causal_clustering.refuse_to_be_leader";
            final var refuseToBeLeaderValue = values.get(refuseToBeLeader);
            if (isNotBlank(refuseToBeLeaderValue)) {
                log.warn(
                        "The setting '" + refuseToBeLeader + "' is deprecated. Use please '%s' as a replacement",
                        read_only_database_default.name());
            }
            migrateSettingNameChange(values, log, refuseToBeLeader, read_only_database_default);
        }

        private static void migrateReadOnlySetting(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.read_only", read_only_database_default);
        }

        private static void migrateDatabaseMaxSize(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(
                    values, log, "dbms.memory.transaction.datababase_max_size", memory_transaction_database_max_size);
        }
    }

    public static void migrateGroupSettingPrefixChange(
            Map<String, String> values, InternalLog log, String oldGroupSettingPrefix, String newGroupSettingPrefix) {
        List<String> toUpdate = values.keySet().stream()
                .filter(s -> s.startsWith(oldGroupSettingPrefix) && !s.equals(oldGroupSettingPrefix))
                .toList();
        for (String oldSetting : toUpdate) {
            String newSettingName = oldSetting.replace(oldGroupSettingPrefix, newGroupSettingPrefix);
            log.warn("Use of deprecated setting '%s'. It is replaced by '%s'.", oldSetting, newSettingName);
            String oldValue = values.remove(oldSetting);
            values.put(newSettingName, oldValue);
        }
    }

    public static void migrateSettingNameChange(
            Map<String, String> values, InternalLog log, String oldSetting, Setting<?> newSetting) {
        migrateSettingNameChange(values, log, oldSetting, newSetting.name());
    }

    public static void migrateSettingNameChange(
            Map<String, String> values, InternalLog log, String oldSettingName, String newSettingName) {
        String value = values.remove(oldSettingName);
        if (isNotBlank(value)) {
            log.warn("Use of deprecated setting '%s'. It is replaced by '%s'.", oldSettingName, newSettingName);
            values.putIfAbsent(newSettingName, value);
        }
    }

    public static void migrateSettingRemoval(
            Map<String, String> values, InternalLog log, String name, String additionalDescription) {
        if (values.containsKey(name)) {
            log.warn("Setting '%s' is removed. %s.", name, additionalDescription);
            values.remove(name);
        }
    }
}
