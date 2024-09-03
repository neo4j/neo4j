/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.configuration;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.neo4j.configuration.BootloaderSettings.additional_jvm;
import static org.neo4j.configuration.BootloaderSettings.gc_logging_enabled;
import static org.neo4j.configuration.BootloaderSettings.gc_logging_options;
import static org.neo4j.configuration.BootloaderSettings.gc_logging_rotation_keep_number;
import static org.neo4j.configuration.BootloaderSettings.gc_logging_rotation_size;
import static org.neo4j.configuration.BootloaderSettings.initial_heap_size;
import static org.neo4j.configuration.BootloaderSettings.lib_directory;
import static org.neo4j.configuration.BootloaderSettings.max_heap_size;
import static org.neo4j.configuration.BootloaderSettings.run_directory;
import static org.neo4j.configuration.BootloaderSettings.windows_service_name;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.automatic_upgrade_enabled;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.upgrade_processors;
import static org.neo4j.configuration.GraphDatabaseSettings.bookmark_ready_timeout;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_time;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_tx;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_volume;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_iops_limit;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_policy;
import static org.neo4j.configuration.GraphDatabaseSettings.csv_buffer_size;
import static org.neo4j.configuration.GraphDatabaseSettings.csv_legacy_quote_escaping;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_hints_error;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_lenient_create_relationship;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_min_replan_interval;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_planner;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_render_plan_descriptions;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.database_dumps_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.default_advertised_address;
import static org.neo4j.configuration.GraphDatabaseSettings.default_listen_address;
import static org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.fail_on_missing_files;
import static org.neo4j.configuration.GraphDatabaseSettings.filewatcher_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.forbid_exhaustive_shortestpath;
import static org.neo4j.configuration.GraphDatabaseSettings.forbid_shortestpath_common_nodes;
import static org.neo4j.configuration.GraphDatabaseSettings.index_background_sampling_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.index_sample_size_limit;
import static org.neo4j.configuration.GraphDatabaseSettings.index_sampling_update_percentage;
import static org.neo4j.configuration.GraphDatabaseSettings.initial_default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.configuration.GraphDatabaseSettings.licenses_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.load_csv_file_url_root;
import static org.neo4j.configuration.GraphDatabaseSettings.lock_acquisition_timeout;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_annotation_data_format;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_early_raw_logging_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_obfuscate_literals;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_parameter_logging_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_query_plan;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_transaction_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_transactions_level;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.max_concurrent_transactions;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_database_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_global_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_buffered_flush_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_direct_io;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_flush_buffer_size_in_pages;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_scan_prefetch;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_prefetch;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_prefetch_allowlist;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_profiling_interval;
import static org.neo4j.configuration.GraphDatabaseSettings.plugin_dir;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_store_files;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_allowlist;
import static org.neo4j.configuration.GraphDatabaseSettings.query_cache_size;
import static org.neo4j.configuration.GraphDatabaseSettings.query_log_max_parameter_length;
import static org.neo4j.configuration.GraphDatabaseSettings.query_statistics_divergence_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;
import static org.neo4j.configuration.GraphDatabaseSettings.script_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.shutdown_transaction_end_timeout;
import static org.neo4j.configuration.GraphDatabaseSettings.track_query_cpu_time;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_log_buffer_size;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_monitor_check_interval;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_sampling_percentage;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_timeout;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_tracing_level;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_max_off_heap_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_memory_allocation;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_off_heap_block_cache_size;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_off_heap_max_cacheable_block_size;
import static org.neo4j.configuration.GraphDatabaseSettings.writable_databases;
import static org.neo4j.configuration.connectors.BoltConnectorInternalSettings.thread_pool_shutdown_wait_time;
import static org.neo4j.configuration.connectors.BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_max_inbound_bytes;
import static org.neo4j.configuration.connectors.BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_timeout;
import static org.neo4j.configuration.connectors.BoltConnectorInternalSettings.unsupported_thread_pool_queue_size;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.GraphDatabaseSettings.AnnotationDataFormat;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.logging.InternalLog;

public final class SettingMigrators {

    private SettingMigrators() {}

    @ServiceProvider
    public static class Neo4j5_0SettingMigrator implements SettingMigrator {

        private static final String OLD_PREFIX = "dbms.connector";
        private static final Pattern SUPPORTED_CONNECTOR_PATTERN = Pattern.compile("(.+)\\.(bolt|http|https)\\.(.+)");
        private static final List<String> REMOVED_SETTINGS = List.of(
                "dbms.allow_single_automatic_upgrade",
                "dbms.allow_upgrade",
                "dbms.clustering.enable",
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
                "dbms.logs.debug.format",
                "dbms.logs.debug.level",
                "dbms.logs.debug.path",
                "dbms.logs.debug.rotation.keep_number",
                "dbms.logs.debug.rotation.size",
                "dbms.logs.default_format",
                "dbms.logs.http.format",
                "dbms.logs.http.path",
                "dbms.logs.http.rotation.keep_number",
                "dbms.logs.http.rotation.size",
                "dbms.logs.query.format",
                "dbms.logs.query.path",
                "dbms.logs.query.rotation.keep_number",
                "dbms.logs.query.rotation.size",
                "dbms.logs.security.format",
                "dbms.logs.security.level",
                "dbms.logs.security.path",
                "dbms.logs.security.rotation.delay",
                "dbms.logs.security.rotation.keep_number",
                "dbms.logs.security.rotation.size",
                "dbms.logs.user.format",
                "dbms.logs.user.path",
                "dbms.logs.user.rotation.keep_number",
                "dbms.logs.user.rotation.size",
                "dbms.logs.user.stdout_enabled",
                "unsupported.cypher.parser",
                "unsupported.dbms.block_remote_alias",
                "unsupported.dbms.large_cluster.enable",
                "unsupported.dbms.memory.managed_network_buffers",
                "unsupported.dbms.memory.pagecache.warmup.legacy_profile_loader",
                "unsupported.dbms.lucene.ephemeral",
                "unsupported.dbms.recovery.ignore_store_id_validation",
                "unsupported.dbms.storage_engine",
                "unsupported.dbms.tokenscan.log.enabled",
                "unsupported.dbms.tokenscan.log.prune_threshold",
                "unsupported.dbms.tokenscan.log.rotation_threshold",
                "unsupported.dbms.topology_graph.enable",
                "unsupported.dbms.topology_graph_updater.enable",
                "unsupported.dbms.uris.rest",
                "unsupported.dbms.reserved.page.header.bytes",
                "unsupported.dbms.index.default_fulltext_provider",
                "unsupported.dbms.lock_manager",
                "internal.dbms.lock_manager",
                "cypher.default_language_version",
                "dbms.logs.query.allocation_logging_enabled",
                "dbms.track_query_allocation",
                "dbms.logs.query.page_logging_enabled",
                "dbms.logs.query.runtime_logging_enabled",
                "dbms.logs.query.time_logging_enabled",
                "dbms.logs.query.transaction_id.enabled",
                "dbms.logs.query.parameter_full_entities",
                "fabric.database.name",
                "fabric.driver.api",
                "fabric.driver.connection.connect_timeout",
                "fabric.driver.connection.max_lifetime",
                "fabric.driver.connection.pool.acquisition_timeout",
                "fabric.driver.connection.pool.idle_test",
                "fabric.driver.connection.pool.max_size",
                "fabric.driver.event_loop_count",
                "fabric.driver.idle_check_interval",
                "fabric.driver.logging.leaked_sessions",
                "fabric.driver.logging.level",
                "fabric.driver.timeout",
                "fabric.enabled_by_default",
                "fabric.graph.mega.database",
                "fabric.graph.mega.driver.api",
                "fabric.graph.mega.driver.connection.connect_timeout",
                "fabric.graph.mega.driver.connection.max_lifetime",
                "fabric.graph.mega.driver.connection.pool.acquisition_timeout",
                "fabric.graph.mega.driver.connection.pool.idle_test",
                "fabric.graph.mega.driver.connection.pool.max_size",
                "fabric.graph.mega.driver.logging.leaked_sessions",
                "fabric.graph.mega.driver.logging.level",
                "fabric.graph.mega.driver.ssl_enabled",
                "fabric.graph.mega.name",
                "fabric.graph.mega.uri",
                "fabric.routing.servers",
                "fabric.routing.ttl",
                "fabric.stream.batch_size",
                "fabric.stream.buffer.low_watermark",
                "fabric.stream.buffer.size",
                "fabric.stream.concurrency",
                "internal.cypher.planning_point_indexes_enabled",
                "internal.cypher.planning_text_indexes_enabled",
                "internal.cypher.planning_range_indexes_enabled",
                "unsupported.cypher.planning_point_indexes_enabled",
                "unsupported.cypher.planning_range_indexes_enabled",
                "unsupported.cypher.planning_text_indexes_enabled",
                "unsupported.cypher.splitting_top_behavior",
                "internal.cypher.number_of_workers",
                "internal.dbms.cluster.info_service_deallocated_enabled",
                "internal.dbms.cluster.discovery.parallel_enabled",
                "unsupported.dbms.kernel_id",
                "internal.dbms.kernel_id",
                "internal.dbms.linked_users");

        private static final Collection<Mapping> LEGACY_UNSUPPORTED_SETTINGS_MAPPING = List.of(
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
                new Mapping("unsupported.cypher.number_of_workers", "server.cypher.parallel.worker_limit"),
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
                new Mapping("unsupported.cypher.replan_algorithm", "internal.cypher.replan_algorithm"),
                new Mapping("unsupported.cypher.runtime", "internal.cypher.runtime"),
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
                        "unsupported.dbms.bolt.netty_server_use_epoll",
                        "internal.dbms.bolt.netty_server_use_native_transport"),
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
                        "internal.dbms.include_dev_format_versions"),
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
                new Mapping(
                        "unsupported.dbms.lock_manager.verbose_deadlocks",
                        "internal.dbms.lock_manager.verbose_deadlocks"),
                new Mapping(
                        "unsupported.dbms.logs.query.heap_dump_enabled", "internal.dbms.logs.query.heap_dump_enabled"),
                new Mapping("unsupported.dbms.loopback_delete", "internal.dbms.loopback_delete"),
                new Mapping("unsupported.dbms.loopback_enabled", "internal.dbms.loopback_enabled"),
                new Mapping("unsupported.dbms.loopback_file", "internal.dbms.loopback_file"),
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
                new Mapping(
                        "unsupported.dbms.strictly_prioritize_id_freelist",
                        "internal.dbms.strictly_prioritize_id_freelist"),
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
            migrateConnectors(values, defaultValues, log);
            migrateDatabaseMemorySettings(values, defaultValues, log);
            migrateWhitelistSettings(values, defaultValues, log);
            migrateWindowsServiceName(values, defaultValues, log);
            migrateGroupSpatialSettings(values, defaultValues, log);
            migrateCheckpointSettings(values, defaultValues, log);
            migrateKeepAliveSetting(values, defaultValues, log);
            migrateReadOnlySettings(values, defaultValues, log);
            migrateDefaultDatabaseSetting(values, defaultValues, log);
            migrateDatabaseMaxSize(values, defaultValues, log);
            migrateCypherNamespace(values, defaultValues, log);
            migrateCypherQueryCacheSize(values, defaultValues, log);
            migrateTxStateAndLogsSettings(values, defaultValues, log);
            migrateTransactionAndTrackingSettings(values, defaultValues, log);
            migrateGcLogsSettings(values, defaultValues, log);
            migrateMaxProcessorToInternal(values, defaultValues, log);
            migratePageCacheWarmerSettings(values, defaultValues, log);
            migrateShutdownTimeoutAndFilePreallocation(values, defaultValues, log);
            migrateQueryCacheSize(values, defaultValues, log);
            migrateTransactionMemorySettings(values, defaultValues, log);
            migrateGroupAndRecoverySettings(values, defaultValues, log);
            migrateWatcherSetting(values, defaultValues, log);
            migrateCsvImportSetting(values, defaultValues, log);
            migrateLockAcquisitionSetting(values, defaultValues, log);
            migrateDefaultAddress(values, defaultValues, log);
            migrateQueryLoggingSettings(values, defaultValues, log);
            migrateJvmAdditional(values, defaultValues, log);
            migrateSamplingSettings(values, defaultValues, log);
            migratePageCacheAndMemorySettings(values, defaultValues, log);
            migrateAutoUpgrade(values, defaultValues, log);
            migrateAnnotationDataAsJson(values, defaultValues, log);
        }

        private void migratePageCacheAndMemorySettings(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.memory.pagecache.size", pagecache_memory);
            migrateSettingNameChange(values, log, "dbms.memory.pagecache.scan.prefetchers", pagecache_scan_prefetch);
            migrateSettingNameChange(
                    values,
                    log,
                    "dbms.memory.pagecache.flush.buffer.size_in_pages",
                    pagecache_flush_buffer_size_in_pages);
            migrateSettingNameChange(
                    values, log, "dbms.memory.pagecache.flush.buffer.enabled", pagecache_buffered_flush_enabled);
            migrateSettingNameChange(values, log, "dbms.memory.pagecache.directio", pagecache_direct_io);

            migrateSettingNameChange(values, log, "dbms.memory.off_heap.max_size", tx_state_max_off_heap_memory);
            // renaming the previous 5.x setting
            migrateSettingNameChange(values, log, "server.memory.off_heap.max_size", tx_state_max_off_heap_memory);
            migrateSettingNameChange(
                    values,
                    log,
                    "dbms.memory.off_heap.max_cacheable_block_size",
                    tx_state_off_heap_max_cacheable_block_size);
            migrateSettingNameChange(
                    values, log, "dbms.memory.off_heap.block_cache_size", tx_state_off_heap_block_cache_size);

            migrateSettingNameChange(values, log, "dbms.memory.heap.max_size", max_heap_size);
            migrateSettingNameChange(values, log, "dbms.memory.heap.initial_size", initial_heap_size);
        }

        private void migrateSamplingSettings(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(
                    values, log, "dbms.index_sampling.background_enabled", index_background_sampling_enabled);
            migrateSettingNameChange(values, log, "dbms.index_sampling.sample_size_limit", index_sample_size_limit);
            migrateSettingNameChange(
                    values, log, "dbms.index_sampling.update_percentage", index_sampling_update_percentage);
        }

        private void migrateJvmAdditional(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            String oldSettingName = "dbms.jvm.additional";
            String value = values.get(oldSettingName);

            // Remove a deprecated jvm additional we used to have in the default config in 4.x
            if (isNotBlank(value)) {
                String deprecatedValue = "-XX:-UseBiasedLocking";
                if (value.contains(deprecatedValue)) {
                    String newValue = value.replaceAll(System.lineSeparator() + deprecatedValue, "");
                    // If it was the first one it won't have a line separator before it
                    newValue = newValue.replaceAll(deprecatedValue + System.lineSeparator(), "");
                    // If it is coming from the migrate-config tool or is the only additional it won't have separator
                    newValue = newValue.replaceAll(deprecatedValue, "");

                    values.put(oldSettingName, newValue);
                }
            }

            migrateSettingNameChange(values, log, oldSettingName, additional_jvm);
        }

        private void migrateQueryLoggingSettings(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(
                    values, log, "dbms.logs.query.transaction.threshold", log_queries_transaction_threshold);
            migrateSettingNameChange(
                    values, log, "dbms.logs.query.transaction.enabled", log_queries_transactions_level);
            migrateSettingNameChange(values, log, "dbms.logs.query.threshold", log_queries_threshold);
            migrateSettingNameChange(values, log, "dbms.logs.query.plan_description_enabled", log_queries_query_plan);
            migrateSettingNameChange(
                    values, log, "dbms.logs.query.parameter_logging_enabled", log_queries_parameter_logging_enabled);
            migrateSettingNameChange(values, log, "dbms.logs.query.obfuscate_literals", log_queries_obfuscate_literals);
            migrateSettingNameChange(
                    values, log, "dbms.logs.query.max_parameter_length", query_log_max_parameter_length);
            migrateSettingNameChange(values, log, "dbms.logs.query.enabled", log_queries);
            migrateSettingNameChange(
                    values, log, "dbms.logs.query.early_raw_logging_enabled", log_queries_early_raw_logging_enabled);
        }

        private void migrateDefaultAddress(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.default_listen_address", default_listen_address);
            migrateSettingNameChange(values, log, "dbms.default_advertised_address", default_advertised_address);
        }

        private void migrateLockAcquisitionSetting(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.lock.acquisition.timeout", lock_acquisition_timeout);
        }

        private void migrateCsvImportSetting(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.import.csv.buffer_size", csv_buffer_size);
            migrateSettingNameChange(values, log, "dbms.import.csv.legacy_quote_escaping", csv_legacy_quote_escaping);
        }

        private void migrateWatcherSetting(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.filewatcher.enabled", filewatcher_enabled);
        }

        private void migrateGroupAndRecoverySettings(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.relationship_grouping_threshold", dense_node_threshold);
            migrateSettingNameChange(values, log, "dbms.recovery.fail_on_missing_files", fail_on_missing_files);
        }

        private void migrateTransactionMemorySettings(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(
                    values, log, "dbms.memory.transaction.database_max_size", memory_transaction_database_max_size);
            migrateSettingNameChange(
                    values, log, "dbms.memory.transaction.global_max_size", memory_transaction_global_max_size);
            migrateSettingNameChange(values, log, "dbms.memory.transaction.max_size", memory_transaction_max_size);
        }

        private void migrateQueryCacheSize(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.query_cache_size", query_cache_size);
        }

        private void migrateShutdownTimeoutAndFilePreallocation(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(
                    values, log, "dbms.shutdown_transaction_end_timeout", shutdown_transaction_end_timeout);
            migrateSettingNameChange(values, log, "dbms.store.files.preallocate", preallocate_store_files);
        }

        private void migratePageCacheWarmerSettings(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.memory.pagecache.warmup.enable", pagecache_warmup_enabled);
            migrateSettingNameChange(values, log, "dbms.memory.pagecache.warmup.preload", pagecache_warmup_prefetch);
            migrateSettingNameChange(
                    values, log, "dbms.memory.pagecache.warmup.preload.allowlist", pagecache_warmup_prefetch_allowlist);
            migrateSettingNameChange(
                    values, log, "dbms.memory.pagecache.warmup.profile.interval", pagecache_warmup_profiling_interval);
        }

        private void migrateMaxProcessorToInternal(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.upgrade_max_processors", upgrade_processors);
        }

        private void migrateGcLogsSettings(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.logs.gc.enabled", gc_logging_enabled);
            migrateSettingNameChange(values, log, "dbms.logs.gc.options", gc_logging_options);
            migrateSettingNameChange(values, log, "dbms.logs.gc.rotation.keep_number", gc_logging_rotation_keep_number);
            migrateSettingNameChange(values, log, "dbms.logs.gc.rotation.size", gc_logging_rotation_size);
        }

        private void migrateTxStateAndLogsSettings(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.tx_log.buffer.size", transaction_log_buffer_size);
            migrateSettingNameChange(values, log, "dbms.tx_log.preallocate", preallocate_logical_logs);
            migrateSettingNameChange(values, log, "dbms.tx_log.rotation.retention_policy", keep_logical_logs);
            migrateSettingNameChange(values, log, "dbms.tx_log.rotation.size", logical_log_rotation_threshold);
            migrateSettingNameChange(values, log, "dbms.tx_state.memory_allocation", tx_state_memory_allocation);
        }

        private void migrateTransactionAndTrackingSettings(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.track_query_cpu_time", track_query_cpu_time);
            migrateSettingNameChange(values, log, "dbms.transaction.bookmark_ready_timeout", bookmark_ready_timeout);
            migrateSettingNameChange(values, log, "dbms.transaction.concurrent.maximum", max_concurrent_transactions);
            migrateSettingNameChange(
                    values, log, "dbms.transaction.monitor.check.interval", transaction_monitor_check_interval);
            migrateSettingNameChange(
                    values, log, "dbms.transaction.sampling.percentage", transaction_sampling_percentage);
            migrateSettingNameChange(values, log, "dbms.transaction.timeout", transaction_timeout);
            migrateSettingNameChange(values, log, "dbms.transaction.tracing.level", transaction_tracing_level);
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

        private static void migrateReadOnlySettings(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.read_only", read_only_database_default);
            migrateSettingNameChange(values, log, "dbms.databases.default_to_read_only", read_only_database_default);
            migrateSettingNameChange(values, log, "dbms.databases.read_only", read_only_databases);
            migrateSettingNameChange(values, log, "dbms.databases.writable", writable_databases);
        }

        private static void migrateDefaultDatabaseSetting(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "dbms.default_database", initial_default_database);
        }

        private static void migrateDatabaseMaxSize(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(
                    values, log, "dbms.memory.transaction.datababase_max_size", memory_transaction_database_max_size);
        }

        private static void migrateCypherNamespace(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(
                    values, log, "cypher.forbid_exhaustive_shortestpath", forbid_exhaustive_shortestpath);
            migrateSettingNameChange(
                    values, log, "cypher.forbid_shortestpath_common_nodes", forbid_shortestpath_common_nodes);
            migrateSettingNameChange(values, log, "cypher.hints_error", cypher_hints_error);
            migrateSettingNameChange(
                    values, log, "cypher.lenient_create_relationship", cypher_lenient_create_relationship);
            migrateSettingNameChange(values, log, "cypher.min_replan_interval", cypher_min_replan_interval);
            migrateSettingNameChange(values, log, "cypher.planner", cypher_planner);
            migrateSettingNameChange(values, log, "cypher.render_plan_description", cypher_render_plan_descriptions);
            migrateSettingNameChange(
                    values, log, "cypher.statistics_divergence_threshold", query_statistics_divergence_threshold);
        }

        private static void migrateCypherQueryCacheSize(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(values, log, "server.db.query_cache_size", query_cache_size);
        }

        private static void migrateAutoUpgrade(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            migrateSettingNameChange(
                    values, log, "internal.dbms.allow_single_automatic_upgrade", automatic_upgrade_enabled);
        }

        private static void migrateAnnotationDataAsJson(
                Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            String jsonEnabledString = values.remove("db.logs.query.annotation_data_as_json_enabled");
            String annotationDataFormat = values.get(log_queries_annotation_data_format.name());
            if (isNotBlank(jsonEnabledString)) {
                log.warn(
                        "Use of deprecated setting 'db.logs.query.annotation_data_as_json_enabled'. It is replaced by %s.",
                        log_queries_annotation_data_format.name());
                if (isNotBlank(annotationDataFormat)) {
                    log.warn(
                            "The setting %s is already configured, ignoring db.logs.query.annotation_data_as_json_enabled.",
                            log_queries_annotation_data_format.name());
                } else {
                    boolean jsonEnabled = SettingValueParsers.BOOL.parse(jsonEnabledString);
                    if (jsonEnabled) {
                        values.put(log_queries_annotation_data_format.name(), AnnotationDataFormat.FLAT_JSON.name());
                    } else {
                        values.put(log_queries_annotation_data_format.name(), AnnotationDataFormat.CYPHER.name());
                    }
                }
            }
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
            Map<String, String> values,
            InternalLog log,
            Collection<String> prefixes,
            String oldSettingSuffix,
            Setting<?> newSetting) {
        prefixes.stream()
                .map(p -> p + oldSettingSuffix)
                .forEach(key -> migrateSettingNameChange(values, log, key, newSetting));
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

    public static void migrateSettingRemoval(
            Map<String, String> values,
            InternalLog log,
            Collection<String> prefixes,
            String oldSettingSuffix,
            String explanation) {
        prefixes.stream()
                .map(p -> p + oldSettingSuffix)
                .forEach(key -> migrateSettingRemoval(values, log, key, explanation));
    }
}
