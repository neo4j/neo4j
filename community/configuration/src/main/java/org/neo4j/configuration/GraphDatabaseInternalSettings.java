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

import inet.ipaddr.IPAddressString;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.graphdb.config.Setting;

import static java.time.Duration.ofDays;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.neo4j.configuration.SettingConstraints.any;
import static org.neo4j.configuration.SettingConstraints.is;
import static org.neo4j.configuration.SettingConstraints.max;
import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.configuration.SettingValueParsers.CIDR_IP;
import static org.neo4j.configuration.SettingValueParsers.DOUBLE;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.LONG;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.listOf;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;
import static org.neo4j.configuration.SettingValueParsers.setOf;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;

@ServiceProvider
public class GraphDatabaseInternalSettings implements SettingsDeclaration
{

    //=========================================================================
    // LOAD CSV and apoc.load.json input URI restrictions
    //=========================================================================
    @Internal
    @Description( "A list of CIDR-notation IPv4 or IPv6 addresses to block when accessing URLs." +
                  "This list is checked when LOAD CSV or apoc.load.json is called." )
    public static final Setting<List<IPAddressString>> cypher_ip_blocklist =
            newBuilder( "unsupported.dbms.cypher_ip_blocklist", listOf( CIDR_IP ), List.of() ).build();

    @Internal
    @Description( "Path of the databases directory" )
    public static final Setting<Path> databases_root_path =
            newBuilder( "unsupported.dbms.directories.databases.root", PATH, Path.of( GraphDatabaseSettings.DEFAULT_DATABASES_ROOT_DIR_NAME ) )
                    .setDependency( GraphDatabaseSettings.data_directory ).immutable().build();

    @Deprecated
    @Internal
    @Description( "Location where Neo4j keeps the logical transaction logs." )
    public static final Setting<Path> logical_logs_location =
            newBuilder( "dbms.directories.tx_log", PATH, Path.of( GraphDatabaseSettings.DEFAULT_DATABASE_NAME ) ).setDependency( databases_root_path ).build();

    @Internal
    @Description( "Configure lucene to be in memory only, for test environment. This is set in code and should never be configured explicitly." )
    public static final Setting<Boolean> ephemeral_lucene = newBuilder( "unsupported.dbms.lucene.ephemeral", BOOL, false ).build();

    @Internal
    @Description( "Name of the lock manager to be used, as defined in the corresponding LocksFactory." )
    public static final Setting<String> lock_manager = newBuilder( "unsupported.dbms.lock_manager", STRING, "forseti" ).build();

    @Internal
    @Description( "Include additional information in deadlock descriptions." )
    public static final Setting<Boolean> lock_manager_verbose_deadlocks = newBuilder( "unsupported.dbms.lock_manager.verbose_deadlocks", BOOL, false ).build();

    @Internal
    @Description( "Name of the tracer factory to be used. Current implementations are: null, default & verbose." )
    public static final Setting<String> tracer = newBuilder( "unsupported.dbms.tracer", STRING, null ).build();

    @Internal
    @Description( "Print out the effective Neo4j configuration after startup." )
    public static final Setting<Boolean> dump_configuration = newBuilder( "unsupported.dbms.report_configuration", BOOL, false ).build();

    @Internal
    @Description( "Specifies if the consistency checker should stop when number of observed inconsistencies exceed the threshold. " +
            "If the value is zero, all inconsistencies will be reported" )
    public static final Setting<Integer> consistency_checker_fail_fast_threshold =
            newBuilder( "unsupported.consistency_checker.fail_fast_threshold", INT, 0 ).addConstraint( min( 0 ) ).build();

    public enum CypherRuntime
    {
        DEFAULT, INTERPRETED, SLOTTED, PIPELINED
    }

    @Internal
    @Description( "Set this to specify the default runtime for the default language version." )
    public static final Setting<CypherRuntime> cypher_runtime =
            newBuilder( "unsupported.cypher.runtime", ofEnum( CypherRuntime.class ), CypherRuntime.DEFAULT ).build();

    public enum CypherExpressionEngine
    {
        DEFAULT, INTERPRETED, COMPILED, ONLY_WHEN_HOT
    }

    @Internal
    @Description( "Choose the expression engine. The default is to only compile expressions that are hot, if 'COMPILED' " +
            "is chosen all expressions will be compiled directly and if 'INTERPRETED' is chosen expressions will " +
            "never be compiled." )
    public static final Setting<CypherExpressionEngine> cypher_expression_engine =
            newBuilder( "unsupported.cypher.expression_engine", ofEnum( CypherExpressionEngine.class ), CypherExpressionEngine.DEFAULT ).build();

    @Internal
    @Description( "The maximum size in bytes of methods generated for compiled expressions" )
    public static final Setting<Integer> cypher_expression_compiled_method_limit = newBuilder(
            "unsupported.cypher.expression_method_limit", INT, 8000 )
            .addConstraint( min( 0 ) )
            .addConstraint( max( 65535 ) )
            .build();

    @Internal
    @Description( "Number of uses before an expression is considered for compilation" )
    public static final Setting<Integer> cypher_expression_recompilation_limit = newBuilder( "unsupported.cypher.expression_recompilation_limit", INT, 10 )
            .addConstraint( min( 0 ) )
            .build();

    @Internal
    @Description( "Enable tracing of compilation in cypher." )
    public static final Setting<Boolean> cypher_compiler_tracing = newBuilder( "unsupported.cypher.compiler_tracing", BOOL, false ).build();

    @Internal
    @Description( "Large databases might change slowly, and so to prevent queries from never being replanned " +
            "the divergence threshold set by cypher.statistics_divergence_threshold is configured to " +
            "shrink over time. " +
            "The algorithm used to manage this change is set by unsupported.cypher.replan_algorithm " +
            "and will cause the threshold to reach the value set here once the time since the previous " +
            "replanning has reached unsupported.cypher.target_replan_interval. " +
            "Setting this value to higher than the cypher.statistics_divergence_threshold will cause the " +
            "threshold to not decay over time." )
    public static final Setting<Double> query_statistics_divergence_target =
            newBuilder( "unsupported.cypher.statistics_divergence_target", DOUBLE, 0.10 ).addConstraint( range( 0.0, 1.0 ) ).build();

    @Internal
    @Description( "The threshold when a warning is generated if a label scan is done after a load csv where the label has no index" )
    public static final Setting<Long> query_non_indexed_label_warning_threshold =
            newBuilder( "unsupported.cypher.non_indexed_label_warning_threshold", LONG, 10000L ).build();

    @Internal
    @Description( "To improve IDP query planning time, we can restrict the internal planning table size, " +
            "triggering compaction of candidate plans. The smaller the threshold the faster the planning, " +
            "but the higher the risk of sub-optimal plans." )
    public static final Setting<Integer> cypher_idp_solver_table_threshold =
            newBuilder( "unsupported.cypher.idp_solver_table_threshold", INT, 128 ).addConstraint( min( 16 ) ).build();

    @Internal
    @Description( "To improve IDP query planning time, we can restrict the internal planning loop duration, " +
            "triggering more frequent compaction of candidate plans. The smaller the threshold the " +
            "faster the planning, but the higher the risk of sub-optimal plans." )
    public static final Setting<Long> cypher_idp_solver_duration_threshold =
            newBuilder( "unsupported.cypher.idp_solver_duration_threshold", LONG, 1000L ).addConstraint( min( 10L ) ).build();

    @Internal
    @Description( "Large databases might change slowly, and to prevent queries from never being replanned " +
            "the divergence threshold set by cypher.statistics_divergence_threshold is configured to " +
            "shrink over time. The algorithm used to manage this change is set by " +
            "unsupported.cypher.replan_algorithm and will cause the threshold to reach " +
            "the value set by unsupported.cypher.statistics_divergence_target once the time since the " +
            "previous replanning has reached the value set here. Setting this value to less than the " +
            "value of cypher.min_replan_interval will cause the threshold to not decay over time." )
    public static final Setting<Duration> cypher_replan_interval_target =
            newBuilder( "unsupported.cypher.target_replan_interval", DURATION, Duration.ofHours( 7 ) ).build();

    public enum CypherReplanAlgorithm
    {
        DEFAULT, NONE, INVERSE, EXPONENTIAL
    }

    @Internal
    @Description( "Large databases might change slowly, and to prevent queries from never being replanned " +
            "the divergence threshold set by cypher.statistics_divergence_threshold is configured to " +
            "shrink over time using the algorithm set here. This will cause the threshold to reach " +
            "the value set by unsupported.cypher.statistics_divergence_target once the time since the " +
            "previous replanning has reached the value set in unsupported.cypher.target_replan_interval. " +
            "Setting the algorithm to 'none' will cause the threshold to not decay over time." )
    public static final Setting<CypherReplanAlgorithm> cypher_replan_algorithm =
            newBuilder( "unsupported.cypher.replan_algorithm", ofEnum( CypherReplanAlgorithm.class ), CypherReplanAlgorithm.DEFAULT ).build();

    @Internal
    @Description( "Set this to enable monitors in the Cypher runtime." )
    public static final Setting<Boolean> cypher_enable_runtime_monitors =
            newBuilder( "unsupported.cypher.enable_runtime_monitors", BOOL, false ).build();

    @Internal
    @Description( "Enable tracing of pipelined runtime scheduler." )
    public static final Setting<Boolean> enable_pipelined_runtime_trace =
            newBuilder( "unsupported.cypher.pipelined.enable_runtime_trace", BOOL, false ).build();

    @Internal
    @Description( "Path to the pipelined runtime scheduler trace. If 'stdOut' and tracing is on, will print to std out." )
    public static final Setting<Path> pipelined_scheduler_trace_filename =
            newBuilder( "unsupported.cypher.pipelined.runtime_trace_path", PATH, Path.of( "stdOut" ) )
                    .setDependency( GraphDatabaseSettings.neo4j_home )
                    .immutable()
                    .build();

    @Internal
    @Description( "The size of batches in the pipelined runtime for queries which work with few rows." )
    public static final Setting<Integer> cypher_pipelined_batch_size_small =
            newBuilder( "unsupported.cypher.pipelined.batch_size_small", INT, 128 ).addConstraint( min( 1 ) ).build();

    @Internal
    @Description( "The size of batches in the pipelined runtime for queries which work with many rows." )
    public static final Setting<Integer> cypher_pipelined_batch_size_big =
            newBuilder( "unsupported.cypher.pipelined.batch_size_big", INT, 1024 ).addConstraint( min( 1 ) ).build();

    @Internal
    @Description( "Number of threads to allocate to Cypher worker threads for the parallel runtime. If set to 0, two workers will be started" +
                  " for every physical core in the system. If set to -1, no workers will be started and the parallel runtime cannot be used." )
    public static final Setting<Integer> cypher_worker_count = newBuilder( "unsupported.cypher.number_of_workers", INT, 0 ).build();

    public enum CypherOperatorEngine
    {
        DEFAULT, COMPILED, INTERPRETED
    }

    @Internal
    @Description( "For compiled execution, specialized code is generated and then executed. " +
                  "More optimizations such as operator fusion may apply. " +
                  "Operator fusion means that multiple operators such as for example " +
                  "AllNodesScan -> Filter -> ProduceResult can be compiled into a single specialized operator. " +
                  "This setting only applies to the pipelined and parallel runtime. " +
                  "Allowed values are \"default\" (the default, use compiled when applicable), \"compiled\" and \"interpreted\"." )
    public static final Setting<CypherOperatorEngine> cypher_operator_engine =
            newBuilder( "unsupported.cypher.pipelined.operator_engine", ofEnum( CypherOperatorEngine.class ), CypherOperatorEngine.DEFAULT ).build();

    @Internal
    @Description( "The maximum size in bytes of methods generated for fused operators" )
    public static final Setting<Integer> cypher_operator_compiled_method_limit = newBuilder(
            "unsupported.cypher.pipelined.method_limit", INT, 65535 )
            .addConstraint( min( 0 ) )
            .addConstraint( max( 65535 ) )
            .build();

    public enum CypherPipelinedInterpretedPipesFallback
    {
        DISABLED, DEFAULT, ALL, WHITELISTED_PLANS_ONLY
    }
    @Internal
    @Description( "If set to true we can force source code generation by appending debug=generate_java_source to query" )
    public static final Setting<Boolean> cypher_allow_source_generation = newBuilder(
            "internal.cypher.pipelined.allow_source_generation", BOOL, false )
            .build();

    @Internal
    @Description( "Use interpreted pipes as a fallback for operators that do not have a specialized implementation in the pipelined runtime. " +
                  "Allowed values are \"disabled\", \"default\" (the default, use whitelisted_plans_only when applicable), \"whitelisted_plans_only\" " +
                  "and \"all\" (experimental). " +
                  "The default is to enable the use of a subset of whitelisted operators that are known to be supported, whereas \"all\" is an " +
                  "experimental option that enables the fallback to be used for all possible operators that are not known to be unsupported." )
    public static final Setting<CypherPipelinedInterpretedPipesFallback> cypher_pipelined_interpreted_pipes_fallback =
            newBuilder( "unsupported.cypher.pipelined_interpreted_pipes_fallback", ofEnum( CypherPipelinedInterpretedPipesFallback.class ),
                    CypherPipelinedInterpretedPipesFallback.DEFAULT ).build();

    @Internal
    @Description( "The maximum number of operator fusions over pipelines (i.e. where an operator that would normally be considered pipeline-breaking, " +
                  "e.g. expand), that is considered before a pipeline break is forced." )
    public static final Setting<Integer> cypher_pipelined_operator_fusion_over_pipeline_limit =
            newBuilder( "unsupported.cypher.pipelined.operator_fusion_over_pipeline_limit", INT, 8 ).build();

    public enum CypherParser
    {
        DEFAULT, PARBOILED, JAVACC
    }
    @Internal
    @Description( "The parser implementation to use for parsing cypher queries." )
    public static final Setting<CypherParser> cypher_parser =
            newBuilder( "unsupported.cypher.parser", ofEnum( CypherParser.class ), CypherParser.DEFAULT ).build();

    public enum SplittingTopBehavior
    {
        DEFAULT, DISALLOW
    }
    @Internal
    @Description( "Determines whether the planner is allowed to push down the sort portion of an ORDER BY + LIMIT combination" )
    public static final Setting<SplittingTopBehavior> cypher_splitting_top_behavior =
            newBuilder( "unsupported.cypher.splitting_top_behavior", ofEnum( SplittingTopBehavior.class ), SplittingTopBehavior.DEFAULT ).build();

    @Internal
    @Description( "Enables extra SemanticFeature:s during cypher semantic checking" )
    public static final Setting<Set<String>> cypher_enable_extra_semantic_features =
            newBuilder( "unsupported.cypher.enable_extra_semantic_features", setOf(STRING), Set.of() ).build();

    @Internal
    @Description( "Max number of recent queries to collect in the data collector module. Will round down to the" +
            " nearest power of two. The default number (8192 query invocations) " +
            " was chosen as a trade-off between getting a useful amount of queries, and not" +
            " wasting too much heap. Even with a buffer full of unique queries, the estimated" +
            " footprint lies in tens of MBs. If the buffer is full of cached queries, the" +
            " retained size was measured to 265 kB. Setting this to 0 will disable data collection" +
            " of queries completely." )
    public static final Setting<Integer> data_collector_max_recent_query_count =
            newBuilder( "unsupported.datacollector.max_recent_query_count", INT, 8192 ).addConstraint( min( 0 ) ).build();

    @Internal
    @Description( "Sets the upper limit for how much of the query text that will be retained by the query collector." +
            " For queries longer than the limit, only a prefix of size limit will be retained by the collector." +
            " Lowering this value will reduce the memory footprint of collected query invocations under loads with" +
            " many queries with long query texts, which could occur for generated queries. The downside is that" +
            " on retrieving queries by `db.stats.retrieve`, queries longer than this max size would be returned" +
            " incomplete. Setting this to 0 will completely drop query texts from the collected queries." )
    public static final Setting<Integer> data_collector_max_query_text_size =
            newBuilder( "unsupported.datacollector.max_query_text_size", INT, 10000 ).addConstraint( min( 0 ) ).build();

    @Internal
    @Description( "Enable or disable the ability to create and drop databases." )
    public static final Setting<Boolean> block_create_drop_database =
            newBuilder( "unsupported.dbms.block_create_drop_database", BOOL, false ).build();

    @Internal
    @Description( "Enable or disable the ability to start and stop databases." )
    public static final Setting<Boolean> block_start_stop_database =
            newBuilder( "unsupported.dbms.block_start_stop_database", BOOL, false ).build();

    @Internal
    @Description( "Enable or disable the ability to alter databases." )
    public static final Setting<Boolean> block_alter_database =
            newBuilder( "unsupported.dbms.block_alter_database", BOOL, false ).build();

    @Internal
    @Description( "Enable or disable the ability to use remote aliases." )
    public static final Setting<Boolean> block_remote_alias =
            newBuilder( "unsupported.dbms.block_remote_alias", BOOL, false ).build();

    @Internal
    @Description( "Enable or disable the ability to execute the `dbms.upgrade` procedure." )
    public static final Setting<Boolean> block_upgrade_procedures =
            newBuilder( "unsupported.dbms.upgrade_restriction_enabled", BOOL, false ).build();

    @Internal
    @Description( "The maximum amount of time to wait for the database to become available, when starting a new transaction." )
    public static final Setting<Duration> transaction_start_timeout =
            newBuilder( "unsupported.dbms.transaction_start_timeout", DURATION, ofSeconds( 1 ) ).build();

    @Internal
    @Description( "Location of the database scripts directory." )
    public static final Setting<Path> scripts_dir = newBuilder( "unsupported.dbms.directories.scripts", PATH, Path.of("scripts" ) )
            .setDependency( GraphDatabaseSettings.neo4j_home )
            .immutable()
            .build();

    @Internal
    @Description( "Name of file containing commands to be run during initialization of the system database. " +
                  "The file should exists in the scripts directory in neo4j home directory." )
    public static final Setting<Path> system_init_file =
            newBuilder( "dbms.init_file", PATH, null ).immutable().setDependency( scripts_dir ).build();

    @Internal
    @Description( "Maximum time to wait for active transaction completion when rotating counts store" )
    public static final Setting<Duration> counts_store_rotation_timeout =
            newBuilder( "unsupported.dbms.counts_store_rotation_timeout", DURATION, ofMinutes( 10 ) ).build();

    @Internal
    @Description( "Set the maximum number of threads that can concurrently be used to sample indexes. Zero means unrestricted." )
    public static final Setting<Integer> index_sampling_parallelism =
            newBuilder( "unsupported.dbms.index_sampling.parallelism", INT, 4 ).addConstraint( min( 0 ) ).build();

    @Internal
    @Description( "Set the maximum number of concurrent index populations across system. " +
            "This also limit the number of threads used to scan store. " +
            "Note that multiple indexes can be populated by a single index population if they were created in the same transaction. " +
            "Zero means unrestricted. " )
    public static final Setting<Integer> index_population_parallelism =
            newBuilder( "unsupported.dbms.index_population.parallelism", INT, 2 ).addConstraint( min( 0 ) ).build();

    @Internal
    @Description( "Set the number of threads used for each index population job. " +
            "Those threads execute individual subtasks provided by index population main threads, see unsupported.dbms.index_population.parallelism." +
            "Zero means one thread per cpu core. " +
            "Thus the maximum total number of index worker threads in the system is " +
            "unsupported.dbms.index_population.workers * unsupported.dbms.index_population.parallelism." )
    public static final Setting<Integer> index_population_workers =
            newBuilder( "unsupported.dbms.index_population.workers", INT, Integer.max( 1, Runtime.getRuntime().availableProcessors() / 4 ) )
                    .addConstraint( min( 0 ) ).build();

    @Internal
    @Description( "The default index provider used for managing full-text indexes. Only 'fulltext-1.0' is supported." )
    public static final Setting<String> default_fulltext_provider =
            newBuilder( "unsupported.dbms.index.default_fulltext_provider", STRING, "fulltext-1.0" ).build();

    @Internal
    @Description( "If 'true', new database will be created without token indexes for labels and relationships." )
    public static final Setting<Boolean> skip_default_indexes_on_creation =
            newBuilder( "unsupported.dbms.index.skip_default_indexes_on_creation", BOOL, false ).build();

    @Internal
    @Description( "If `true`, Neo4j will abort recovery if any errors are encountered in the logical log. Setting " +
            "this to `false` will allow Neo4j to restore as much as possible from the corrupted log files and ignore " +
            "the rest, but, the integrity of the database might be compromised." )
    public static final Setting<Boolean> fail_on_corrupted_log_files =
            newBuilder("unsupported.dbms.tx_log.fail_on_corrupted_log_files", BOOL, true ).build();

    @Internal
    @Description( "Specifies if engine should run cypher query based on a snapshot of accessed data. " +
            "Query will be restarted in case if concurrent modification of data will be detected." )
    public static final Setting<Boolean> snapshot_query = newBuilder( "unsupported.dbms.query.snapshot", BOOL, false ).build();

    @Internal
    @Description( "Specifies number or retries that query engine will do to execute query based on stable accessed data snapshot before giving up." )
    public static final Setting<Integer> snapshot_query_retries =
            newBuilder( "unsupported.dbms.query.snapshot.retries", INT, 5 ).addConstraint( range( 1, Integer.MAX_VALUE ) ).build();

    @Description( "Cypher keeps a cache of the conversion from logical plans to execution plans. " +
                  "This cache is mainly meant to avoid generating code multiple times if different queries use the same logical plan. " +
                  "Items are only evicted from the cache when all query caches are cleared, e.g. by calling `db.clearQueryCaches()`. " +
                  "The cache is allowed to grow up to this size. " +
                  "If the size is set to -1 (default), it will use the size configured for the query cache, that is `dbms.query_cache_size`" +
                  "Setting the size to 0 means disabling this cache." )
    public static final Setting<Integer> query_execution_plan_cache_size =
            newBuilder( "unsupported.dbms.query_execution_plan_cache_size", INT, -1 ).addConstraint( min( -1 ) ).build();

    /**
     * Block size properties values depends from selected record format.
     * We can't figured out record format until it will be selected by corresponding edition.
     * As soon as we will figure it out properties will be re-evaluated and overwritten, except cases of user
     * defined value.
     */
    @Internal
    @Description( "Specifies the block size for storing strings. This parameter is only honored when the store is " +
            "created, otherwise it is ignored. " +
            "Note that each character in a string occupies two bytes, meaning that e.g a block size of 120 will hold " +
            "a 60 character long string before overflowing into a second block. " +
            "Also note that each block carries a ~10B of overhead so record size on disk will be slightly larger " +
            "than the configured block size" )
    public static final Setting<Integer> string_block_size =
            newBuilder( "unsupported.dbms.block_size.strings", INT, 0 ).addConstraint( min( 0 ) ).build();

    @Internal
    @Description( "Specifies the block size for storing arrays. This parameter is only honored when the store is " +
            "created, otherwise it is ignored. " +
            "Also note that each block carries a ~10B of overhead so record size on disk will be slightly larger " +
            "than the configured block size" )
    public static final Setting<Integer> array_block_size =
            newBuilder( "unsupported.dbms.block_size.array_properties", INT, 0 ).addConstraint( min( 0 ) ).build();

    @Internal
    @Description( "Specifies the block size for storing labels exceeding in-lined space in node record. " +
            "This parameter is only honored when the store is created, otherwise it is ignored. " +
            "Also note that each block carries a ~10B of overhead so record size on disk will be slightly larger " +
            "than the configured block size" )
    public static final Setting<Integer> label_block_size =
            newBuilder( "unsupported.dbms.block_size.labels", INT, 0 ).addConstraint( min( 0 ) ).build();

    @Internal
    @Description( "An identifier that uniquely identifies this graph database instance within this JVM. " +
            "Defaults to an auto-generated number depending on how many instance are started in this JVM." )
    public static final Setting<String> forced_kernel_id = newBuilder( "unsupported.dbms.kernel_id", STRING, null )
            .addConstraint( SettingConstraints.matches( "[a-zA-Z0-9]*", "has to be a valid kernel identifier" ) )
            .build();

    @Internal
    @Description( "VM pause monitor measurement duration" )
    public static final Setting<Duration> vm_pause_monitor_measurement_duration =
            newBuilder( "unsupported.vm_pause_monitor.measurement_duration", DURATION, ofMillis( 200 ) ).build();

    @Internal
    @Description( "Alert threshold for total pause time during one VM pause monitor measurement" )
    public static final Setting<Duration> vm_pause_monitor_stall_alert_threshold =
            newBuilder( "unsupported.vm_pause_monitor.stall_alert_threshold", DURATION, ofMillis( 200 ) ).build();

    @Internal
    @Description( "Create a heap dump just before the end of each query execution. " +
            "The heap dump will be placed in log directory and the file name will contain the query id, to be correlated with an entry in the query log. " +
            "Only live objects will be included to minimize the file size. " )
    public static final Setting<Boolean> log_queries_heap_dump_enabled =
            newBuilder( "unsupported.dbms.logs.query.heap_dump_enabled", BOOL, false ).dynamic().build();

    @Internal
    @Description( "Enables or disables logging of deprecation notifications to the structured query log." )
    public static final Setting<Boolean> log_queries_deprecation_notifications =
            newBuilder( "internal.dbms.logs.query.deprecation_notifications.enabled", BOOL, false ).dynamic().build();

    @Internal
    @Description( "Specifies number of operations that batch inserter will try to group into one batch before " +
            "flushing data into underlying storage." )
    public static final Setting<Integer> batch_inserter_batch_size =
            newBuilder( "unsupported.tools.batch_inserter.batch_size", INT, 10000 ).build();

    @Internal
    @Description( "Location of the auth store repository directory" )
    public static final Setting<Path> auth_store_directory = newBuilder( "unsupported.dbms.directories.auth", PATH, Path.of( "dbms" ) )
            .immutable()
            .setDependency( GraphDatabaseSettings.data_directory )
            .build();

    @Internal
    @Description( "Whether to apply network level outbound network buffer based throttling" )
    public static final Setting<Boolean> bolt_outbound_buffer_throttle =
            newBuilder( "unsupported.dbms.bolt.outbound_buffer_throttle", BOOL, true ).build();

    @Internal
    @Description( "When the size (in bytes) of outbound network buffers, used by bolt's network layer, " +
            "grows beyond this value bolt channel will advertise itself as unwritable and will block " +
            "related processing thread until it becomes writable again." )
    public static final Setting<Integer> bolt_outbound_buffer_throttle_high_water_mark =
            newBuilder( "unsupported.dbms.bolt.outbound_buffer_throttle.high_watermark", INT, (int) kibiBytes( 512 ) )
                    .addConstraint( range( (int) kibiBytes( 64 ), Integer.MAX_VALUE ) )
                    .build();

    @Internal
    @Description( "When the size (in bytes) of outbound network buffers, previously advertised as unwritable, " +
            "gets below this value bolt channel will re-advertise itself as writable and blocked processing " +
            "thread will resume execution." )
    public static final Setting<Integer> bolt_outbound_buffer_throttle_low_water_mark =
            newBuilder( "unsupported.dbms.bolt.outbound_buffer_throttle.low_watermark", INT, (int) kibiBytes( 128 ))
                    .addConstraint( range( (int) kibiBytes( 16 ), Integer.MAX_VALUE ) ).build();

    @Internal
    @Description( "When the total time outbound network buffer based throttle lock is held exceeds this value, " +
            "the corresponding bolt channel will be aborted. Setting " +
            "this to 0 will disable this behaviour." )
    public static final Setting<Duration> bolt_outbound_buffer_throttle_max_duration =
            newBuilder( "unsupported.dbms.bolt.outbound_buffer_throttle.max_duration", DURATION, ofMinutes( 15 )  )
                    .addConstraint( any( min( ofSeconds( 30 ) ), is( Duration.ZERO )  ) )
                    .build();

    @Internal
    @Description( "When the number of queued inbound messages grows beyond this value, reading from underlying " +
            "channel will be paused (no more inbound messages will be available) until queued number of " +
            "messages drops below the configured low watermark value." )
    public static final Setting<Integer> bolt_inbound_message_throttle_high_water_mark =
            newBuilder( "unsupported.dbms.bolt.inbound_message_throttle.high_watermark", INT, 300 )
                    .addConstraint( range( 1, Integer.MAX_VALUE ) ).build();

    @Internal
    @Description( "When the number of queued inbound messages, previously reached configured high watermark value, " +
            "drops below this value, reading from underlying channel will be enabled and any pending messages " +
            "will start queuing again." )
    public static final Setting<Integer> bolt_inbound_message_throttle_low_water_mark =
            newBuilder( "unsupported.dbms.bolt.inbound_message_throttle.low_watermark", INT, 100 )
                    .addConstraint( range( 1, Integer.MAX_VALUE ) )
                    .build();

    @Internal
    @Description( "Enable/disable the use of Epoll for netty" )
    public static final Setting<Boolean> netty_server_use_epoll = newBuilder( "unsupported.dbms.bolt.netty_server_use_epoll", BOOL, true ).build();

    @Internal
    @Description( "Quiet period for netty shutdown" )
    public static final Setting<Integer> netty_server_shutdown_quiet_period =
            newBuilder( "unsupported.dbms.bolt.netty_server_shutdown_quiet_period", INT, 5 ).build();

    @Internal
    @Description( "Timeout for netty shutdown" )
    public static final Setting<Duration> netty_server_shutdown_timeout =
            newBuilder( "unsupported.dbms.bolt.netty_server_shutdown_timeout", DURATION, ofSeconds( 15 ) ).build();

    @Internal
    @Description( "Enable/disable the use of a merge cumulator for netty" )
    public static final Setting<Boolean> netty_message_merge_cumulator =
            newBuilder( "unsupported.dbms.bolt.netty_message_merge_cumulator", BOOL, false ).build();

    @Internal
    @Description( "Create an archive of an index before re-creating it if failing to load on startup." )
    public static final Setting<Boolean> archive_failed_index =
            newBuilder( "unsupported.dbms.index.archive_failed", BOOL, false ).build();

    @Internal
    @Description( "Forces smaller ID cache, in order to preserve memory." )
    public static final Setting<Boolean> force_small_id_cache = newBuilder( "unsupported.dbms.force_small_id_cache", BOOL, Boolean.FALSE ).build();

    @Internal
    @Description( "Perform some data consistency checks on transaction apply" )
    public static final Setting<Boolean> consistency_check_on_apply =
            newBuilder( "unsupported.dbms.storage.consistency_check_on_apply", BOOL, Boolean.FALSE ).build();

    @Internal
    @Description( "Time interval of inactivity after which a driver will be closed." )
    public static final Setting<Duration> routing_driver_idle_timeout =
            newBuilder( "dbms.routing.driver.timeout", DURATION, ofMinutes( 1 ) ).build();

    @Internal
    @Description( "Time interval between driver idleness check." )
    public static final Setting<Duration> routing_driver_idle_check_interval =
            newBuilder( "dbms.routing.driver.idle_check_interval", DURATION, ofMinutes( 1 ) ).build();

    @Internal
    @Description( "Number of event loops used by drivers. Event loops are shard between drivers, so this is the total number of event loops created." )
    @DocumentedDefaultValue( "Number of available processors" )
    public static final Setting<Integer> routing_driver_event_loop_count =
            newBuilder( "dbms.routing.driver.event_loop_count", INT, Runtime.getRuntime().availableProcessors() ).build();

    @Internal
    @Description( "Enables logging of leaked driver session" )
    public static final Setting<Boolean> routing_driver_log_leaked_sessions =
            newBuilder( "dbms.routing.driver.logging.leaked_sessions", BOOL, false ).build();

    @Description( "Specifies at which file size the checkpoint log will auto-rotate. Minimum accepted value is 1 KiB. " )
    public static final Setting<Long> checkpoint_logical_log_rotation_threshold =
            newBuilder( "unsupported.dbms.checkpoint_log.rotation.size", BYTES, mebiBytes( 1 ) ).addConstraint( min( kibiBytes( 1 ) ) ).build();

    @Description( "Number of checkpoint logs files to keep." )
    public static final Setting<Integer> checkpoint_logical_log_keep_threshold =
            newBuilder( "unsupported.dbms.checkpoint_log.rotation.keep.files", INT, 3 ).addConstraint( range( 2, 100 ) ).build();

    @Internal
    @Description( "Whether or not to dump system and database diagnostics. This takes a non-negligible amount of time to do and therefore " +
            "test databases can disable this to reduce startup times" )
    public static final Setting<Boolean> dump_diagnostics = newBuilder( "unsupported.dbms.dump_diagnostics", BOOL, Boolean.TRUE ).build();

    // === SETTINGS FROM FEATURE TOGGLES ===

    @Internal
    @Description( "Validate if transaction statements are properly closed" )
    public static final Setting<Boolean> track_tx_statement_close = newBuilder( "unsupported.dbms.debug.track_tx_statement_close", BOOL, false ).build();

    @Internal
    @Description( "Trace open/close transaction statements" )
    public static final Setting<Boolean> trace_tx_statements = newBuilder( "unsupported.dbms.debug.trace_tx_statement", BOOL, false ).build();

    @Internal
    @Description( "Validate if cursors are properly closed" )
    public static final Setting<Boolean> track_cursor_close = newBuilder( "unsupported.dbms.debug.track_cursor_close", BOOL, false ).build();

    @Internal
    @Description( "Trace unclosed cursors" )
    public static final Setting<Boolean> trace_cursors = newBuilder( "unsupported.dbms.debug.trace_cursors", BOOL, false ).build();

    @Internal
    @Description( "Reporting interval for page cache speed logging" )
    public static final Setting<Duration> page_cache_tracer_speed_reporting_threshold =
            newBuilder( "unsupported.dbms.debug.page_cache_tracer_speed_reporting_threshold", DURATION, ofSeconds( 10 ) ).build();

    @Internal
    @Description( "Logging information about recovered index samples" )
    public static final Setting<Boolean> log_recover_index_samples = newBuilder( "unsupported.dbms.index.sampling.log_recovered_samples", BOOL, false ).build();

    @Internal
    @Description( "Enable asynchronous index sample recovery" )
    public static final Setting<Boolean> async_recover_index_samples = newBuilder( "unsupported.dbms.index.sampling.async_recovery", BOOL, true )
            .immutable().build();

    @Internal
    @Description( "Wait for asynchronous index sample recovery to finish" )
    public static final Setting<Boolean> async_recover_index_samples_wait =
            newBuilder( "unsupported.dbms.index.sampling.async_recovery_wait", BOOL, null )
                    .setDependency( async_recover_index_samples )
                    .build();

    @Internal
    @Description( "Ignore store id validation during recovery" )
    public static final Setting<Boolean> recovery_ignore_store_id_validation =
            newBuilder( "unsupported.dbms.recovery.ignore_store_id_validation", BOOL, false ).build();

    @Internal
    @Description( "Track heap memory allocations for transactions" )
    public static final Setting<Boolean> enable_transaction_heap_allocation_tracking =
            newBuilder( "unsupported.dbms.enable_transaction_heap_allocation_tracking", BOOL, false ).build();

    @Internal
    @Description( "Chunk size for heap memory reservation from the memory pool" )
    public static final Setting<Long> initial_transaction_heap_grab_size =
            newBuilder( "unsupported.dbms.initial_transaction_heap_grab_size", BYTES, mebiBytes( 2 ) ).build();

    @Internal
    @Description( "Default value whether or not to strictly prioritize ids from freelist, as opposed to allocating from high id." +
            "Given a scenario where there are multiple concurrent calls to allocating IDs" +
            "and there are free ids on the freelist, some perhaps cached, some not. Thread noticing that there are no free ids cached will try to acquire" +
            "scanner lock and if it succeeds it will perform a scan and place found free ids in the cache and return. Otherwise:" +
            "   If `false`: thread will allocate from high id and return, to not block id allocation request." +
            "   If `true` : thread will await lock released and check cache afterwards. If no id is cached even then it will allocate from high id." )
    public static final Setting<Boolean> strictly_prioritize_id_freelist =
            newBuilder( "unsupported.dbms.strictly_prioritize_id_freelist", BOOL, true ).build();

    @Internal
    @Description( "Block/buffer size for index population" )
    public static final Setting<Long> index_populator_block_size = newBuilder( "unsupported.dbms.index.populator_block_size", BYTES, mebiBytes( 1 ) )
            .addConstraint( min( 20L ) )
            .addConstraint( max( (long) Integer.MAX_VALUE ) )
            .build();

    @Internal
    @Description( "Merge factory for index population" )
    public static final Setting<Integer> index_populator_merge_factor = newBuilder( "unsupported.dbms.index.populator_merge_factor", INT, 8 ).build();

    @Internal
    @Description( "Enable/disable logging for the id generator" )
    public static final Setting<Boolean> id_generator_log_enabled = newBuilder( "unsupported.dbms.idgenerator.log.enabled", BOOL, false ).build();

    @Internal
    @Description( "Log file rotation threshold for id generator logging" )
    public static final Setting<Long> id_generator_log_rotation_threshold =
            newBuilder( "unsupported.dbms.idgenerator.log.rotation_threshold", BYTES, mebiBytes( 200 ) ).build();

    @Internal
    @Description( "Log file prune threshold for id generator logging" )
    public static final Setting<Duration> id_generator_log_prune_threshold =
            newBuilder( "unsupported.dbms.idgenerator.log.prune_threshold", DURATION, ofDays( 2 ) ).build();

    @Internal
    @Description( "Enable/disable write log for token lookup indexes" )
    public static final Setting<Boolean> token_scan_write_log_enabled = newBuilder( "unsupported.dbms.tokenscan.log.enabled", BOOL, false ).build();

    @Internal
    @Description( "Log file rotation threshold for token lookup index write logging" )
    public static final Setting<Long> token_scan_write_log_rotation_threshold =
            newBuilder( "unsupported.dbms.tokenscan.log.rotation_threshold", BYTES, mebiBytes( 200 ) ).build();

    @Internal
    @Description( "Log file prune threshold for token lookup index write logging" )
    public static final Setting<Duration> token_scan_write_log_prune_threshold =
            newBuilder( "unsupported.dbms.tokenscan.log.prune_threshold", DURATION, ofDays( 2 ) ).build();

    @Internal
    @Description( "Print stack trace on failed native io buffer allocation" )
    public static final Setting<Boolean> print_page_buffer_allocation_trace =
            newBuilder( "unsupported.dbms.debug.print_page_buffer_allocation_trace", BOOL, false ).build();

    @Internal
    @Description( "Printing debug information on index population" )
    public static final Setting<Boolean> index_population_print_debug =
            newBuilder( "unsupported.dbms.index.population_print_debug", BOOL, false ).build();

    @Internal
    @Description( "Queue size for index population batched updates" )
    public static final Setting<Integer> index_population_queue_threshold =
            newBuilder( "unsupported.dbms.index.population_queue_threshold", INT, 20_000 ).build();

    @Internal
    @Description( "Max size for an index population batch" )
    public static final Setting<Long> index_population_batch_max_byte_size =
            newBuilder( "unsupported.dbms.index.population_batch_max_byte_size", BYTES, mebiBytes( 10 ) )
                    .addConstraint( max( (long) Integer.MAX_VALUE ) )
                    .build();

    @Internal
    @Description( "Timeout for configuration command evaluation, per command." )
    public static final Setting<Duration> config_command_evaluation_timeout =
            newBuilder( "unsupported.dbms.config.command_evaluation_timeout", DURATION, ofSeconds( 30 ) ).build();

    @Internal
    @Description( "Whether or not to do additional checks for locks when making changes as part of commit. This may be expensive to enable." )
    public static final Setting<Boolean> additional_lock_verification = newBuilder( "unsupported.dbms.extra_lock_verification", BOOL, false ).build();

    @Internal
    @Description( "Let the IO controller consider/ignore external IO" )
    public static final Setting<Boolean> io_controller_consider_external_io =
            newBuilder( "unsupported.dbms.io.controller.consider.external.enabled", BOOL, false ).dynamic().build();

    @Internal
    @Description( "Whether or not DBMS's byte buffer manager should be used for network stack buffers instead " +
            "of each network library managing its buffers on its own" )
    public static final Setting<Boolean> managed_network_buffers =
            newBuilder( "unsupported.dbms.memory.managed_network_buffers", BOOL, false ).build();

    @Internal
    @Description( "The maximum number of cached entries in count store (based) stores " )
    public static final Setting<Integer> counts_store_max_cached_entries =
            newBuilder( "unsupported.dbms.memory.counts_store_max_cached_entries", INT, 1_000_000 ).build();

    @Internal
    @Description( "Whether or not to use multiple threads whilst performing recovery. Provides performance improvement for some workloads." )
    public static final Setting<Boolean> do_parallel_recovery =
            newBuilder( "unsupported.dbms.recovery.enable_parallelism", BOOL, false ).build();

    @Description( "Name of storage engine to use when creating new databases (except system database). If null or empty string then a default will be used." +
            "This setting will not be used for loading existing databases, where instead the appropriate storage engine for the specific database " +
            "will be used" )
    @Internal
    public static final Setting<String> storage_engine = newBuilder( "unsupported.dbms.storage_engine", STRING, "record" ).build();

    @Internal
    @Description( "Whether or not to log contents of data that is inconsistent when deleting it." )
    public static final Setting<Boolean> log_inconsistent_data_deletion = newBuilder( "dbms.log_inconsistent_data_deletion", BOOL, Boolean.FALSE )
            .dynamic()
            .build();

    @Internal
    @Description( "Whether or database should switch to read only mode on disk space problems." )
    public static final Setting<Boolean> dynamic_read_only_failover = newBuilder( "unsupported.dbms.readonly.failover", BOOL, Boolean.TRUE )
            .dynamic()
            .build();

    @Internal
    @Description( "Feature flag to enable/disable planning use of text indexes." )
    public static final Setting<Boolean> planning_text_indexes_enabled = newBuilder( "unsupported.cypher.planning_text_indexes_enabled", BOOL, true ).build();

    @Internal
    @Description( "Feature flag to enable/disable planning use of range indexes." )
    public static final Setting<Boolean> planning_range_indexes_enabled =
            newBuilder( "unsupported.cypher.planning_range_indexes_enabled", BOOL, false ).build();

    @Internal
    @Description( "Feature flag to enable/disable planning use of point indexes." )
    public static final Setting<Boolean> planning_point_indexes_enabled =
            newBuilder( "unsupported.cypher.planning_point_indexes_enabled", BOOL, false ).build();

    @Internal
    @Description( "Limits the maximum amount of off-heap memory the consistency checker will allocate. The value is given as a factor between 0.1 .. 1 " +
            "and will be multiplied with actual available memory to get the effectively available amount of memory taken into consideration" )
    public static final Setting<Double> consistency_check_memory_limit_factor =
            newBuilder( "unsupported.consistency_checker.memory_limit_factor", DOUBLE, 0.9D )
                    .addConstraint( min( 0.1D ) )
                    .addConstraint( max( 1D ) )
                    .build();

    @Description( "Number of reserved header bytes in each page in page cache. Please note changing it for already existing store is not supported." )
    public static final Setting<Integer> reserved_page_header_bytes = newBuilder( "unsupported.dbms.reserved.page.header.bytes", INT, 0 ).build();

    @Description( "Allow database to use dedicated transaction appender writer thread." )
    public static final Setting<Boolean> dedicated_transaction_appender =
            newBuilder( "unsupported.dbms.tx.logs.dedicated.appender", BOOL, Boolean.FALSE ).build();

    @Internal
    @Description( "Enable per page file metrics collection in a default page cache and cursor tracer." )
    public static final Setting<Boolean> per_file_metrics_counters = newBuilder( "unsupported.dbms.page.file.tracer", BOOL, false ).build();

    @Internal
    @Description( "Enables legacy strategy for loading pages from a profile." +
            " This strategy uses an aggressive per-file parallelism which turns what is mostly sequential IO into random IO." +
            " As a result, in most environments, this strategy is slower and stresses the IO subsystem more than the default strategy." )
    public static final Setting<Boolean> pagecache_warmup_legacy_profile_loader =
            newBuilder( "unsupported.dbms.memory.pagecache.warmup.legacy_profile_loader", BOOL, false ).build();

    @Internal
    @Description( "Enables blocking Page Cache warmup. Database start will be blocked until warmer is completed." )
    public static final Setting<Boolean> pagecache_warmup_blocking =
            newBuilder( "unsupported.dbms.memory.pagecache.warmup.blocking", BOOL, false ).build();

    @Internal
    @Description( "Enables sketching of next transaction log file in the background during reverse recovery." )
    public static final Setting<Boolean> pre_sketch_transaction_logs = newBuilder( "unsupported.dbms.tx_log.presketch", BOOL, false ).build();

    @Internal
    @Description( "Maximum size after which the planner will not attempt to plan the disjunction of predicates on a single variable as a distinct union." +
              "For example, given the following pattern: `()-[e:FOO|BAR|BAZ]->()`, the planner will attempt to plan a union of `e:Foo`, `e:Bar`, and `e:Baz`" +
              "unless `unsupported.cypher.predicates_as_union_max_size` is less than 3." )
    public static final Setting<Integer> predicates_as_union_max_size =
            newBuilder( "unsupported.cypher.predicates_as_union_max_size", INT, 255 )
                    .addConstraint( min( 0 ) )
                    .build();

}
