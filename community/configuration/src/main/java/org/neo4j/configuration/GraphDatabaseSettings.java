/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogTimeZone;

import static java.time.Duration.ofHours;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static java.util.Collections.emptyList;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation.OFF_HEAP;
import static org.neo4j.configuration.SettingConstraints.ABSOLUTE_PATH;
import static org.neo4j.configuration.SettingConstraints.HOSTNAME_ONLY;
import static org.neo4j.configuration.SettingConstraints.POWER_OF_2;
import static org.neo4j.configuration.SettingConstraints.any;
import static org.neo4j.configuration.SettingConstraints.is;
import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.configuration.SettingValueParsers.DATABASENAME;
import static org.neo4j.configuration.SettingValueParsers.DOUBLE;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.LONG;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.TIMEZONE;
import static org.neo4j.configuration.SettingValueParsers.listOf;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;

@ServiceProvider
@PublicApi
public class GraphDatabaseSettings implements SettingsDeclaration
{
    /**
     * Data block sizes for dynamic array stores.
     */
    public static final int DEFAULT_BLOCK_SIZE = 128;
    public static final int DEFAULT_LABEL_BLOCK_SIZE = 64;
    public static final int MINIMAL_BLOCK_SIZE = 16;

    // default unspecified transaction timeout
    public static final long UNSPECIFIED_TIMEOUT = 0L;

    public static final String SYSTEM_DATABASE_NAME = "system";
    public static final String DEFAULT_DATABASE_NAME = "neo4j";

    public static final String DEFAULT_DATA_DIR_NAME = "data";
    public static final String DEFAULT_DATABASES_ROOT_DIR_NAME = "databases";
    public static final String DEFAULT_TX_LOGS_ROOT_DIR_NAME = "transactions";

    @Description( "Root relative to which directory settings are resolved." )
    @DocumentedDefaultValue( "Defaults to current working directory" )
    public static final Setting<Path> neo4j_home = newBuilder( "dbms.directories.neo4j_home", PATH, Path.of( "" ).toAbsolutePath() )
            .addConstraint( ABSOLUTE_PATH )
            .immutable()
            .build();

    @Description( "Name of the default database." )
    public static final Setting<String> default_database =
            newBuilder( "dbms.default_database", DATABASENAME, DEFAULT_DATABASE_NAME ).build();

    @Description( "Path of the data directory. You must not configure more than one Neo4j installation to use the " +
            "same data directory." )
    public static final Setting<Path> data_directory = newBuilder( "dbms.directories.data", PATH, Path.of( DEFAULT_DATA_DIR_NAME ) )
            .setDependency( neo4j_home )
            .immutable()
            .build();

    @Internal
    public static final Setting<Path> databases_root_path =
            newBuilder( "unsupported.dbms.directories.databases.root", PATH, Path.of( DEFAULT_DATABASES_ROOT_DIR_NAME ) )
                    .setDependency( data_directory ).immutable().build();

    @Deprecated
    @Internal
    @Description( "Location where Neo4j keeps the logical transaction logs." )
    public static final Setting<Path> logical_logs_location = newBuilder( "dbms.directories.tx_log", PATH, Path.of( DEFAULT_DATABASE_NAME ) )
            .setDependency( databases_root_path )
            .build();

    @Description( "Root location where Neo4j will store transaction logs for configured databases." )
    public static final Setting<Path> transaction_logs_root_path =
            newBuilder( "dbms.directories.transaction.logs.root", PATH, Path.of( DEFAULT_TX_LOGS_ROOT_DIR_NAME ) )
                    .setDependency( data_directory ).immutable().build();

    @Description( "Only allow read operations from this Neo4j instance. " +
            "This mode still requires write access to the directory for lock purposes." )
    public static final Setting<Boolean> read_only = newBuilder( "dbms.read_only", BOOL, false ).build();

    @Internal
    @Description( "Configure lucene to be in memory only, for test environment. This is set in code and should never be configured explicitly." )
    public static final Setting<Boolean> ephemeral_lucene = newBuilder( "unsupported.dbms.lucene.ephemeral", BOOL, false ).build();

    @Internal
    public static final Setting<String> lock_manager = newBuilder( "unsupported.dbms.lock_manager", STRING, "" ).build();

    @Internal
    public static final Setting<String> tracer = newBuilder( "unsupported.dbms.tracer", STRING, null ).build();

    @Description( "Print out the effective Neo4j configuration after startup." )
    @Internal
    public static final Setting<Boolean> dump_configuration = newBuilder( "unsupported.dbms.report_configuration", BOOL, false )
            .build();

    @Description( "A strict configuration validation will prevent the database from starting up if unknown " +
            "configuration options are specified in the neo4j settings namespace (such as dbms., cypher., etc)." )
    public static final Setting<Boolean> strict_config_validation = newBuilder( "dbms.config.strict_validation", BOOL, false ).build();

    @Description( "Whether to allow an upgrade in case the current version of the database starts against an older version." )
    public static final Setting<Boolean> allow_upgrade = newBuilder( "dbms.allow_upgrade", BOOL, false ).dynamic().build();

    @Description( "Max number of processors used when upgrading the store. Defaults to the number of processors available to the JVM. " +
            "There is a certain amount of minimum threads needed so for that reason there is no lower bound for this " +
            "value. For optimal performance this value shouldn't be greater than the number of available processors." )
    public static final Setting<Integer> upgrade_processors = newBuilder( "dbms.upgrade_max_processors", INT, 0 ).addConstraint( min( 0 ) ).dynamic().build();

    @Description( "Database record format. Valid values: `standard`, `high_limit`. " +
            "The `high_limit` format is available for Enterprise Edition only. " +
            "It is required if you have a graph that is larger than 34 billion nodes, 34 billion relationships, or 68 billion properties. " +
            "A change of the record format is irreversible. " +
            "Certain operations may suffer from a performance penalty of up to 10%, which is why this format is not switched on by default." )
    public static final Setting<String> record_format = newBuilder( "dbms.record_format", STRING, "" ).build();

    // Cypher settings

    public enum CypherParserVersion
    {
        DEFAULT( "default" ), V_35( "3.5" ), V_40( "4.0" ), V_41( "4.1" );

        private final String name;

        CypherParserVersion( String name )
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    @Description( "Set this to specify the default parser (language version)." )
    public static final Setting<CypherParserVersion> cypher_parser_version =
            newBuilder( "cypher.default_language_version", ofEnum( CypherParserVersion.class ), CypherParserVersion.DEFAULT ).build();

    public enum CypherPlanner
    {
        DEFAULT, COST
    }
    @Description( "Set this to specify the default planner for the default language version." )
    public static final Setting<CypherPlanner> cypher_planner =
            newBuilder( "cypher.planner", ofEnum( CypherPlanner.class ), CypherPlanner.DEFAULT ).build();

    @Description( "Set this to specify the behavior when Cypher planner or runtime hints cannot be fulfilled. "
            + "If true, then non-conformance will result in an error, otherwise only a warning is generated." )
    public static final Setting<Boolean> cypher_hints_error = newBuilder( "cypher.hints_error", BOOL, false ).build();

    @Description( "This setting is associated with performance optimization. Set this to `true` in situations where " +
            "it is preferable to have any queries using the 'shortestPath' function terminate as soon as " +
            "possible with no answer, rather than potentially running for a long time attempting to find an " +
            "answer (even if there is no path to be found). " +
            "For most queries, the 'shortestPath' algorithm will return the correct answer very quickly. However " +
            "there are some cases where it is possible that the fast bidirectional breadth-first search " +
            "algorithm will find no results even if they exist. This can happen when the predicates in the " +
            "`WHERE` clause applied to 'shortestPath' cannot be applied to each step of the traversal, and can " +
            "only be applied to the entire path. When the query planner detects these special cases, it will " +
            "plan to perform an exhaustive depth-first search if the fast algorithm finds no paths. However, " +
            "the exhaustive search may be orders of magnitude slower than the fast algorithm. If it is critical " +
            "that queries terminate as soon as possible, it is recommended that this option be set to `true`, " +
            "which means that Neo4j will never consider using the exhaustive search for shortestPath queries. " +
            "However, please note that if no paths are found, an error will be thrown at run time, which will " +
            "need to be handled by the application." )
    public static final Setting<Boolean> forbid_exhaustive_shortestpath =
            newBuilder( "cypher.forbid_exhaustive_shortestpath", BOOL, false ).build();

    @Description( "This setting is associated with performance optimization. The shortest path algorithm does not " +
            "work when the start and end nodes are the same. With this setting set to `false` no path will " +
            "be returned when that happens. The default value of `true` will instead throw an exception. " +
            "This can happen if you perform a shortestPath search after a cartesian product that might have " +
            "the same start and end nodes for some of the rows passed to shortestPath. If it is preferable " +
            "to not experience this exception, and acceptable for results to be missing for those rows, then " +
            "set this to `false`. If you cannot accept missing results, and really want the shortestPath " +
            "between two common nodes, then re-write the query using a standard Cypher variable length pattern " +
            "expression followed by ordering by path length and limiting to one result." )
    public static final Setting<Boolean> forbid_shortestpath_common_nodes =
            newBuilder( "cypher.forbid_shortestpath_common_nodes", BOOL, true ).build();

    @Description( "Set this to change the behavior for Cypher create relationship when the start or end node is missing. " +
            "By default this fails the query and stops execution, but by setting this flag the create operation is " +
            "simply not performed and execution continues." )
    public static final Setting<Boolean> cypher_lenient_create_relationship =
            newBuilder( "cypher.lenient_create_relationship", BOOL, false ).build();

    public enum CypherRuntime
    {
        DEFAULT, INTERPRETED, COMPILED, SLOTTED, PIPELINED
    }
    @Description( "Set this to specify the default runtime for the default language version." )
    @Internal
    public static final Setting<CypherRuntime> cypher_runtime =
            newBuilder( "unsupported.cypher.runtime", ofEnum( CypherRuntime.class ), CypherRuntime.DEFAULT ).build();

    public enum CypherExpressionEngine
    {
        DEFAULT, INTERPRETED, COMPILED, ONLY_WHEN_HOT
    }
    @Description( "Choose the expression engine. The default is to only compile expressions that are hot, if 'COMPILED' " +
            "is chosen all expressions will be compiled directly and if 'INTERPRETED' is chosen expressions will " +
            "never be compiled." )
    @Internal
    public static final Setting<CypherExpressionEngine> cypher_expression_engine =
            newBuilder( "unsupported.cypher.expression_engine", ofEnum( CypherExpressionEngine.class ), CypherExpressionEngine.DEFAULT  ).build();

    @Description( "Number of uses before an expression is considered for compilation" )
    @Internal
    public static final Setting<Integer> cypher_expression_recompilation_limit = newBuilder( "unsupported.cypher.expression_recompilation_limit", INT, 10 )
            .addConstraint( min( 0 ) )
            .build();

    @Description( "Enable tracing of compilation in cypher." )
    @Internal
    public static final Setting<Boolean> cypher_compiler_tracing =
            newBuilder( "unsupported.cypher.compiler_tracing", BOOL, false ).build();

    @Description( "The number of Cypher query execution plans that are cached." )
    public static final Setting<Integer> query_cache_size =
            newBuilder( "dbms.query_cache_size", INT, 1000 ).addConstraint( min( 0 ) ).build();

    @Description( "The threshold when a plan is considered stale. If any of the underlying " +
            "statistics used to create the plan have changed more than this value, " +
            "the plan will be considered stale and will be replanned. Change is calculated as " +
            "abs(a-b)/max(a,b). This means that a value of 0.75 requires the database to approximately " +
            "quadruple in size. A value of 0 means replan as soon as possible, with the soonest being " +
            "defined by the cypher.min_replan_interval which defaults to 10s. After this interval the " +
            "divergence threshold will slowly start to decline, reaching 10% after about 7h. This will " +
            "ensure that long running databases will still get query replanning on even modest changes, " +
            "while not replanning frequently unless the changes are very large." )
    public static final Setting<Double> query_statistics_divergence_threshold =
            newBuilder( "cypher.statistics_divergence_threshold", DOUBLE, 0.75 ).addConstraint( range( 0.0, 1.0 ) ).build();

    @Description( "Large databases might change slowly, and so to prevent queries from never being replanned " +
            "the divergence threshold set by cypher.statistics_divergence_threshold is configured to " +
            "shrink over time. " +
            "The algorithm used to manage this change is set by unsupported.cypher.replan_algorithm " +
            "and will cause the threshold to reach the value set here once the time since the previous " +
            "replanning has reached unsupported.cypher.target_replan_interval. " +
            "Setting this value to higher than the cypher.statistics_divergence_threshold will cause the " +
            "threshold to not decay over time." )
    @Internal
    public static final Setting<Double> query_statistics_divergence_target =
            newBuilder( "unsupported.cypher.statistics_divergence_target", DOUBLE, 0.10 ).addConstraint( range( 0.0, 1.0 ) ).build();

    @Description( "The threshold when a warning is generated if a label scan is done after a load csv " +
            "where the label has no index" )
    @Internal
    public static final Setting<Long> query_non_indexed_label_warning_threshold =
            newBuilder( "unsupported.cypher.non_indexed_label_warning_threshold", LONG, 10000L ).build();

    @Description( "To improve IDP query planning time, we can restrict the internal planning table size, " +
            "triggering compaction of candidate plans. The smaller the threshold the faster the planning, " +
            "but the higher the risk of sub-optimal plans." )
    @Internal
    public static final Setting<Integer> cypher_idp_solver_table_threshold =
            newBuilder( "unsupported.cypher.idp_solver_table_threshold", INT, 128 ).addConstraint( min( 16 ) ).build();

    @Description( "To improve IDP query planning time, we can restrict the internal planning loop duration, " +
            "triggering more frequent compaction of candidate plans. The smaller the threshold the " +
            "faster the planning, but the higher the risk of sub-optimal plans." )
    @Internal
    public static final Setting<Long> cypher_idp_solver_duration_threshold =
            newBuilder( "unsupported.cypher.idp_solver_duration_threshold", LONG, 1000L ).addConstraint( min( 10L ) ).build();

    @Description( "The minimum time between possible cypher query replanning events. After this time, the graph " +
            "statistics will be evaluated, and if they have changed by more than the value set by " +
            "cypher.statistics_divergence_threshold, the query will be replanned. If the statistics have " +
            "not changed sufficiently, the same interval will need to pass before the statistics will be " +
            "evaluated again. Each time they are evaluated, the divergence threshold will be reduced slightly " +
            "until it reaches 10% after 7h, so that even moderately changing databases will see query replanning " +
            "after a sufficiently long time interval." )
    public static final Setting<Duration> cypher_min_replan_interval =
            newBuilder( "cypher.min_replan_interval", DURATION, ofSeconds( 10 ) ).build();

    @Description( "Large databases might change slowly, and to prevent queries from never being replanned " +
            "the divergence threshold set by cypher.statistics_divergence_threshold is configured to " +
            "shrink over time. The algorithm used to manage this change is set by " +
            "unsupported.cypher.replan_algorithm and will cause the threshold to reach " +
            "the value set by unsupported.cypher.statistics_divergence_target once the time since the " +
            "previous replanning has reached the value set here. Setting this value to less than the " +
            "value of cypher.min_replan_interval will cause the threshold to not decay over time." )
    @Internal
    public static final Setting<Duration> cypher_replan_interval_target =
            newBuilder( "unsupported.cypher.target_replan_interval", DURATION, Duration.ofHours( 7 ) ).build();

    public enum CypherReplanAlgorithm
    {
        DEFAULT, NONE, INVERSE, EXPONENTIAL
    }
    @Description( "Large databases might change slowly, and to prevent queries from never being replanned " +
            "the divergence threshold set by cypher.statistics_divergence_threshold is configured to " +
            "shrink over time using the algorithm set here. This will cause the threshold to reach " +
            "the value set by unsupported.cypher.statistics_divergence_target once the time since the " +
            "previous replanning has reached the value set in unsupported.cypher.target_replan_interval. " +
            "Setting the algorithm to 'none' will cause the threshold to not decay over time." )
    @Internal
    public static final Setting<CypherReplanAlgorithm> cypher_replan_algorithm =
            newBuilder( "unsupported.cypher.replan_algorithm", ofEnum( CypherReplanAlgorithm.class ), CypherReplanAlgorithm.DEFAULT ).build();

    @Description( "Set this to enable monitors in the Cypher runtime." )
    @Internal
    public static final Setting<Boolean> cypher_enable_runtime_monitors =
            newBuilder( "unsupported.cypher.enable_runtime_monitors", BOOL, false ).build();

    @Description( "Determines if Cypher will allow using file URLs when loading data using `LOAD CSV`. Setting this "
            + "value to `false` will cause Neo4j to fail `LOAD CSV` clauses that load data from the file system." )
    public static final Setting<Boolean> allow_file_urls =
            newBuilder( "dbms.security.allow_csv_import_from_file_urls", BOOL, true ).build();

    @Description( "Sets the root directory for file URLs used with the Cypher `LOAD CSV` clause. This should be set to a " +
            "directory relative to the Neo4j installation path, restricting access to only those files within that directory " +
            "and its subdirectories. For example the value \"import\" will only enable access to files within the 'import' folder. " +
            "Removing this setting will disable the security feature, allowing all files in the local system to be imported. " +
            "Setting this to an empty field will allow access to all files within the Neo4j installation folder." )
    public static final Setting<Path> load_csv_file_url_root =
            newBuilder( "dbms.directories.import", PATH, null ).immutable().setDependency( neo4j_home ).build();

    @Description( "Selects whether to conform to the standard https://tools.ietf.org/html/rfc4180 for interpreting " +
            "escaped quotation characters in CSV files loaded using `LOAD CSV`. Setting this to `false` will use" +
            " the standard, interpreting repeated quotes '\"\"' as a single in-lined quote, while `true` will " +
            "use the legacy convention originally supported in Neo4j 3.0 and 3.1, allowing a backslash to " +
            "include quotes in-lined in fields." )
    public static final Setting<Boolean> csv_legacy_quote_escaping =
            newBuilder( "dbms.import.csv.legacy_quote_escaping", BOOL, true ).build();

    @Description( "The size of the internal buffer in bytes used by `LOAD CSV`. If the csv file contains huge fields " +
            "this value may have to be increased." )
    public static final Setting<Long> csv_buffer_size =
            newBuilder( "dbms.import.csv.buffer_size", LONG, mebiBytes( 2 ) ).addConstraint( min( 1L ) ).build();

    @Description( "Enables or disables tracking of how much time a query spends actively executing on the CPU. " +
            "Calling `dbms.listQueries` will display the time. " +
            "This can also be logged in the query log by using `dbms.logs.query.time_logging_enabled`." )
    public static final Setting<Boolean> track_query_cpu_time =
            newBuilder( "dbms.track_query_cpu_time", BOOL, false ).dynamic().build();

    @Description( "Enables or disables tracking of how many bytes are allocated by the execution of a query. " +
                  "If enabled, calling `dbms.listQueries` will display the allocated bytes. " +
                  "This can also be logged in the query log by using `dbms.logs.query.allocation_logging_enabled`." )
    public static final Setting<Boolean> track_query_allocation =
            newBuilder( "dbms.track_query_allocation", BOOL, true ).dynamic().build();

    @Description( "Enable tracing of pipelined runtime scheduler." )
    @Internal
    public static final Setting<Boolean> enable_pipelined_runtime_trace =
            newBuilder( "unsupported.cypher.pipelined.enable_runtime_trace", BOOL, false ).build();

    @Description( "Path to the pipelined runtime scheduler trace. If 'stdOut' and tracing is on, will print to std out." )
    @Internal
    public static final Setting<Path> pipelined_scheduler_trace_filename =
            newBuilder( "unsupported.cypher.pipelined.runtime_trace_path", PATH, Path.of( "stdOut" ) ).setDependency( neo4j_home ).immutable().build();

    @Description( "The size of batches in the pipelined runtime for queries which work with few rows." )
    @Internal
    public static final Setting<Integer> cypher_pipelined_batch_size_small =
            newBuilder( "unsupported.cypher.pipelined.batch_size_small", INT, 128 ).addConstraint( min( 1 ) ).build();

    @Description( "The size of batches in the pipelined runtime for queries which work with many rows." )
    @Internal
    public static final Setting<Integer> cypher_pipelined_batch_size_big =
            newBuilder( "unsupported.cypher.pipelined.batch_size_big", INT, 1024 ).addConstraint( min( 1 ) ).build();

    @Description( "Number of threads to allocate to Cypher worker threads for the parallel runtime. If set to 0, two workers will be started" +
                  " for every physical core in the system. If set to -1, no workers will be started and the parallel runtime cannot be used." )
    @Internal
    public static final Setting<Integer> cypher_worker_count = newBuilder( "unsupported.cypher.number_of_workers", INT, 0 ).build();

    public enum CypherOperatorEngine
    {
        COMPILED,
        INTERPRETED
    }

    @Description( "For compiled execution, specialized code is generated and then executed. " +
                  "More optimizations such as operator fusion may apply. " +
                  "Operator fusion means that multiple operators such as for example " +
                  "AllNodesScan -> Filter -> ProduceResult can be compiled into a single specialized operator. " +
                  "This setting only applies to the pipelined and parallel runtime. " +
                  "Allowed values are \"COMPILED\" (default) and \"INTERPRETED\"." )
    @Internal
    public static final Setting<CypherOperatorEngine> cypher_operator_engine =
            newBuilder( "unsupported.cypher.pipelined.operator_engine", ofEnum( CypherOperatorEngine.class ), CypherOperatorEngine.COMPILED ).build();

    @Description( "Use interpreted pipes as a fallback for operators that do not have a specialized implementation in the pipelined runtime. " +
                  "Allowed values are \"disabled\", \"default\" (the default) and \"all\" (experimental). " +
                  "The default is to enable the use of a subset of whitelisted operators that are known to be supported, whereas \"all\" is an " +
                  "experimental option that enables the fallback to be used for all possible operators that are not known to be unsupported." )
    @Internal
    public static final Setting<CypherPipelinedInterpretedPipesFallback> cypher_pipelined_interpreted_pipes_fallback =
            newBuilder( "unsupported.cypher.pipelined_interpreted_pipes_fallback", ofEnum( CypherPipelinedInterpretedPipesFallback.class ),
                    CypherPipelinedInterpretedPipesFallback.DEFAULT ).build();

    public enum CypherPipelinedInterpretedPipesFallback
    {
        DISABLED, DEFAULT, ALL
    }

    @Description( "Max number of recent queries to collect in the data collector module. Will round down to the" +
            " nearest power of two. The default number (8192 query invocations) " +
            " was chosen as a trade-off between getting a useful amount of queries, and not" +
            " wasting too much heap. Even with a buffer full of unique queries, the estimated" +
            " footprint lies in tens of MBs. If the buffer is full of cached queries, the" +
            " retained size was measured to 265 kB. Setting this to 0 will disable data collection" +
            " of queries completely." )
    @Internal
    public static final Setting<Integer> data_collector_max_recent_query_count =
            newBuilder( "unsupported.datacollector.max_recent_query_count", INT, 8192 ).addConstraint( min( 0 ) ).build();

    @Description( "Sets the upper limit for how much of the query text that will be retained by the query collector." +
            " For queries longer than the limit, only a prefix of size limit will be retained by the collector." +
            " Lowering this value will reduce the memory footprint of collected query invocations under loads with" +
            " many queries with long query texts, which could occur for generated queries. The downside is that" +
            " on retrieving queries by `db.stats.retrieve`, queries longer than this max size would be returned" +
            " incomplete. Setting this to 0 will completely drop query texts from the collected queries." )
    @Internal
    public static final Setting<Integer> data_collector_max_query_text_size =
            newBuilder( "unsupported.datacollector.max_query_text_size", INT, 10000 ).addConstraint( min( 0 ) ).build();

    @Description( "The maximum amount of time to wait for the database to become available, when " +
            "starting a new transaction." )
    @Internal
    public static final Setting<Duration> transaction_start_timeout =
            newBuilder( "unsupported.dbms.transaction_start_timeout", DURATION, ofSeconds( 1 ) ).build();

    @Description( "The maximum number of concurrently running transactions. If set to 0, limit is disabled." )
    public static final Setting<Integer> max_concurrent_transactions =
            newBuilder( "dbms.transaction.concurrent.maximum", INT, 1000 ).dynamic().build();

    public enum TransactionTracingLevel
    {
        DISABLED, SAMPLE, ALL
    }

    @Description( "Transaction creation tracing level." )
    public static final Setting<TransactionTracingLevel> transaction_tracing_level =
            newBuilder( "dbms.transaction.tracing.level", ofEnum( TransactionTracingLevel.class ),TransactionTracingLevel.DISABLED ).dynamic().build();

    @Description( "Transaction sampling percentage." )
    public static final Setting<Integer> transaction_sampling_percentage =
            newBuilder( "dbms.transaction.sampling.percentage", INT, 5 ).dynamic().addConstraint( range( 1, 100 ) ).build();

    // @see Status.Transaction#TransactionTimedOut
    @Description( "The maximum time interval of a transaction within which it should be completed." )
    public static final Setting<Duration> transaction_timeout =
            newBuilder( "dbms.transaction.timeout", DURATION, Duration.ZERO ).dynamic().build();

    // @see Status.Transaction#LockAcquisitionTimeout
    @Description( "The maximum time interval within which lock should be acquired." )
    public static final Setting<Duration> lock_acquisition_timeout =
            newBuilder( "dbms.lock.acquisition.timeout", DURATION, Duration.ZERO ).build();

    @Description( "Configures the time interval between transaction monitor checks. Determines how often " +
            "monitor thread will check transaction for timeout." )
    public static final Setting<Duration> transaction_monitor_check_interval =
            newBuilder( "dbms.transaction.monitor.check.interval", DURATION, ofSeconds( 2 ) ).build();

    @Description( "The maximum amount of time to wait for running transactions to complete before allowing "
            + "initiated database shutdown to continue" )
    public static final Setting<Duration> shutdown_transaction_end_timeout =
            newBuilder( "dbms.shutdown_transaction_end_timeout", DURATION, ofSeconds( 10 ) ).build();

    @Description( "Location of the database plugin directory. Compiled Java JAR files that contain database " +
            "procedures will be loaded if they are placed in this directory." )
    public static final Setting<Path> plugin_dir = newBuilder( "dbms.directories.plugins", PATH, Path.of("plugins" ) )
            .setDependency( neo4j_home )
            .immutable()
            .build();

    @Internal
    @Description( "Location of the database scripts directory." )
    public static final Setting<Path> scripts_dir = newBuilder( "unsupported.dbms.directories.scripts", PATH, Path.of("scripts" ) )
            .setDependency( neo4j_home )
            .immutable()
            .build();

    @Description( "Name of file containing commands to be run during initialization of the system database. " +
                  "The file should exists in the scripts directory in neo4j home directory." )
    public static final Setting<Path> system_init_file =
            newBuilder( "dbms.init_file", PATH, null ).immutable().setDependency( scripts_dir ).build();

    @Description( "Threshold for rotation of the user log. If set to 0 log rotation is disabled." )
    public static final Setting<Long> store_user_log_rotation_threshold =
            newBuilder( "dbms.logs.user.rotation.size", BYTES, 0L ).addConstraint( range( 0L, Long.MAX_VALUE ) ).build();

    @Description( "Threshold for rotation of the debug log." )
    public static final Setting<Long> store_internal_log_rotation_threshold =
            newBuilder( "dbms.logs.debug.rotation.size", BYTES, mebiBytes( 20 ) ).addConstraint( range( 0L, Long.MAX_VALUE ) ).build();

    @Description( "Debug log contexts that should output debug level logging" )
    @Internal
    public static final Setting<List<String>> store_internal_debug_contexts =
            newBuilder( "unsupported.dbms.logs.debug.debug_loggers", listOf( STRING ), List.of( "org.neo4j.diagnostics" ) ).dynamic().build();

    @Description( "Debug log level threshold." )
    public static final Setting<Level> store_internal_log_level =
            newBuilder( "dbms.logs.debug.level", ofEnum( Level.class ), Level.INFO ).dynamic().build();

    @Description( "Database timezone. Among other things, this setting influences which timezone the logs and monitoring procedures use." )
    public static final Setting<LogTimeZone> db_timezone =
            newBuilder( "dbms.db.timezone", ofEnum( LogTimeZone.class ), LogTimeZone.UTC ).build();

    @Description( "Database timezone for temporal functions. All Time and DateTime values that are created without " +
            "an explicit timezone will use this configured default timezone." )
    public static final Setting<ZoneId> db_temporal_timezone = newBuilder( "db.temporal.timezone", TIMEZONE, ZoneOffset.UTC ).build();

    @Description( "Maximum time to wait for active transaction completion when rotating counts store" )
    @Internal
    public static final Setting<Duration> counts_store_rotation_timeout =
            newBuilder( "unsupported.dbms.counts_store_rotation_timeout", DURATION, ofMinutes( 10 ) ).build();

    @Description( "Minimum time interval after last rotation of the user log before it may be rotated again." )
    public static final Setting<Duration> store_user_log_rotation_delay =
            newBuilder( "dbms.logs.user.rotation.delay", DURATION, ofSeconds( 300 ) ).build();

    @Description( "Minimum time interval after last rotation of the debug log before it may be rotated again." )
    public static final Setting<Duration> store_internal_log_rotation_delay =
            newBuilder( "dbms.logs.debug.rotation.delay", DURATION, ofSeconds( 300 ) ).build();

    @Description( "Maximum number of history files for the user log." )
    public static final Setting<Integer> store_user_log_max_archives =
            newBuilder( "dbms.logs.user.rotation.keep_number", INT, 7 ).addConstraint( min( 1 ) ).build();

    @Description( "Maximum number of history files for the debug log." )
    public static final Setting<Integer> store_internal_log_max_archives =
            newBuilder( "dbms.logs.debug.rotation.keep_number", INT, 7 ).addConstraint( min( 1 ) ).build();

    public enum CheckpointPolicy
    {
        PERIODIC, CONTINUOUS, VOLUMETRIC
    }
    @Description( "Configures the general policy for when check-points should occur. The default policy is the " +
            "'periodic' check-point policy, as specified by the 'dbms.checkpoint.interval.tx' and " +
            "'dbms.checkpoint.interval.time' settings. " +
            "The Neo4j Enterprise Edition provides two alternative policies: " +
            "The first is the 'continuous' check-point policy, which will ignore those settings and run the " +
            "check-point process all the time. " +
            "The second is the 'volumetric' check-point policy, which makes a best-effort at check-pointing " +
            "often enough so that the database doesn't get too far behind on deleting old transaction logs in " +
            "accordance with the 'dbms.tx_log.rotation.retention_policy' setting." )
    public static final Setting<CheckpointPolicy> check_point_policy =
            newBuilder( "dbms.checkpoint", ofEnum( CheckpointPolicy.class ), CheckpointPolicy.PERIODIC ).build();

    @Description( "Configures the transaction interval between check-points. The database will not check-point more " +
            "often  than this (unless check pointing is triggered by a different event), but might check-point " +
            "less often than this interval, if performing a check-point takes longer time than the configured " +
            "interval. A check-point is a point in the transaction logs, from which recovery would start from. " +
            "Longer check-point intervals typically means that recovery will take longer to complete in case " +
            "of a crash. On the other hand, a longer check-point interval can also reduce the I/O load that " +
            "the database places on the system, as each check-point implies a flushing and forcing of all the " +
            "store files.  The default is '100000' for a check-point every 100000 transactions." )
    public static final Setting<Integer> check_point_interval_tx =
            newBuilder( "dbms.checkpoint.interval.tx", INT, 100000 ).addConstraint( min( 1 ) ).build();

    @Description( "Configures the time interval between check-points. The database will not check-point more often " +
            "than this (unless check pointing is triggered by a different event), but might check-point less " +
            "often than this interval, if performing a check-point takes longer time than the configured " +
            "interval. A check-point is a point in the transaction logs, from which recovery would start from. " +
            "Longer check-point intervals typically means that recovery will take longer to complete in case " +
            "of a crash. On the other hand, a longer check-point interval can also reduce the I/O load that " +
            "the database places on the system, as each check-point implies a flushing and forcing of all the " +
            "store files." )
    public static final Setting<Duration> check_point_interval_time =
            newBuilder( "dbms.checkpoint.interval.time", DURATION, ofMinutes( 15 ) ).build();

    @Description( "Limit the number of IOs the background checkpoint process will consume per second. " +
            "This setting is advisory, is ignored in Neo4j Community Edition, and is followed to " +
            "best effort in Enterprise Edition. " +
            "An IO is in this case a 8 KiB (mostly sequential) write. Limiting the write IO in " +
            "this way will leave more bandwidth in the IO subsystem to service random-read IOs, " +
            "which is important for the response time of queries when the database cannot fit " +
            "entirely in memory. The only drawback of this setting is that longer checkpoint times " +
            "may lead to slightly longer recovery times in case of a database or system crash. " +
            "A lower number means lower IO pressure, and consequently longer checkpoint times. " +
            "Set this to -1 to disable the IOPS limit and remove the limitation entirely; " +
            "this will let the checkpointer flush data as fast as the hardware will go. "  +
            "Removing the setting, or commenting it out, will set the default value of 300." )
    public static final Setting<Integer> check_point_iops_limit =
            newBuilder( "dbms.checkpoint.iops.limit", INT, 300 ).dynamic().build();

    // Index sampling
    @Description( "Enable or disable background index sampling" )
    public static final Setting<Boolean> index_background_sampling_enabled =
            newBuilder( "dbms.index_sampling.background_enabled", BOOL, true ).build();

    @Description( "Index sampling chunk size limit" )
    public static final Setting<Integer> index_sample_size_limit = newBuilder( "dbms.index_sampling.sample_size_limit", INT, (int) mebiBytes( 8 ) )
            .addConstraint( range( (int) mebiBytes( 1 ), Integer.MAX_VALUE ) )
            .build();

    @Description( "Percentage of index updates of total index size required before sampling of a given index is " +
            "triggered" )
    public static final Setting<Integer> index_sampling_update_percentage =
            newBuilder( "dbms.index_sampling.update_percentage", INT, 5 ).addConstraint( min( 0 ) ).build();

    @Description( "Set the maximum number of threads that can concurrently be used to sample indexes. Zero means unrestricted." )
    @Internal
    public static final Setting<Integer> index_sampling_parallelism =
            newBuilder( "unsupported.dbms.index_sampling.parallelism", INT, 4 ).addConstraint( min( 0 ) ).build();

    @Description( "Set the maximum number of concurrent index populations across system. " +
            "This also limit the number of threads used to scan store. " +
            "Note that multiple indexes can be populated by a single index population if they were created in the same transaction. " +
            "Zero means unrestricted. " )
    @Internal
    public static final Setting<Integer> index_population_parallelism =
            newBuilder( "unsupported.dbms.index_population.parallelism", INT, 4 ).addConstraint( min( 0 ) ).build();

    @Description( "Set the number of threads used for index population work. " +
            "Those threads execute individual subtasks provided by index population main threads, see unsupported.dbms.index_population.parallelism." +
            "Zero means one thread per cpu core." )
    @Internal
    public static final Setting<Integer> index_population_workers =
            newBuilder( "unsupported.dbms.index_population.workers", INT, 8 ).addConstraint( min( 0 ) ).build();

    // Lucene settings
    @Deprecated( since = "4.0.0", forRemoval = true )
    @Description( "The maximum number of open Lucene index searchers." )
    public static final Setting<Integer> lucene_searcher_cache_size =
            newBuilder( "dbms.index_searcher_cache_size", INT, Integer.MAX_VALUE ).addConstraint( min( 1 ) ).build();

    // Lucene schema indexes
    public enum SchemaIndex
    {
        NATIVE_BTREE10( "native-btree", "1.0", false ),
        NATIVE30( "lucene+native", "3.0", false );

        private final String providerKey;
        private final String providerVersion;
        private final boolean deprecated;
        private final String providerName;

        // NOTE: if any providers are deprecated in the future, go to the git history and bring back IndexingServiceTest.shouldLogDeprecatedIndexesOnStart.
        SchemaIndex( String providerKey, String providerVersion, boolean deprecated )
        {
            this.providerKey = providerKey;
            this.providerVersion = providerVersion;
            this.deprecated = deprecated;
            this.providerName = toProviderName( providerKey, providerVersion );
        }

        public String providerName()
        {
            return providerName;
        }

        public String providerKey()
        {
            return providerKey;
        }

        public String providerVersion()
        {
            return providerVersion;
        }

        public boolean deprecated()
        {
            return deprecated;
        }

        @Override
        public String toString()
        {
            return providerName;
        }

        private static String toProviderName( String providerName, String providerVersion )
        {
            return providerName + '-' + providerVersion;
        }
    }

    @Description(
            "Index provider to use for newly created schema indexes. " +
                    "An index provider may store different value types in separate physical indexes. " +
                    "native-btree-1.0: All value types and arrays of all value types, even composite keys, are stored in one native index. " +
                    "lucene+native-3.0: Like native-btree-1.0 but single property strings are stored in Lucene. " +
                    "A native index has faster updates, less heap and CPU usage compared to a Lucene index. " +
                    "A native index has some limitations around key size and slower execution of CONTAINS and ENDS WITH string index queries, " +
                    "compared to a Lucene index.\n" +
                    "Deprecated: Which index provider to use will be a fully internal concern." )
    @Deprecated
    public static final Setting<String> default_schema_provider =
            newBuilder( "dbms.index.default_schema_provider", STRING, SchemaIndex.NATIVE_BTREE10.toString() ).build();

    @Description( "The default index provider used for managing full-text indexes. Only 'fulltext-1.0' is supported." )
    @Internal
    public static final Setting<String> default_fulltext_provider =
            newBuilder( "unsupported.dbms.index.default_fulltext_provider", STRING, "fulltext-1.0" ).build();

    // Store settings
    @Description( "Make Neo4j keep the logical transaction logs for being able to backup the database. " +
            "Can be used for specifying the threshold to prune logical logs after. For example \"10 days\" will " +
            "prune logical logs that only contains transactions older than 10 days from the current time, " +
            "or \"100k txs\" will keep the 100k latest transactions and prune any older transactions." )
    public static final Setting<String> keep_logical_logs = newBuilder( "dbms.tx_log.rotation.retention_policy", STRING, "7 days" )
                    .dynamic()
                    .addConstraint( SettingConstraints.matches( "^(true|keep_all|false|keep_none|(\\d+[KkMmGg]?( (files|size|txs|entries|hours|days))))$",
                            "must be `true`, `false` or of format `<number><optional unit> <type>`. " +
                                    "Valid units are `k`, `M` and `G`. " +
                                    "Valid types are `files`, `size`, `txs`, `entries`, `hours` and `days`. " +
                                    "For example, `100M size` will limiting logical log space on disk to 100Mb," +
                                    " or `200k txs` will limiting the number of transactions to keep to 200 000" ) )
                    .build();

    @Description( "Specifies at which file size the logical log will auto-rotate. Minimum accepted value is 128 KiB. " )
    public static final Setting<Long> logical_log_rotation_threshold =
            newBuilder( "dbms.tx_log.rotation.size", BYTES, mebiBytes( 250 ) ).addConstraint( min( kibiBytes( 128 ) ) ).dynamic().build();

    @Description( "Specify if Neo4j should try to preallocate logical log file in advance." )
    public static final Setting<Boolean> preallocate_logical_logs = newBuilder( "dbms.tx_log.preallocate", BOOL, true ).dynamic().build();

    @Description( "If `true`, Neo4j will abort recovery if any errors are encountered in the logical log. Setting " +
            "this to `false` will allow Neo4j to restore as much as possible from the corrupted log files and ignore " +
            "the rest, but, the integrity of the database might be compromised." )
    @Internal
    public static final Setting<Boolean> fail_on_corrupted_log_files =
            newBuilder("unsupported.dbms.tx_log.fail_on_corrupted_log_files", BOOL, true ).build();

    @Description( "If `true`, Neo4j will abort recovery if logical log files are missing. Setting " +
            "this to `false` will allow Neo4j to create new empty missing files for already existing database, but, " +
            "the integrity of the database might be compromised." )
    public static final Setting<Boolean> fail_on_missing_files = newBuilder( "dbms.recovery.fail_on_missing_files", BOOL, true ).build();

    @Description( "Specifies if engine should run cypher query based on a snapshot of accessed data. " +
            "Query will be restarted in case if concurrent modification of data will be detected." )
    @Internal
    public static final Setting<Boolean> snapshot_query = newBuilder( "unsupported.dbms.query.snapshot", BOOL, false ).build();

    @Description( "Specifies number or retries that query engine will do to execute query based on " +
            "stable accessed data snapshot before giving up." )
    @Internal
    public static final Setting<Integer> snapshot_query_retries =
            newBuilder( "unsupported.dbms.query.snapshot.retries", INT, 5 ).addConstraint( range( 1, Integer.MAX_VALUE ) ).build();

    @Description( "The amount of memory to use for mapping the store files, in bytes (or kilobytes with the 'k' " +
            "suffix, megabytes with 'm' and gigabytes with 'g'). If Neo4j is running on a dedicated server, " +
            "then it is generally recommended to leave about 2-4 gigabytes for the operating system, give the " +
            "JVM enough heap to hold all your transaction state and query context, and then leave the rest for " +
            "the page cache. If no page cache memory is configured, then a heuristic setting is computed based " +
            "on available system resources." )
    public static final Setting<String> pagecache_memory = newBuilder( "dbms.memory.pagecache.size", STRING, null ).build();

    @Description( "This setting is not used anymore." )
    @Deprecated
    public static final Setting<String> pagecache_swapper = newBuilder( "dbms.memory.pagecache.swapper", STRING, null ).build();

    @Description( "The maximum number of worker threads to use for pre-fetching data when doing sequential scans. " +
            "Set to '0' to disable pre-fetching for scans." )
    public static final Setting<Integer> pagecache_scan_prefetch = newBuilder( "dbms.memory.pagecache.scan.prefetchers", INT, 4 )
            .addConstraint( range( 0, 255 ) ).build();

    @Description( "The profiling frequency for the page cache. Accurate profiles allow the page cache to do active " +
            "warmup after a restart, reducing the mean time to performance. " +
            "This feature available in Neo4j Enterprise Edition." )
    public static final Setting<Duration> pagecache_warmup_profiling_interval =
            newBuilder( "dbms.memory.pagecache.warmup.profile.interval", DURATION, ofMinutes( 1 ) ).build();

    @Description( "Page cache can be configured to perform usage sampling of loaded pages that can be used to construct active load profile. " +
            "According to that profile pages can be reloaded on the restart, replication, etc. " +
            "This setting allows disabling that behavior. " +
            "This feature available in Neo4j Enterprise Edition." )
    public static final Setting<Boolean> pagecache_warmup_enabled =
            newBuilder( "dbms.memory.pagecache.warmup.enable", BOOL, true ).build();

    @Description( "Page cache warmup can be configured to prefetch files, preferably when cache size is bigger than store size. " +
            "Files to be prefetched can be filtered by 'dbms.memory.pagecache.warmup.preload.whitelist'. " +
            "Enabling this disables warmup by profile " )
    public static final Setting<Boolean> pagecache_warmup_prefetch =
            newBuilder( "dbms.memory.pagecache.warmup.preload", BOOL, false ).build();

    @Description( "Page cache warmup prefetch file whitelist regex. By default matches all files" )
    public static final Setting<String> pagecache_warmup_prefetch_whitelist =
            newBuilder( "dbms.memory.pagecache.warmup.preload.whitelist", STRING, ".*" ).build();

    @Description( "Use direct I/O for page cache. Setting is supported only on Linux and only for a subset of record formats" +
            " that use platform aligned page size." )
    public static final Setting<Boolean> pagecache_direct_io =
            newBuilder( "dbms.memory.pagecache.directio", BOOL, false ).build();

    @Description( "Allows the enabling or disabling of the file watcher service." +
            " This is an auxiliary service but should be left enabled in almost all cases." )
    public static final Setting<Boolean> filewatcher_enabled = newBuilder( "dbms.filewatcher.enabled", BOOL, true ).build();

    /**
     * Block size properties values depends from selected record format.
     * We can't figured out record format until it will be selected by corresponding edition.
     * As soon as we will figure it out properties will be re-evaluated and overwritten, except cases of user
     * defined value.
     */
    @Description( "Specifies the block size for storing strings. This parameter is only honored when the store is " +
            "created, otherwise it is ignored. " +
            "Note that each character in a string occupies two bytes, meaning that e.g a block size of 120 will hold " +
            "a 60 character long string before overflowing into a second block. " +
            "Also note that each block carries a ~10B of overhead so record size on disk will be slightly larger " +
            "than the configured block size" )
    @Internal
    public static final Setting<Integer> string_block_size =
            newBuilder( "unsupported.dbms.block_size.strings", INT, 0 ).addConstraint( min( 0 ) ).build();

    @Description( "Specifies the block size for storing arrays. This parameter is only honored when the store is " +
            "created, otherwise it is ignored. " +
            "Also note that each block carries a ~10B of overhead so record size on disk will be slightly larger " +
            "than the configured block size" )
    @Internal
    public static final Setting<Integer> array_block_size =
            newBuilder( "unsupported.dbms.block_size.array_properties", INT, 0 ).addConstraint( min( 0 ) ).build();

    @Description( "Specifies the block size for storing labels exceeding in-lined space in node record. " +
            "This parameter is only honored when the store is created, otherwise it is ignored. " +
            "Also note that each block carries a ~10B of overhead so record size on disk will be slightly larger " +
            "than the configured block size" )
    @Internal
    public static final Setting<Integer> label_block_size =
            newBuilder( "unsupported.dbms.block_size.labels", INT, 0 ).addConstraint( min( 0 ) ).build();

    @Description( "An identifier that uniquely identifies this graph database instance within this JVM. " +
            "Defaults to an auto-generated number depending on how many instance are started in this JVM." )
    @Internal
    public static final Setting<String> forced_kernel_id = newBuilder( "unsupported.dbms.kernel_id", STRING, null )
            .addConstraint( SettingConstraints.matches( "[a-zA-Z0-9]*", "has to be a valid kernel identifier" ) )
            .build();

    @Internal
    public static final Setting<Duration> vm_pause_monitor_measurement_duration =
            newBuilder( "unsupported.vm_pause_monitor.measurement_duration", DURATION, ofMillis( 100 ) ).build();

    @Internal
    public static final Setting<Duration> vm_pause_monitor_stall_alert_threshold =
            newBuilder( "unsupported.vm_pause_monitor.stall_alert_threshold", DURATION, ofMillis( 100 ) ).build();

    @Description( "Relationship count threshold for considering a node to be dense" )
    public static final Setting<Integer> dense_node_threshold =
            newBuilder( "dbms.relationship_grouping_threshold", INT, 50 ).addConstraint( min( 1 ) ).build();

    @Description( "Specifies the use of the new faster but experimental consistency checker" )
    public static final Setting<Boolean> experimental_consistency_checker = newBuilder( "unsupported.consistency_checker.experimental", BOOL, true ).build();

    @Description( "Specifies if the experimental consistency checker should stop when number of observed inconsistencies exceed the threshold. " +
            "If the value is zero, all inconsistencies will be reported" )
    public static final Setting<Integer> experimental_consistency_checker_stop_threshold =
            newBuilder( "unsupported.consistency_checker.experimental.fail_fast", INT, 0 ).addConstraint( min( 0 ) ).build();

    @Description( "Log executed queries. Valid values are 'OFF', 'INFO' & 'VERBOSE'.\n" +
            "OFF:  no logging.\n" +
            "INFO: log queries at the end of execution, that take longer than the configured threshold, dbms.logs.query.threshold.\n" +
            "VERBOSE: log queries at the start and end of execution, regardless of dbms.logs.query.threshold.\n" +
            "Log entries are by default written to the file _query.log_ located in the Logs directory. " +
            "For location of the Logs directory, see <<file-locations>>. " +
            "This feature is available in the Neo4j Enterprise Edition." )
    public static final Setting<LogQueryLevel> log_queries =
            newBuilder( "dbms.logs.query.enabled", ofEnum( LogQueryLevel.class ), LogQueryLevel.VERBOSE ).dynamic().build();

    public enum LogQueryLevel
    {
        OFF, INFO, VERBOSE
    }

    @Description( "Send user logs to the process stdout. " +
            "If this is disabled then logs will instead be sent to the file _neo4j.log_ located in the logs directory. " +
            "For location of the Logs directory, see <<file-locations>>." )
    public static final Setting<Boolean> store_user_log_to_stdout = newBuilder( "dbms.logs.user.stdout_enabled", BOOL, true ).build();

    @Description( "Path of the logs directory." )
    public static final Setting<Path> logs_directory = newBuilder( "dbms.directories.logs", PATH, Path.of( "logs" ) )
            .setDependency( neo4j_home )
            .immutable()
            .build();

    @Description( "Path to the query log file." )
    public static final Setting<Path> log_queries_filename = newBuilder( "dbms.logs.query.path", PATH, Path.of( "query.log" ) )
            .setDependency( logs_directory )
            .immutable()
            .build();

    @Description( "Path to the user log file." )
    public static final Setting<Path> store_user_log_path = newBuilder( "dbms.logs.user.path", PATH, Path.of( "neo4j.log" ) )
            .setDependency( logs_directory )
            .immutable()
            .build();

    @Description( "Path to the debug log file." )
    public static final Setting<Path> store_internal_log_path = newBuilder( "dbms.logs.debug.path", PATH, Path.of( "debug.log" ) )
            .setDependency( logs_directory )
            .immutable()
            .build();

    @Description( "Log parameters for the executed queries being logged." )
    public static final Setting<Boolean> log_queries_parameter_logging_enabled =
            newBuilder( "dbms.logs.query.parameter_logging_enabled", BOOL, true ).dynamic().build();

    @Description( "Log complete parameter entities including id, labels or relationship type, and properties. If false, " +
                  "only the entity id will be logged. This only takes effect if `dbms.logs.query.parameter_logging_enabled = true`." )
    public static final Setting<Boolean> log_queries_parameter_full_entities =
            newBuilder( "dbms.logs.query.parameter_full_entities", BOOL, false ).dynamic().build();

    @Description( "Log detailed time information for the executed queries being logged. Requires `dbms.track_query_cpu_time=true`" )
    public static final Setting<Boolean> log_queries_detailed_time_logging_enabled =
            newBuilder( "dbms.logs.query.time_logging_enabled", BOOL, false ).dynamic().build();

    @Description( "Log allocated bytes for the executed queries being logged. " +
            "The logged number is cumulative over the duration of the query, " +
            "i.e. for memory intense or long-running queries the value may be larger " +
            "than the current memory allocation. Requires `dbms.track_query_allocation=true`" )
    public static final Setting<Boolean> log_queries_allocation_logging_enabled =
            newBuilder( "dbms.logs.query.allocation_logging_enabled", BOOL, true ).dynamic().build();

    @Description( "Logs which runtime that was used to run the query" )
    public static final Setting<Boolean> log_queries_runtime_logging_enabled =
            newBuilder( "dbms.logs.query.runtime_logging_enabled", BOOL, true ).dynamic().build();

    @Description( "Log page hits and page faults for the executed queries being logged." )
    public static final Setting<Boolean> log_queries_page_detail_logging_enabled =
            newBuilder( "dbms.logs.query.page_logging_enabled", BOOL, false ).dynamic().build();

    @Description( "Log query text and parameters without obfuscating passwords. " +
            "This allows queries to be logged earlier before parsing starts." )
    public static final Setting<Boolean> log_queries_early_raw_logging_enabled =
            newBuilder( "dbms.logs.query.early_raw_logging_enabled", BOOL, false ).dynamic().build();

    @Description( "If the execution of query takes more time than this threshold, the query is logged once completed - " +
            "provided query logging is set to INFO. Defaults to 0 seconds, that is all queries are logged." )
    public static final Setting<Duration> log_queries_threshold =
            newBuilder( "dbms.logs.query.threshold", DURATION, Duration.ZERO ).dynamic().build();

    @Description( "The file size in bytes at which the query log will auto-rotate. If set to zero then no rotation " +
            "will occur. Accepts a binary suffix `k`, `m` or `g`." )
    public static final Setting<Long> log_queries_rotation_threshold = newBuilder( "dbms.logs.query.rotation.size", BYTES, mebiBytes( 20 ) )
            .addConstraint( range( 0L, Long.MAX_VALUE ) )
            .dynamic()
            .build();

    @Description( "Maximum number of history files for the query log." )
    public static final Setting<Integer> log_queries_max_archives =
            newBuilder( "dbms.logs.query.rotation.keep_number", INT, 7 ).addConstraint( min( 1 ) ).dynamic().build();

    @Description( "Create a heap dump just before the end of each query execution. " +
            "The heap dump will be placed in log directory and the file name will contain the query id, to be correlated with an entry in the query log. " +
            "Only live objects will be included to minimize the file size. " )
    @Internal
    public static final Setting<Boolean> log_queries_heap_dump_enabled =
            newBuilder( "unsupported.dbms.logs.query.heap_dump_enabled", BOOL, false ).dynamic().build();

    @Description( "Specifies number of operations that batch inserter will try to group into one batch before " +
            "flushing data into underlying storage." )
    @Internal
    public static final Setting<Integer> batch_inserter_batch_size =
            newBuilder( "unsupported.tools.batch_inserter.batch_size", INT, 10000 ).build();

    // Security settings

    @Description( "Enable auth requirement to access Neo4j." )
    @DocumentedDefaultValue( "true" ) // Should document server defaults.
    public static final Setting<Boolean> auth_enabled = newBuilder( "dbms.security.auth_enabled", BOOL, false ).build();

    @Internal
    public static final Setting<Path> auth_store =
            newBuilder( "unsupported.dbms.security.auth_store.location", PATH, null ).setDependency( neo4j_home ).immutable().build();

    @Description( "The maximum number of unsuccessful authentication attempts before imposing a user lock for the configured amount of time." +
            "The locked out user will not be able to log in until the lock period expires, even if correct credentials are provided. " +
            "Setting this configuration option to values less than 3 is not recommended because it might make it easier for an attacker " +
            "to brute force the password." )
    public static final Setting<Integer> auth_max_failed_attempts =
            newBuilder( "dbms.security.auth_max_failed_attempts", INT, 3 ).addConstraint( min( 0 ) ).build();

    @Description( "The amount of time user account should be locked after a configured number of unsuccessful authentication attempts. " +
            "The locked out user will not be able to log in until the lock period expires, even if correct credentials are provided. " +
            "Setting this configuration option to a low value is not recommended because it might make it easier for an attacker to " +
            "brute force the password." )
    public static final Setting<Duration> auth_lock_time =
            newBuilder( "dbms.security.auth_lock_time", DURATION, ofSeconds( 5 ) ).addConstraint( min( ofSeconds( 0 ) ) ).build();

    @Description( "A list of procedures and user defined functions (comma separated) that are allowed full access to " +
            "the database. The list may contain both fully-qualified procedure names, and partial names with the " +
            "wildcard '*'. Note that this enables these procedures to bypass security. Use with caution." )
    public static final Setting<List<String>> procedure_unrestricted =
            newBuilder( "dbms.security.procedures.unrestricted", listOf( STRING ), emptyList() ) .build();

    @Description( "A list of procedures (comma separated) that are to be loaded. " +
            "The list may contain both fully-qualified procedure names, and partial names with the wildcard '*'. " +
            "If this setting is left empty no procedures will be loaded." )
    public static final Setting<List<String>> procedure_whitelist =
            newBuilder( "dbms.security.procedures.whitelist", listOf( STRING ), List.of("*") ).build();

    //=========================================================================
    // Procedure security settings
    //=========================================================================

    @Description( "The default role that can execute all procedures and user-defined functions that are not covered " +
            "by the `" + "dbms.security.procedures.roles" + "` setting. If the `" + "dbms.security.procedures.default_allowed" +
            "` setting is the empty string (default), procedures will be executed according to the same security " +
            "rules as normal Cypher statements." )
    public static final Setting<String> default_allowed = newBuilder( "dbms.security.procedures.default_allowed", STRING, "" ).build();

    @Description( "This provides a finer level of control over which roles can execute procedures than the " +
            "`" + "dbms.security.procedures.default_allowed" + "` setting. For example: `+dbms.security.procedures.roles=" +
            "apoc.convert.*:reader;apoc.load.json*:writer;apoc.trigger.add:TriggerHappy+` will allow the role " +
            "`reader` to execute all procedures in the `apoc.convert` namespace, the role `writer` to execute " +
            "all procedures in the `apoc.load` namespace that starts with `json` and the role `TriggerHappy` " +
            "to execute the specific procedure `apoc.trigger.add`. Procedures not matching any of these " +
            "patterns will be subject to the `" + "dbms.security.procedures.default_allowed" + "` setting." )
    public static final Setting<String> procedure_roles = newBuilder( "dbms.security.procedures.roles", STRING, "" ).build();

    @Description( "Default network interface to listen for incoming connections. " +
            "To listen for connections on all interfaces, use \"0.0.0.0\". " )
    public static final Setting<SocketAddress> default_listen_address =
            newBuilder( "dbms.default_listen_address", SOCKET_ADDRESS, new SocketAddress( "localhost" ) )
                    .addConstraint( HOSTNAME_ONLY )
                    .immutable()
                    .build();

    @Description( "Default hostname or IP address the server uses to advertise itself." )
    public static final Setting<SocketAddress> default_advertised_address =
            newBuilder( "dbms.default_advertised_address", SOCKET_ADDRESS, new SocketAddress( "localhost" ) )
                    .addConstraint( HOSTNAME_ONLY )
                    .immutable()
                    .build();

    // Bolt Settings

    @Description( "Whether to apply network level outbound network buffer based throttling" )
    @Internal
    public static final Setting<Boolean> bolt_outbound_buffer_throttle =
            newBuilder( "unsupported.dbms.bolt.outbound_buffer_throttle", BOOL, true ).build();

    @Description( "When the size (in bytes) of outbound network buffers, used by bolt's network layer, " +
            "grows beyond this value bolt channel will advertise itself as unwritable and will block " +
            "related processing thread until it becomes writable again." )
    @Internal
    public static final Setting<Integer> bolt_outbound_buffer_throttle_high_water_mark =
            newBuilder( "unsupported.dbms.bolt.outbound_buffer_throttle.high_watermark", INT, (int) kibiBytes( 512  ) )
                    .addConstraint( range( (int) kibiBytes( 64 ), Integer.MAX_VALUE ) )
                    .build();

    @Description( "When the size (in bytes) of outbound network buffers, previously advertised as unwritable, " +
            "gets below this value bolt channel will re-advertise itself as writable and blocked processing " +
            "thread will resume execution." )
    @Internal
    public static final Setting<Integer> bolt_outbound_buffer_throttle_low_water_mark =
            newBuilder( "unsupported.dbms.bolt.outbound_buffer_throttle.low_watermark", INT, (int) kibiBytes( 128 ))
                    .addConstraint( range( (int) kibiBytes( 16 ), Integer.MAX_VALUE ) ).build();

    @Description( "When the total time outbound network buffer based throttle lock is held exceeds this value, " +
            "the corresponding bolt channel will be aborted. Setting " +
            "this to 0 will disable this behaviour." )
    @Internal
    public static final Setting<Duration> bolt_outbound_buffer_throttle_max_duration =
            newBuilder( "unsupported.dbms.bolt.outbound_buffer_throttle.max_duration", DURATION, ofMinutes( 15 )  )
                    .addConstraint( any( min( ofSeconds( 30 ) ), is( Duration.ZERO )  ) )
                    .build();

    @Description( "When the number of queued inbound messages grows beyond this value, reading from underlying " +
            "channel will be paused (no more inbound messages will be available) until queued number of " +
            "messages drops below the configured low watermark value." )
    @Internal
    public static final Setting<Integer> bolt_inbound_message_throttle_high_water_mark =
            newBuilder( "unsupported.dbms.bolt.inbound_message_throttle.high_watermark", INT, 300 )
                    .addConstraint( range( 1, Integer.MAX_VALUE ) ).build();

    @Description( "When the number of queued inbound messages, previously reached configured high watermark value, " +
            "drops below this value, reading from underlying channel will be enabled and any pending messages " +
            "will start queuing again." )
    @Internal
    public static final Setting<Integer> bolt_inbound_message_throttle_low_water_mark =
            newBuilder( "unsupported.dbms.bolt.inbound_message_throttle.low_watermark", INT, 100 )
                    .addConstraint( range( 1, Integer.MAX_VALUE ) )
                    .build();

    @Description( "Create an archive of an index before re-creating it if failing to load on startup." )
    @Internal
    public static final Setting<Boolean> archive_failed_index =
            newBuilder( "unsupported.dbms.index.archive_failed", BOOL, false ).build();

    @Description( "The maximum amount of time to wait for the database state represented by the bookmark." )
    public static final Setting<Duration> bookmark_ready_timeout =
            newBuilder( "dbms.transaction.bookmark_ready_timeout", DURATION, ofSeconds( 30 ) ).addConstraint( min( ofSeconds( 1 ) ) ).build();

    @Description( "How long callers should cache the response of the routing procedure `dbms.routing.getRoutingTable()`" )
    public static final Setting<Duration> routing_ttl =
            newBuilder( "dbms.routing_ttl", DURATION, ofSeconds( 300 ) ).addConstraint( min( ofSeconds( 1 ) ) ).build();

    @Description( "Limit the amount of memory that all of the running transactions can consume, in bytes (or kilobytes with the 'k' " +
            "suffix, megabytes with 'm' and gigabytes with 'g'). Zero means 'unlimited'." )
    public static final Setting<Long> memory_transaction_global_max_size =
            newBuilder( "dbms.memory.transaction.global_max_size", BYTES, 0L )
                    .addConstraint( any( min( mebiBytes( 10 ) ), is( 0L) ) )
                    .dynamic().build();

    @Description( "Limit the amount of memory that all transaction in one database can consume, in bytes (or kilobytes with the 'k' " +
            "suffix, megabytes with 'm' and gigabytes with 'g'). Zero means 'unlimited'." )
    public static final Setting<Long> memory_transaction_database_max_size =
            newBuilder( "dbms.memory.transaction.datababase_max_size", BYTES, 0L )
                    .addConstraint( any( min( mebiBytes( 10 ) ), is( 0L) ) )
                    .dynamic().build();

    @Description( "Limit the amount of memory that a single transaction can consume, in bytes (or kilobytes with the 'k' " +
            "suffix, megabytes with 'm' and gigabytes with 'g'). Zero means 'unlimited'." )
    public static final Setting<Long> memory_transaction_max_size =
            newBuilder( "dbms.memory.transaction.max_size", BYTES, 0L )
                    .addConstraint( any( min( mebiBytes( 1 ) ), is( 0L) ) )
                    .dynamic().build();

    public enum TransactionStateMemoryAllocation
    {
        ON_HEAP, OFF_HEAP
    }

    @Description( "Defines whether memory for transaction state should be allocated on- or off-heap." )
    public static final Setting<TransactionStateMemoryAllocation> tx_state_memory_allocation =
            newBuilder( "dbms.tx_state.memory_allocation", ofEnum( TransactionStateMemoryAllocation.class ), OFF_HEAP ).build();

    @Description( "The maximum amount of off-heap memory that can be used to store transaction state data; it's a total amount of memory " +
            "shared across all active transactions. Zero means 'unlimited'. Used when dbms.tx_state.memory_allocation is set to 'OFF_HEAP'." )
    public static final Setting<Long> tx_state_max_off_heap_memory =
            newBuilder( "dbms.memory.off_heap.max_size", BYTES, BYTES.parse("2G") ).addConstraint( min( 0L ) ).build();

    @Description( "Defines the maximum size of an off-heap memory block that can be cached to speed up allocations. The value must be a power of 2." )
    public static final Setting<Long> tx_state_off_heap_max_cacheable_block_size =
            newBuilder( "dbms.memory.off_heap.max_cacheable_block_size", BYTES, ByteUnit.kibiBytes( 512 ) )
                    .addConstraint( min( kibiBytes( 4 ) ) ).addConstraint( POWER_OF_2 ).build();

    @Description( "Defines the size of the off-heap memory blocks cache. The cache will contain this number of blocks for each block size " +
            "that is power of two. Thus, maximum amount of memory used by blocks cache can be calculated as " +
            "2 * dbms.memory.off_heap.max_cacheable_block_size * dbms.memory.off_heap.block_cache_size" )
    public static final Setting<Integer> tx_state_off_heap_block_cache_size =
            newBuilder( "dbms.memory.off_heap.block_cache_size", INT, 128 ).addConstraint( min( 16 ) ).build();

    @Description( "Defines whether the dbms may retry reconciling a database to its desired state." )
    public static final Setting<Boolean> reconciler_may_retry = newBuilder( "dbms.reconciler.may_retry", BOOL, false ).build();

    @Description( "Defines the maximum amount of time to wait before retrying after the dbms fails to reconcile a database to its desired state." )
    public static final Setting<Duration> reconciler_maximum_backoff = newBuilder( "dbms.reconciler.max_backoff", DURATION, ofHours( 1 ) )
            .addConstraint( min( ofMinutes( 1 ) ) )
            .build();

    @Description( "Defines the minimum amount of time to wait before retrying after the dbms fails to reconcile a database to its desired state." )
    public static final Setting<Duration> reconciler_minimum_backoff = newBuilder( "dbms.reconciler.min_backoff", DURATION, ofSeconds( 2 ) )
            .addConstraint( min( Duration.ofSeconds( 1 ) ) )
            .build();

    @Description( "Defines the level of parallelism employed by the reconciler. By default the parallelism equals the number of available processors or 8 " +
            "(whichever is smaller). If configured as 0, the parallelism of the reconciler will be unbounded." )
    public static final Setting<Integer> reconciler_maximum_parallelism =
            newBuilder( "dbms.reconciler.max_parallelism", INT, Math.min( Runtime.getRuntime().availableProcessors(), 8 ) )
            .addConstraint( min( 0 ) )
            .build();

    @Description( "Forces smaller ID cache, in order to preserve memory." )
    @Internal
    public static final Setting<Boolean> force_small_id_cache = newBuilder( "unsupported.dbms.force_small_id_cache", BOOL, Boolean.FALSE ).build();

    @Internal
    public static final Setting<Boolean> consistency_check_on_apply =
            newBuilder( "unsupported.dbms.storage.consistency_check_on_apply", BOOL, Boolean.FALSE ).build();

    /**
     * Default settings for connectors. The default values are assumes to be default for embedded deployments through the code.
     * This map contains default connector settings that you can pass to the builders.
     */
    public static final Map<Setting<?>, Object> SERVER_DEFAULTS = Map.of(
            HttpConnector.enabled, true,
            HttpsConnector.enabled, false,
            BoltConnector.enabled, true,
            auth_enabled, true
    );
}
