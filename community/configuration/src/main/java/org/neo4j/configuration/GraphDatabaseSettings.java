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

import org.eclipse.collections.api.factory.Maps;

import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.connectors.ConnectorDefaults;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.FormattedLogFormat;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogTimeZone;

import static java.lang.Runtime.getRuntime;
import static java.time.Duration.ofHours;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation.OFF_HEAP;
import static org.neo4j.configuration.SettingConstraints.ABSOLUTE_PATH;
import static org.neo4j.configuration.SettingConstraints.HOSTNAME_ONLY;
import static org.neo4j.configuration.SettingConstraints.POWER_OF_2;
import static org.neo4j.configuration.SettingConstraints.any;
import static org.neo4j.configuration.SettingConstraints.ifPrimary;
import static org.neo4j.configuration.SettingConstraints.is;
import static org.neo4j.configuration.SettingConstraints.max;
import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingConstraints.shouldNotContain;
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
import static org.neo4j.configuration.SettingValueParsers.setOf;
import static org.neo4j.io.ByteUnit.gibiBytes;
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

    public static final String SYSTEM_DATABASE_NAME = NamedDatabaseId.SYSTEM_DATABASE_NAME;
    public static final String DEFAULT_DATABASE_NAME = "neo4j";

    public static final String DEFAULT_DATA_DIR_NAME = "data";
    public static final String DEFAULT_DATABASES_ROOT_DIR_NAME = "databases";
    public static final String DEFAULT_TX_LOGS_ROOT_DIR_NAME = "transactions";
    public static final String DEFAULT_SCRIPT_FOLDER = "scripts";
    public static final String DEFAULT_DUMPS_DIR_NAME = "dumps";
    public static final String DEFAULT_LICENSES_DIR_NAME = "licenses";

    public static final int DEFAULT_ROUTING_CONNECTOR_PORT = 7688;

    @Description( "Root relative to which directory settings are resolved." )
    @DocumentedDefaultValue( "Defaults to current working directory" )
    public static final Setting<Path> neo4j_home = newBuilder( "dbms.directories.neo4j_home", PATH, Path.of( "" ).toAbsolutePath() )
            .addConstraint( ABSOLUTE_PATH )
            .immutable()
            .build();

    @Description( "Name of the default database (aliases are not supported)." )
    public static final Setting<String> default_database =
            newBuilder( "dbms.default_database", DATABASENAME, DEFAULT_DATABASE_NAME ).build();

    @Description( "Path of the data directory. You must not configure more than one Neo4j installation to use the " +
            "same data directory." )
    public static final Setting<Path> data_directory = newBuilder( "dbms.directories.data", PATH, Path.of( DEFAULT_DATA_DIR_NAME ) )
            .setDependency( neo4j_home )
            .immutable()
            .build();

    @Description( "Root location where Neo4j will store transaction logs for configured databases." )
    public static final Setting<Path> transaction_logs_root_path =
            newBuilder( "dbms.directories.transaction.logs.root", PATH, Path.of( DEFAULT_TX_LOGS_ROOT_DIR_NAME ) )
                    .setDependency( data_directory ).immutable().build();

    @Description( "Root location where Neo4j will store scripts for configured databases." )
    public static final Setting<Path> script_root_path =
            newBuilder( "dbms.directories.script.root", PATH, Path.of( DEFAULT_SCRIPT_FOLDER ) )
                    .setDependency( data_directory ).immutable().build();

    @Description( "Root location where Neo4j will store database dumps optionally produced when dropping said databases." )
    public static final Setting<Path> database_dumps_root_path =
            newBuilder( "dbms.directories.dumps.root", PATH, Path.of( DEFAULT_DUMPS_DIR_NAME ) )
                    .setDependency( data_directory ).immutable().build();

    @Description( "Only allow read operations from this Neo4j instance. " +
            "This mode still requires write access to the directory for lock purposes. " +
            "Replaced by: dbms.databases.default_to_read_only, dbms.databases.read_only, dbms.databases.writable." )
    @Deprecated( since = "4.3.0", forRemoval = true )
    public static final Setting<Boolean> read_only = newBuilder( "dbms.read_only", BOOL, false ).build();

    @Description( "Whether or not any database on this instance are read_only by default. If false, individual databases may be marked as read_only using " +
                  "dbms.database.read_only. If true, individual databases may be marked as writable using dbms.databases.writable." )
    public static final Setting<Boolean> read_only_database_default = newBuilder( "dbms.databases.default_to_read_only", BOOL, false )
            .dynamic().build();

    @Description( "List of databases for which to prevent write queries. Databases not included in this list maybe read_only anyway depending upon the value " +
                  "of dbms.databases.default_to_read_only." )
    public static final Setting<Set<String>> read_only_databases = newBuilder( "dbms.databases.read_only", setOf( DATABASENAME ), emptySet() )
            .addConstraint( shouldNotContain( SYSTEM_DATABASE_NAME, "read only databases collection" ) ).dynamic().build();

    @Description( "List of databases for which to allow write queries. Databases not included in this list will allow write queries anyway, unless " +
                  "dbms.databases.default_to_read_only is set to true." )
    public static final Setting<Set<String>> writable_databases = newBuilder( "dbms.databases.writable", setOf( DATABASENAME ), emptySet() )
            .dynamic().build();

    @Description( "A strict configuration validation will prevent the database from starting up if unknown " +
            "configuration options are specified in the neo4j settings namespace (such as dbms., cypher., etc)." )
    public static final Setting<Boolean> strict_config_validation = newBuilder( "dbms.config.strict_validation", BOOL, false ).build();

    @Description( "Whether to allow a store upgrade in case the current version of the database starts against an older version of the store." )
    public static final Setting<Boolean> allow_upgrade = newBuilder( "dbms.allow_upgrade", BOOL, false ).dynamic().build();

    @Description( "Max number of processors used when upgrading the store. Defaults to the number of processors available to the JVM. " +
            "There is a certain amount of minimum threads needed so for that reason there is no lower bound for this " +
            "value. For optimal performance this value shouldn't be greater than the number of available processors." )
    public static final Setting<Integer> upgrade_processors = newBuilder( "dbms.upgrade_max_processors", INT, 0 ).addConstraint( min( 0 ) ).dynamic().build();

    @Description( "Database record format. Valid values are blank(no value, default), `standard`, `aligned`, or `high_limit`. " +
            "Specifying a value will force new databases to that format and existing databases to migrate if `dbms.allow_upgrade=true` is specified. " +
            "The `aligned` format is essentially the `standard` format with some minimal padding at the end of pages such that a single " +
            "record will never cross a page boundary. The `high_limit` format is available for Enterprise Edition only. " +
            "It is required if you have a graph that is larger than 34 billion nodes, 34 billion relationships, or 68 billion properties. " +
            "A change of the record format is irreversible. " +
            "Certain operations may suffer from a performance penalty of up to 10%, which is why this format is not switched on by default. " +
            "However, if you want to change the configured record format value, you must also set `dbms.allow_upgrade=true`, " +
            "because the setting implies a one-way store format migration." )
    @DocumentedDefaultValue( "`aligned` for new databases. Existing databases stay on their current format." )
    public static final Setting<String> record_format = newBuilder( "dbms.record_format", STRING, "" ).build();

    @Description( "Whether to allow a system graph upgrade to happen automatically in single instance mode (dbms.mode=SINGLE). " +
                  "Default is true. In clustering environments no automatic upgrade will happen (dbms.mode=CORE or dbms.mode=READ_REPLICA). " +
                  "If set to false, or when in a clustering environment, it is necessary to call the procedure `dbms.upgrade()` to " +
                  "complete the upgrade." )
    public static final Setting<Boolean> allow_single_automatic_upgrade = newBuilder( "dbms.allow_single_automatic_upgrade", BOOL, true ).dynamic().build();

    @Description( "Configure the operating mode of the database -- 'SINGLE' for stand-alone operation, " +
                  "'CORE' for operating as a core member of a Causal Cluster, " +
                  "or 'READ_REPLICA' for operating as a read replica member of a Causal Cluster. Only SINGLE mode is allowed in Community" )
    public static final Setting<Mode> mode = newBuilder( "dbms.mode", ofEnum( Mode.class ), Mode.SINGLE ).build();

    @Description( "Enable discovery service and a catchup server to be started on an Enterprise Standalone Instance 'dbms.mode=SINGLE', " +
                  "and with that allow for Read Replicas to connect and pull transaction from it. " +
                  "When 'dbms.mode' is clustered (CORE, READ_REPLICA) this setting is not recognized." )
    public static final Setting<Boolean> enable_clustering_in_standalone = newBuilder( "dbms.clustering.enable", BOOL, false ).build();

    @Description( "Routing strategy for neo4j:// protocol connections.\n" +
                  "Default is `CLIENT`, using client-side routing, with server-side routing as a fallback (if enabled).\n" +
                  "When set to `SERVER`, client-side routing is short-circuited, and requests will rely on server-side routing " +
                  "(which must be enabled for proper operation, i.e. `dbms.routing.enabled=true`).\n" +
                  "Can be overridden by `dbms.routing.client_side.enforce_for_domains`." )
    public static final Setting<RoutingMode> routing_default_router = newBuilder( "dbms.routing.default_router", ofEnum( RoutingMode.class ),
                                                                                  RoutingMode.CLIENT ).build();

    @Description( "Always use client side routing (regardless of the default router) for neo4j:// protocol connections to these domains. " +
                  "A comma separated list of domains. Wildcards (*) are supported." )
    public static final Setting<Set<String>> client_side_router_enforce_for_domains = newBuilder( "dbms.routing.client_side.enforce_for_domains",
                                                                                                  setOf( STRING ), Set.of() ).dynamic().build();

    // Cypher settings

    public enum CypherParserVersion
    {
        DEFAULT( "default" ), V_35( "3.5" ), V_43( "4.3" ), V_44( "4.4" );

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

    @Description( "The number of cached Cypher query execution plans per database. " +
            "The max number of query plans that can be kept in cache is the `number of databases` * `dbms.query_cache_size`. " +
            "With 10 databases and `dbms.query_cache_size`=1000, the caches can keep 10000 plans in total on the instance, " +
            "assuming that each DB receives queries that fill up its cache. " )
    public static final Setting<Integer> query_cache_size =
            newBuilder( "dbms.query_cache_size", INT, 1000 ).addConstraint( min( 0 ) ).build();

    @Description( "The threshold for statistics above which a plan is considered stale.\n\n" +
                  "If any of the underlying statistics used to create the plan have changed more than this value, " +
                  "the plan will be considered stale and will be replanned. Change is calculated as " +
                  "`abs(a-b)/max(a,b)`.\n\n" +
                  "This means that a value of `0.75` requires the database to " +
                  "quadruple in size before query replanning. A value of `0` means that the query will be " +
                  "replanned as soon as there is any change in statistics and the replan interval has elapsed.\n\n" +
                  "This interval is defined by `cypher.min_replan_interval` and defaults to 10s. After this interval, the " +
                  "divergence threshold will slowly start to decline, reaching 10% after about 7h. This will " +
                  "ensure that long running databases will still get query replanning on even modest changes, " +
                  "while not replanning frequently unless the changes are very large." )
    public static final Setting<Double> query_statistics_divergence_threshold =
            newBuilder( "cypher.statistics_divergence_threshold", DOUBLE, 0.75 ).addConstraint( range( 0.0, 1.0 ) ).build();

    @Description( "The minimum time between possible cypher query replanning events. After this time, the graph " +
            "statistics will be evaluated, and if they have changed by more than the value set by " +
            "cypher.statistics_divergence_threshold, the query will be replanned. If the statistics have " +
            "not changed sufficiently, the same interval will need to pass before the statistics will be " +
            "evaluated again. Each time they are evaluated, the divergence threshold will be reduced slightly " +
            "until it reaches 10% after 7h, so that even moderately changing databases will see query replanning " +
            "after a sufficiently long time interval." )
    public static final Setting<Duration> cypher_min_replan_interval =
            newBuilder( "cypher.min_replan_interval", DURATION, ofSeconds( 10 ) ).build();

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
    @Description( "The maximum time interval within which lock should be acquired. Zero (default) means timeout is disabled." )
    public static final Setting<Duration> lock_acquisition_timeout =
            newBuilder( "dbms.lock.acquisition.timeout", DURATION, Duration.ZERO ).dynamic().build();

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

    @Description( "Default log format. Will apply to all logs unless overridden." )
    public static final Setting<FormattedLogFormat> default_log_format =
            newBuilder( "dbms.logs.default_format", ofEnum( FormattedLogFormat.class ), FormattedLogFormat.PLAIN ).immutable().build();

    @Description( "Log format to use for user log." )
    public static final Setting<FormattedLogFormat> store_user_log_format =
            newBuilder( "dbms.logs.user.format", ofEnum( FormattedLogFormat.class ), null ).setDependency( default_log_format ).build();

    @Description( "Threshold for rotation of the user log (_neo4j.log_). If set to 0, log rotation is " +
            "disabled. Note that if dbms.logs.user.stdout_enabled is enabled this setting will be ignored." )
    public static final Setting<Long> store_user_log_rotation_threshold =
            newBuilder( "dbms.logs.user.rotation.size", BYTES, 0L ).addConstraint( range( 0L, Long.MAX_VALUE ) ).build();

    @Description( "Threshold for rotation of the debug log." )
    public static final Setting<Long> store_internal_log_rotation_threshold =
            newBuilder( "dbms.logs.debug.rotation.size", BYTES, mebiBytes( 20 ) ).addConstraint( range( 0L, Long.MAX_VALUE ) ).build();

    @Description( "Debug log level threshold." )
    public static final Setting<Level> store_internal_log_level =
            newBuilder( "dbms.logs.debug.level", ofEnum( Level.class ), Level.INFO ).dynamic().build();

    @Description( "Database timezone. Among other things, this setting influences which timezone the logs and monitoring procedures use." )
    public static final Setting<LogTimeZone> db_timezone =
            newBuilder( "dbms.db.timezone", ofEnum( LogTimeZone.class ), LogTimeZone.UTC ).build();

    @Description( "Database timezone for temporal functions. All Time and DateTime values that are created without " +
            "an explicit timezone will use this configured default timezone." )
    public static final Setting<ZoneId> db_temporal_timezone = newBuilder( "db.temporal.timezone", TIMEZONE, ZoneOffset.UTC ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Minimum time interval after last rotation of the user log (_neo4j.log_) before it " +
            "may be rotated again. Note that if dbms.logs.user.stdout_enabled is enabled this setting will be ignored." )
    public static final Setting<Duration> store_user_log_rotation_delay =
            newBuilder( "dbms.logs.user.rotation.delay", DURATION, ofSeconds( 300 ) ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Minimum time interval after last rotation of the debug log before it may be rotated again." )
    public static final Setting<Duration> store_internal_log_rotation_delay =
            newBuilder( "dbms.logs.debug.rotation.delay", DURATION, ofSeconds( 300 ) ).build();

    @Description( "Maximum number of history files for the user log (_neo4j.log_). " +
                  "Note that if dbms.logs.user.stdout_enabled is enabled this setting will be ignored." )
    public static final Setting<Integer> store_user_log_max_archives =
            newBuilder( "dbms.logs.user.rotation.keep_number", INT, 7 ).addConstraint( min( 1 ) ).build();

    @Description( "Maximum number of history files for the debug log." )
    public static final Setting<Integer> store_internal_log_max_archives =
            newBuilder( "dbms.logs.debug.rotation.keep_number", INT, 7 ).addConstraint( min( 1 ) ).build();

    public enum CheckpointPolicy
    {
        PERIODIC, CONTINUOUS, VOLUME, VOLUMETRIC
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
            "interval. A check-point is a point in the transaction logs, which recovery would start from. " +
            "Longer check-point intervals typically mean that recovery will take longer to complete in case " +
            "of a crash. On the other hand, a longer check-point interval can also reduce the I/O load that " +
            "the database places on the system, as each check-point implies a flushing and forcing of all the " +
            "store files.  The default is '100000' for a check-point every 100000 transactions." )
    public static final Setting<Integer> check_point_interval_tx =
            newBuilder( "dbms.checkpoint.interval.tx", INT, 100000 ).addConstraint( min( 1 ) ).build();

    @Description( "Configures the time interval between check-points. The database will not check-point more often " +
            "than this (unless check pointing is triggered by a different event), but might check-point less " +
            "often than this interval, if performing a check-point takes longer time than the configured " +
            "interval. A check-point is a point in the transaction logs, which recovery would start from. " +
            "Longer check-point intervals typically mean that recovery will take longer to complete in case " +
            "of a crash. On the other hand, a longer check-point interval can also reduce the I/O load that " +
            "the database places on the system, as each check-point implies a flushing and forcing of all the " +
            "store files." )
    public static final Setting<Duration> check_point_interval_time =
            newBuilder( "dbms.checkpoint.interval.time", DURATION, ofMinutes( 15 ) ).build();

    @Description( "Configures the volume of transacton logs between check-points. The database will not check-point more often " +
            "than this (unless check pointing is triggered by a different event), but might check-point less " +
            "often than this interval, if performing a check-point takes longer time than the configured " +
            "interval. A check-point is a point in the transaction logs, which recovery would start from. " +
            "Longer check-point intervals typically mean that recovery will take longer to complete in case " +
            "of a crash. On the other hand, a longer check-point interval can also reduce the I/O load that " +
            "the database places on the system, as each check-point implies a flushing and forcing of all the " +
            "store files." )
    public static final Setting<Long> check_point_interval_volume =
            newBuilder( "dbms.checkpoint.interval.volume", BYTES, mebiBytes( 250 ) ).addConstraint( min( ByteUnit.kibiBytes( 1 ) ) ).build();

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
            "Removing the setting, or commenting it out, will set the default value of 600." )
    public static final Setting<Integer> check_point_iops_limit =
            newBuilder( "dbms.checkpoint.iops.limit", INT, 600 ).dynamic().build();

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

    // Lucene settings
    @Deprecated( since = "4.0.0", forRemoval = true )
    @Description( "The maximum number of open Lucene index searchers." )
    public static final Setting<Integer> lucene_searcher_cache_size =
            newBuilder( "dbms.index_searcher_cache_size", INT, Integer.MAX_VALUE ).addConstraint( min( 1 ) ).build();

    // Lucene schema indexes
    @Deprecated( since = "4.4.0", forRemoval = true )
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
            return providerName + "-" + providerVersion;
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
    @Deprecated( since = "4.0.0", forRemoval = true )
    public static final Setting<String> default_schema_provider =
            newBuilder( "dbms.index.default_schema_provider", STRING, SchemaIndex.NATIVE_BTREE10.toString() ).build();

    // Store settings
    @Description( "Tell Neo4j how long logical transaction logs should be kept to backup the database." +
            "For example, \"10 days\" will prune logical logs that only contain transactions older than 10 days." +
            "Alternatively, \"100k txs\" will keep the 100k latest transactions from each database and prune any older transactions." )
    public static final Setting<String> keep_logical_logs = newBuilder( "dbms.tx_log.rotation.retention_policy", STRING, "7 days" )
                    .dynamic()
                    .addConstraint( SettingConstraints.matches( "^(true|keep_all|false|keep_none|(\\d+[KkMmGg]?( (files|size|txs|entries|hours|days))))$",
                            "Must be `true` or `keep_all`, `false` or `keep_none`, or of format `<number><optional unit> <type>`. " +
                                    "Valid units are `K`, `M` and `G`. " +
                                    "Valid types are `files`, `size`, `txs`, `entries`, `hours` and `days`. " +
                                    "For example, `100M size` will limit logical log space on disk to 100MB per database," +
                                    "and `200K txs` will limit the number of transactions kept to 200 000 per database." ) )
                    .build();

    @Description( "Specifies at which file size the logical log will auto-rotate. Minimum accepted value is 128 KiB. " )
    public static final Setting<Long> logical_log_rotation_threshold =
            newBuilder( "dbms.tx_log.rotation.size", BYTES, mebiBytes( 250 ) ).addConstraint( min( kibiBytes( 128 ) ) ).dynamic().build();

    @Description( "On serialization of transaction logs, they will be temporary stored in the byte buffer that will be flushed at the end of the transaction " +
            "or at any moment when buffer will be full." )
    @DocumentedDefaultValue( "By default the size of byte buffer is based on number of available cpu's with " +
            "minimal buffer size of 512KB. Every another 4 cpu's will add another 512KB into the buffer size. " +
            "Maximal buffer size in this default scheme is 4MB taking into account " +
            "that we can have one transaction log writer per database in multi-database env." +
            "For example, runtime with 4 cpus will have buffer size of 1MB; " +
            "runtime with 8 cpus will have buffer size of 1MB 512KB; " +
            "runtime with 12 cpus will have buffer size of 2MB." )
    public static final Setting<Long> transaction_log_buffer_size =
            newBuilder( "dbms.tx_log.buffer.size", LONG, ByteUnit.kibiBytes( Math.min( (getRuntime().availableProcessors() / 4) + 1, 8 ) * 512L ) )
                    .addConstraint( min( kibiBytes( 128 ) ) )
                    .build();

    @Description( "Specify if Neo4j should try to preallocate logical log file in advance." )
    public static final Setting<Boolean> preallocate_logical_logs = newBuilder( "dbms.tx_log.preallocate", BOOL, true ).dynamic().build();

    @Description( "Specify if Neo4j should try to preallocate store files as they grow." )
    public static final Setting<Boolean> preallocate_store_files = newBuilder( "dbms.store.files.preallocate", BOOL, true ).build();

    @Description( "If `true`, Neo4j will abort recovery if transaction log files are missing. Setting " +
            "this to `false` will allow Neo4j to create new empty missing files for the already existing  " +
            "database, but the integrity of the database might be compromised." )
    public static final Setting<Boolean> fail_on_missing_files = newBuilder( "dbms.recovery.fail_on_missing_files", BOOL, true ).build();

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

    @Description( "Page cache can be configured to use a temporal buffer for flushing purposes. It is used to combine, if possible, sequence of several " +
            "cache pages into one bigger buffer to minimize the number of individual IOPS performed and better utilization of available " +
            "I/O resources, especially when those are restricted." )
    public static final Setting<Boolean> pagecache_buffered_flush_enabled =
            newBuilder( "dbms.memory.pagecache.flush.buffer.enabled", BOOL, false ).dynamic().build();

    @Description( "Page cache can be configured to use a temporal buffer for flushing purposes. It is used to combine, if possible, sequence of several " +
            "cache pages into one bigger buffer to minimize the number of individual IOPS performed and better utilization of available " +
            "I/O resources, especially when those are restricted. " +
            "Use this setting to configure individual file flush buffer size in pages (8KiB). " +
            "To be able to utilize this buffer during page cache flushing, buffered flush should be enabled." )
    public static final Setting<Integer> pagecache_flush_buffer_size_in_pages =
            newBuilder( "dbms.memory.pagecache.flush.buffer.size_in_pages", INT, 128 ).addConstraint( range( 1, 512 ) ).dynamic().build();

    @Description( "The profiling frequency for the page cache. " +
            "Accurate profiles allow the page cache to do active warmup after a restart, reducing the mean time to performance.\n" +
            "This feature is available in Neo4j Enterprise Edition." )
    public static final Setting<Duration> pagecache_warmup_profiling_interval =
            newBuilder( "dbms.memory.pagecache.warmup.profile.interval", DURATION, ofMinutes( 1 ) ).build();

    @Description( "Page cache can be configured to perform usage sampling of loaded pages that can be used to construct active load profile. " +
            "According to that profile pages can be reloaded on the restart, replication, etc. " +
            "This setting allows disabling that behavior.\n" +
            "This feature is available in Neo4j Enterprise Edition." )
    public static final Setting<Boolean> pagecache_warmup_enabled =
            newBuilder( "dbms.memory.pagecache.warmup.enable", BOOL, true ).build();

    @Description( "Page cache warmup can be configured to prefetch files, preferably when cache size is bigger than store size. " +
            "Files to be prefetched can be filtered by 'dbms.memory.pagecache.warmup.preload.allowlist'. " +
            "Enabling this disables warmup by profile " )
    public static final Setting<Boolean> pagecache_warmup_prefetch =
            newBuilder( "dbms.memory.pagecache.warmup.preload", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Page cache warmup prefetch file whitelist regex. " +
            "By default matches all files.\n" +
            "Deprecated, use 'dbms.memory.pagecache.warmup.preload.allowlist'." )
    public static final Setting<String> pagecache_warmup_prefetch_whitelist =
            newBuilder( "dbms.memory.pagecache.warmup.preload.whitelist", STRING, ".*" ).build();

    @Description( "Page cache warmup prefetch file allowlist regex. " +
            "By default matches all files." )
    public static final Setting<String> pagecache_warmup_prefetch_allowlist =
            newBuilder( "dbms.memory.pagecache.warmup.preload.allowlist", STRING, ".*" ).build();

    @Description( "Use direct I/O for page cache. " +
            "Setting is supported only on Linux and only for a subset of record formats that use platform aligned page size." )
    public static final Setting<Boolean> pagecache_direct_io =
            newBuilder( "dbms.memory.pagecache.directio", BOOL, false ).build();

    @Description( "Allows the enabling or disabling of the file watcher service. " +
            "This is an auxiliary service but should be left enabled in almost all cases." )
    public static final Setting<Boolean> filewatcher_enabled = newBuilder( "dbms.filewatcher.enabled", BOOL, true ).build();

    @Description( "Relationship count threshold for considering a node to be dense." )
    public static final Setting<Integer> dense_node_threshold =
            newBuilder( "dbms.relationship_grouping_threshold", INT, 50 ).addConstraint( min( 1 ) ).build();

    @Description( "Log executed queries. Valid values are `OFF`, `INFO`, or `VERBOSE`.\n\n" +
            "`OFF`::  no logging.\n" +
            "`INFO`:: log queries at the end of execution, that take longer than the configured threshold, `dbms.logs.query.threshold`.\n" +
            "`VERBOSE`:: log queries at the start and end of execution, regardless of `dbms.logs.query.threshold`.\n\n" +
            "Log entries are written to the query log (dbms.logs.query.path).\n\n" +
            "This feature is available in the Neo4j Enterprise Edition." )
    public static final Setting<LogQueryLevel> log_queries =
            newBuilder( "dbms.logs.query.enabled", ofEnum( LogQueryLevel.class ), LogQueryLevel.VERBOSE ).dynamic().build();

    public enum LogQueryLevel
    {
        OFF, INFO, VERBOSE
    }

    @Description( "Log format to use for the query log." )
    public static final Setting<FormattedLogFormat> log_query_format =
            newBuilder( "dbms.logs.query.format", ofEnum( FormattedLogFormat.class ), null ).setDependency( default_log_format ).build();

    @Description( "Log transaction ID for the executed queries." )
    public static final Setting<Boolean> log_queries_transaction_id =
            newBuilder( "dbms.logs.query.transaction_id.enabled", BOOL, false ).dynamic().build();

    @Description( "Log the start and end of a transaction. Valid values are 'OFF', 'INFO', or 'VERBOSE'.\n" +
            "OFF:  no logging.\n" +
            "INFO: log start and end of transactions that take longer than the configured threshold, dbms.logs.query.transaction.threshold.\n" +
            "VERBOSE: log start and end of all transactions.\n" +
            "Log entries are written to the query log (dbms.logs.query.path).\n" +
            "This feature is available in the Neo4j Enterprise Edition." )
    public static final Setting<LogQueryLevel> log_queries_transactions_level =
            newBuilder( "dbms.logs.query.transaction.enabled", ofEnum( LogQueryLevel.class ), LogQueryLevel.OFF ).dynamic().build();

    @Description( "Send user logs to the process stdout. " +
            "If this is disabled then logs will instead be sent to the file _neo4j.log_ located in the logs directory." )
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

    @Description( "Path to the user log file. Note that if dbms.logs.user.stdout_enabled is enabled this setting will be ignored." )
    public static final Setting<Path> store_user_log_path = newBuilder( "dbms.logs.user.path", PATH, Path.of( "neo4j.log" ) )
            .setDependency( logs_directory )
            .immutable()
            .build();

    @Description( "Log format to use for debug log." )
    public static final Setting<FormattedLogFormat> store_internal_log_format =
            newBuilder( "dbms.logs.debug.format", ofEnum( FormattedLogFormat.class ), null ).setDependency( default_log_format ).build();

    @Description( "Path to the debug log file." )
    public static final Setting<Path> store_internal_log_path = newBuilder( "dbms.logs.debug.path", PATH, Path.of( "debug.log" ) )
            .setDependency( logs_directory )
            .immutable()
            .build();

    @Description( "Path of the licenses directory." )
    public static final Setting<Path> licenses_directory = newBuilder( "dbms.directories.licenses", PATH, Path.of( DEFAULT_LICENSES_DIR_NAME ) )
            .setDependency( neo4j_home )
            .immutable()
            .build();

    @Description( "Log parameters for the executed queries being logged." )
    public static final Setting<Boolean> log_queries_parameter_logging_enabled =
            newBuilder( "dbms.logs.query.parameter_logging_enabled", BOOL, true ).dynamic().build();

    @Description( "Sets a maximum character length use for each parameter in the log. " +
                  "This only takes effect if `dbms.logs.query.parameter_logging_enabled = true`." )
    public static final Setting<Integer> query_log_max_parameter_length =
            newBuilder( "dbms.logs.query.max_parameter_length", INT, Integer.MAX_VALUE ).dynamic().build();

    @Description( "Log complete parameter entities including id, labels or relationship type, and properties. If false, " +
                  "only the entity id will be logged. This only takes effect if `dbms.logs.query.parameter_logging_enabled = true`." )
    public static final Setting<Boolean> log_queries_parameter_full_entities =
            newBuilder( "dbms.logs.query.parameter_full_entities", BOOL, false ).dynamic().build();

    @Description( "Log detailed time information for the executed queries being logged, such as `(planning: 92, waiting: 0)`." )
    public static final Setting<Boolean> log_queries_detailed_time_logging_enabled =
            newBuilder( "dbms.logs.query.time_logging_enabled", BOOL, false ).dynamic().build();

    @Description( "Log allocated bytes for the executed queries being logged. " +
            "The logged number is cumulative over the duration of the query, " +
            "i.e. for memory intense or long-running queries the value may be larger " +
            "than the current memory allocation. Requires `dbms.track_query_allocation=true`" )
    public static final Setting<Boolean> log_queries_allocation_logging_enabled =
            newBuilder( "dbms.logs.query.allocation_logging_enabled", BOOL, true ).dynamic().build();

    @Description( "Logs which runtime that was used to run the query." )
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

    @Description( "If the transaction is open for more time than this threshold, the transaction is logged once completed - " +
                  "provided transaction logging (dbms.logs.query.transaction.enabled) is set to `INFO`. " +
                  "Defaults to 0 seconds (all transactions are logged)." )
    public static final Setting<Duration> log_queries_transaction_threshold =
            newBuilder( "dbms.logs.query.transaction.threshold", DURATION, Duration.ZERO ).dynamic().build();

    @Description( "The file size in bytes at which the query log will auto-rotate. If set to zero then no rotation " +
            "will occur. Accepts a binary suffix `k`, `m` or `g`." )
    public static final Setting<Long> log_queries_rotation_threshold = newBuilder( "dbms.logs.query.rotation.size", BYTES, mebiBytes( 20 ) )
            .addConstraint( range( 0L, Long.MAX_VALUE ) )
            .dynamic()
            .build();

    @Description( "Maximum number of history files for the query log." )
    public static final Setting<Integer> log_queries_max_archives =
            newBuilder( "dbms.logs.query.rotation.keep_number", INT, 7 ).addConstraint( min( 1 ) ).dynamic().build();

    @Description( "Obfuscates all literals of the query before writing to the log. " +
                  "Note that node labels, relationship types and map property keys are still shown. " +
                  "Changing the setting will not affect queries that are cached. So, if you want the switch " +
                  "to have immediate effect, you must also call `CALL db.clearQueryCaches()`." )
    public static final Setting<Boolean> log_queries_obfuscate_literals =
            newBuilder( "dbms.logs.query.obfuscate_literals", BOOL, false ).dynamic().build();

    @Description( "Log query plan description table, useful for debugging purposes." )
    @DocumentedDefaultValue( "false" )
    public static final Setting<Boolean> log_queries_query_plan =
            newBuilder( "dbms.logs.query.plan_description_enabled", BOOL, false ).dynamic().build();

    // Security settings

    @Description( "Enable auth requirement to access Neo4j." )
    @DocumentedDefaultValue( "true" ) // Should document server defaults.
    public static final Setting<Boolean> auth_enabled = newBuilder( "dbms.security.auth_enabled", BOOL, false ).build();

    @Description( "The maximum number of unsuccessful authentication attempts before imposing a user lock for  " +
            "the configured amount of time, as defined by `dbms.security.auth_lock_time`." +
            "The locked out user will not be able to log in until the lock period expires, even if correct  " +
            "credentials are provided. " +
            "Setting this configuration option to values less than 3 is not recommended because it might make  " +
            "it easier for an attacker to brute force the password." )
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

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "A list of procedures (comma separated) that are to be loaded. " +
            "The list may contain both fully-qualified procedure names, and partial names with the wildcard '*'. " +
            "If this setting is left empty no procedures will be loaded. " +
            "Deprecated, use dbms.security.procedures.allowlist" )
    public static final Setting<List<String>> procedure_whitelist =
            newBuilder( "dbms.security.procedures.whitelist", listOf( STRING ), List.of("*") ).build();

    @Description( "A list of procedures (comma separated) that are to be loaded. " +
            "The list may contain both fully-qualified procedure names, and partial names with the wildcard '*'. " +
            "If this setting is left empty no procedures will be loaded." )
    public static final Setting<List<String>> procedure_allowlist =
            newBuilder( "dbms.security.procedures.allowlist", listOf( STRING ), List.of("*") ).build();

    //=========================================================================
    // Procedure security settings
    //=========================================================================

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "The default role that can execute all procedures and user-defined functions that are not covered " +
            "by the `" + "dbms.security.procedures.roles" + "` setting. " +
            "This setting (if not empty string) will be translated to 'GRANT EXECUTE BOOSTED PROCEDURE *' " +
            "and 'GRANT EXECUTE BOOSTED FUNCTION *' for that role. " +
            "If `" + "dbms.security.procedures.roles" + "`is not empty, any procedure or function that this role is not mapped to will result in a " +
            "'DENY EXECUTE BOOSTED PROCEDURE name' and 'DENY EXECUTE BOOSTED FUNCTION name' for this role. " +
            "Any privilege mapped in this way cannot be revoked, instead the config must be changed and will take effect after a restart." +
            "Deprecated: Replaced by EXECUTE PROCEDURE, EXECUTE BOOSTED PROCEDURE, EXECUTE FUNCTION and EXECUTE BOOSTED FUNCTION privileges." )
    public static final Setting<String> default_allowed = newBuilder( "dbms.security.procedures.default_allowed", STRING, "" ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "This provides a finer level of control over which roles can execute procedures than the " +
            "`" + "dbms.security.procedures.default_allowed" + "` setting. For example: `+dbms.security.procedures.roles=" +
            "apoc.convert.*:reader;apoc.load.json*:writer;apoc.trigger.add:TriggerHappy+` will allow the role " +
            "`reader` to execute all procedures in the `apoc.convert` namespace, the role `writer` to execute " +
            "all procedures in the `apoc.load` namespace that starts with `json` and the role `TriggerHappy` " +
            "to execute the specific procedure `apoc.trigger.add`. Procedures not matching any of these " +
            "patterns will be subject to the `" + "dbms.security.procedures.default_allowed" + "` setting. " +
            "This setting (if not empty string) will be translated to 'GRANT EXECUTE BOOSTED PROCEDURE name' and " +
            "'GRANT EXECUTE BOOSTED FUNCTION name' privileges for the mapped roles. " +
            "Any privilege mapped in this way cannot be revoked, instead the config must be changed and will take effect after a restart." +
            "Deprecated: Replaced by EXECUTE PROCEDURE, EXECUTE BOOSTED PROCEDURE, EXECUTE FUNCTION and EXECUTE BOOSTED FUNCTION privileges." )
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

    @Description( "The maximum amount of time to wait for the database state represented by the bookmark." )
    public static final Setting<Duration> bookmark_ready_timeout =
            newBuilder( "dbms.transaction.bookmark_ready_timeout", DURATION, ofSeconds( 30 ) )
                    .addConstraint( min( ofSeconds( 1 ) ) )
                    .dynamic()
                    .build();

    @Description( "How long callers should cache the response of the routing procedure `dbms.routing.getRoutingTable()`" )
    public static final Setting<Duration> routing_ttl =
            newBuilder( "dbms.routing_ttl", DURATION, ofSeconds( 300 ) ).addConstraint( min( ofSeconds( 1 ) ) ).build();

    @Description( "Limit the amount of memory that all of the running transactions can consume, in bytes (or kilobytes with the 'k' " +
            "suffix, megabytes with 'm' and gigabytes with 'g'). Zero means 'unlimited'." )
    public static final Setting<Long> memory_transaction_global_max_size =
            newBuilder( "dbms.memory.transaction.global_max_size", BYTES, 0L )
                    .addConstraint( any( min( mebiBytes( 10 ) ), is( 0L) ) )
                    .dynamic().build();

    @Description( "Limit the amount of memory that all transactions in one database can consume, in bytes (or kilobytes with the 'k' " +
            "suffix, megabytes with 'm' and gigabytes with 'g'). Zero means 'unlimited'." )
    public static final Setting<Long> memory_transaction_database_max_size =
            newBuilder( "dbms.memory.transaction.database_max_size", BYTES, 0L )
                    .addConstraint( any( min( mebiBytes( 10 ) ), is( 0L) ) )
                    .dynamic().build();

    @Description( "Limit the amount of memory that a single transaction can consume, in bytes (or kilobytes with the 'k' " +
                  "suffix, megabytes with 'm' and gigabytes with 'g'). Zero means 'largest possible value'. " +
                  "When `dbms.mode=CORE` or `dbms.mode=SINGLE` and `dbms.clustering.enable=true` this is '2G', in other cases this is 'unlimited'." )
    public static final Setting<Long> memory_transaction_max_size =
            newBuilder( "dbms.memory.transaction.max_size", BYTES, 0L )
                    .addConstraint( any( min( mebiBytes( 1 ) ), is( 0L ) ) )
                    .addConstraint( ifPrimary( max( gibiBytes( 2 ) ) ) )
                    .dynamic().build();

    @Description( "Enable off heap and on heap memory tracking. Should not be set to `false` for clusters." )
    public static final Setting<Boolean> memory_tracking = newBuilder( "dbms.memory.tracking.enable", BOOL, true ).build();

    public enum TransactionStateMemoryAllocation
    {
        ON_HEAP, OFF_HEAP
    }

    @Description( "Defines whether memory for transaction state should be allocated on- or off-heap. " +
                  "Note that for small transactions you can gain up to 25% write speed by setting it to `ON_HEAP`." )
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

    @Description( "Enable server-side routing in clusters using an additional bolt connector.\n" +
                  "When configured, this allows requests to be forwarded from one cluster member to another, if the requests can't be " +
                  "satisfied by the first member (e.g. write requests received by a non-leader)." )
    public static final Setting<Boolean> routing_enabled =
            newBuilder( "dbms.routing.enabled", BOOL, false )
                    .build();

    @Description( "The address the routing connector should bind to" )
    public static final Setting<SocketAddress> routing_listen_address =
            newBuilder( "dbms.routing.listen_address", SOCKET_ADDRESS, new SocketAddress( DEFAULT_ROUTING_CONNECTOR_PORT ) )
                    .setDependency( default_listen_address )
                    .build();

    @Description( "Sets level for driver internal logging." )
    @DocumentedDefaultValue( "Value of dbms.logs.debug.level" )
    public static final Setting<Level> routing_driver_logging_level =
            newBuilder( "dbms.routing.driver.logging.level", ofEnum(Level.class), null ).build();

    @Description( "Maximum total number of connections to be managed by a connection pool.\n" +
            "The limit is enforced for a combination of a host and user. Negative values are allowed and result in unlimited pool. Value of 0" +
            "is not allowed." )
    @DocumentedDefaultValue( "Unlimited" )
    public static final Setting<Integer> routing_driver_max_connection_pool_size =
            newBuilder( "dbms.routing.driver.connection.pool.max_size", INT, -1 ).build();

    @Description( "Pooled connections that have been idle in the pool for longer than this timeout " +
            "will be tested before they are used again, to ensure they are still alive.\n" +
            "If this option is set too low, an additional network call will be incurred when acquiring a connection, which causes a performance hit.\n" +
            "If this is set high, no longer live connections might be used which might lead to errors.\n" +
            "Hence, this parameter tunes a balance between the likelihood of experiencing connection problems and performance\n" +
            "Normally, this parameter should not need tuning.\n" +
            "Value 0 means connections will always be tested for validity" )
    @DocumentedDefaultValue( "No connection liveliness check is done by default." )
    public static final Setting<Duration> routing_driver_idle_time_before_connection_test =
            newBuilder( "dbms.routing.driver.connection.pool.idle_test", DURATION, null ).build();

    @Description( "Pooled connections older than this threshold will be closed and removed from the pool.\n" +
            "Setting this option to a low value will cause a high connection churn and might result in a performance hit.\n" +
            "It is recommended to set maximum lifetime to a slightly smaller value than the one configured in network\n" +
            "equipment (load balancer, proxy, firewall, etc. can also limit maximum connection lifetime).\n" +
            "Zero and negative values result in lifetime not being checked." )
    public static final Setting<Duration> routing_driver_max_connection_lifetime =
            newBuilder( "dbms.routing.driver.connection.max_lifetime", DURATION, Duration.ofHours( 1 ) ).build();

    @Description( "Maximum amount of time spent attempting to acquire a connection from the connection pool.\n" +
            "This timeout only kicks in when all existing connections are being used and no new " +
            "connections can be created because maximum connection pool size has been reached.\n" +
            "Error is raised when connection can't be acquired within configured time.\n" +
            "Negative values are allowed and result in unlimited acquisition timeout. Value of 0 is allowed " +
            "and results in no timeout and immediate failure when connection is unavailable" )
    public static final Setting<Duration> routing_driver_connection_acquisition_timeout =
            newBuilder( "dbms.routing.driver.connection.pool.acquisition_timeout", DURATION, ofSeconds( 60 ) ).build();

    @Description( "Socket connection timeout.\n" +
            "A timeout of zero is treated as an infinite timeout and will be bound by the timeout configured on the\n" +
            "operating system level." )
    public static final Setting<Duration> routing_driver_connect_timeout =
            newBuilder( "dbms.routing.driver.connection.connect_timeout", DURATION, ofSeconds( 5 ) ).build();

    @Description( "Determines which driver API will be used. ASYNC must be used when the remote instance is 3.5" )
    public static final Setting<DriverApi> routing_driver_api =
            newBuilder( "dbms.routing.driver.api", ofEnum( DriverApi.class), DriverApi.RX ).build();

    /**
     * Default settings for connectors. The default values are assumes to be default for embedded deployments through the code.
     * This map contains default connector settings that you can pass to the builders.
     */
    public static final Map<Setting<?>,Object> SERVER_DEFAULTS = Maps.mutable
            .withMap( ConnectorDefaults.SERVER_CONNECTOR_DEFAULTS )
            .withKeyValue( auth_enabled, true );

    public enum Mode
    {
        SINGLE,
        CORE,
        READ_REPLICA
    }

    public enum DriverApi
    {
        RX,
        ASYNC
    }

    public enum RoutingMode
    {
        SERVER,
        CLIENT
    }
}
