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

import static java.lang.Runtime.getRuntime;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Map.entry;
import static org.neo4j.configuration.Config.DEFAULT_CONFIG_DIR_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation.ON_HEAP;
import static org.neo4j.configuration.SettingConstraints.ABSOLUTE_PATH;
import static org.neo4j.configuration.SettingConstraints.HOSTNAME_ONLY;
import static org.neo4j.configuration.SettingConstraints.NO_ALL_INTERFACES_ADDRESS;
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
import static org.neo4j.configuration.SettingValueParsers.setOf;
import static org.neo4j.configuration.connectors.ConnectorDefaults.SERVER_CONNECTOR_DEFAULTS;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.neo4j.annotations.api.PublicApi;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogTimeZone;

@ServiceProvider
@PublicApi
public class GraphDatabaseSettings implements SettingsDeclaration {
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

    @Description(
            "Root relative to which directory settings are resolved. Calculated and set by the server on startup.\n"
                    + "Defaults to the current working directory.")
    public static final Setting<Path> neo4j_home = newBuilder(
                    "server.directories.neo4j_home", PATH, Path.of("").toAbsolutePath())
            .addConstraint(ABSOLUTE_PATH)
            .immutable()
            .build();

    @Description("Name of the default database (aliases are not supported).")
    public static final Setting<String> initial_default_database = newBuilder(
                    "initial.dbms.default_database", DATABASENAME, DEFAULT_DATABASE_NAME)
            .build();

    public static final String DATA_DIRECTORY_SETTING_NAME = "server.directories.data";

    @Description("Path of the data directory. You must not configure more than one Neo4j installation to use the "
            + "same data directory.")
    public static final Setting<Path> data_directory = newBuilder(
                    DATA_DIRECTORY_SETTING_NAME, PATH, Path.of(DEFAULT_DATA_DIR_NAME))
            .setDependency(neo4j_home)
            .immutable()
            .build();

    public static final String TRANSACTION_LOGS_ROOT_PATH_SETTING_NAME = "server.directories.transaction.logs.root";

    @Description("Root location where Neo4j will store transaction logs for configured databases.")
    public static final Setting<Path> transaction_logs_root_path = newBuilder(
                    TRANSACTION_LOGS_ROOT_PATH_SETTING_NAME, PATH, Path.of(DEFAULT_TX_LOGS_ROOT_DIR_NAME))
            .setDependency(data_directory)
            .immutable()
            .build();

    @Description("Root location where Neo4j will store scripts for configured databases.")
    public static final Setting<Path> script_root_path = newBuilder(
                    "server.directories.script.root", PATH, Path.of(DEFAULT_SCRIPT_FOLDER))
            .setDependency(data_directory)
            .immutable()
            .build();

    @Description(
            "Root location where Neo4j will store database dumps optionally produced when dropping said databases.")
    public static final Setting<Path> database_dumps_root_path = newBuilder(
                    "server.directories.dumps.root", PATH, Path.of(DEFAULT_DUMPS_DIR_NAME))
            .setDependency(data_directory)
            .immutable()
            .build();

    @Description(
            "Whether or not any database on this instance are read_only by default. If false, individual databases may be marked as read_only using "
                    + "server.database.read_only. If true, individual databases may be marked as writable using server.databases.writable.")
    public static final Setting<Boolean> read_only_database_default = newBuilder(
                    "server.databases.default_to_read_only", BOOL, false)
            .dynamic()
            .build();

    @Description(
            "List of databases for which to prevent write queries. Databases not included in this list maybe read_only anyway depending upon the value "
                    + "of server.databases.default_to_read_only.")
    public static final Setting<Set<String>> read_only_databases = newBuilder(
                    "server.databases.read_only", setOf(DATABASENAME), emptySet())
            .dynamic()
            .build();

    @Description(
            "List of databases for which to allow write queries. Databases not included in this list will allow write queries anyway, unless "
                    + "server.databases.default_to_read_only is set to true.")
    public static final Setting<Set<String>> writable_databases = newBuilder(
                    "server.databases.writable", setOf(DATABASENAME), emptySet())
            .dynamic()
            .build();

    @Description("A strict configuration validation will prevent the database from starting up if unknown "
            + "configuration options are specified in the neo4j settings namespace (such as dbms., cypher., etc) "
            + "or if settings are declared multiple times.")
    public static final Setting<Boolean> strict_config_validation =
            newBuilder("server.config.strict_validation.enabled", BOOL, true).build();

    @Description(
            "Database format. This is the format that will be used for new databases. Valid values are `standard`, `aligned`, `high_limit` or `block`."
                    + "The `aligned` format is essentially the `standard` format with some minimal padding at the end of pages such that a single "
                    + "record will never cross a page boundary. The `high_limit` and `block` formats are available for Enterprise Edition only. "
                    + "Either `high_limit` or `block` is required if you have a graph that is larger than 34 billion nodes, 34 billion relationships, or 68 billion properties.")
    public static final Setting<String> db_format =
            newBuilder("db.format", STRING, "aligned").dynamic().build();

    @Description("Routing strategy for neo4j:// protocol connections.\n"
            + "Default is `CLIENT`, using client-side routing, with server-side routing as a fallback (if enabled).\n"
            + "When set to `SERVER`, client-side routing is short-circuited, and requests will rely on server-side routing "
            + "(which must be enabled for proper operation, i.e. `dbms.routing.enabled=true`).\n"
            + "Can be overridden by `dbms.routing.client_side.enforce_for_domains`.")
    public static final Setting<RoutingMode> routing_default_router = newBuilder(
                    "dbms.routing.default_router", ofEnum(RoutingMode.class), RoutingMode.CLIENT)
            .build();

    @Description(
            "Always use client side routing (regardless of the default router) for neo4j:// protocol connections to these domains. "
                    + "A comma separated list of domains. Wildcards (*) are supported.")
    public static final Setting<Set<String>> client_side_router_enforce_for_domains = newBuilder(
                    "dbms.routing.client_side.enforce_for_domains", setOf(STRING), Set.of())
            .dynamic()
            .build();

    // Cypher settings

    @Description("If set to `true` a textual representation of the plan description will be rendered on the "
            + "server for all queries running with `EXPLAIN` or `PROFILE`. This allows clients such as the neo4j "
            + "browser and Cypher shell to show a more detailed plan description.")
    public static final Setting<Boolean> cypher_render_plan_descriptions = newBuilder(
                    "dbms.cypher.render_plan_description", BOOL, false)
            .dynamic()
            .build();

    public enum CypherPlanner {
        DEFAULT,
        COST
    }

    @Description(
            "Number of threads to allocate to Cypher worker threads for the parallel runtime. If set to a positive number, "
                    + "that number of workers will be started. If set to 0, one worker will be started for every logical processor "
                    + "available to the Java Virtual Machine. If set to a negative number, the total number of logical processors "
                    + "available on the server will be reduced by the absolute value of that number. "
                    + "For example, if the server has 16 available processors and you set `server.cypher.parallel.worker_limit` to `-1`, "
                    + "the parallel runtime will have 15 threads available.")
    public static final Setting<Integer> cypher_worker_limit =
            newBuilder("server.cypher.parallel.worker_limit", INT, 0).build();

    @Description("Set this to specify the default planner for the default language version.")
    public static final Setting<CypherPlanner> cypher_planner = newBuilder(
                    "dbms.cypher.planner", ofEnum(CypherPlanner.class), CypherPlanner.DEFAULT)
            .build();

    @Description("Set this to specify the behavior when Cypher planner or runtime hints cannot be fulfilled. "
            + "If true, then non-conformance will result in an error, otherwise only a warning is generated.")
    public static final Setting<Boolean> cypher_hints_error =
            newBuilder("dbms.cypher.hints_error", BOOL, false).build();

    @Description("This setting is associated with performance optimization. Set this to `true` in situations where "
            + "it is preferable to have any queries using the 'shortestPath' function terminate as soon as "
            + "possible with no answer, rather than potentially running for a long time attempting to find an "
            + "answer (even if there is no path to be found). "
            + "For most queries, the 'shortestPath' algorithm will return the correct answer very quickly. However "
            + "there are some cases where it is possible that the fast bidirectional breadth-first search "
            + "algorithm will find no results even if they exist. This can happen when the predicates in the "
            + "`WHERE` clause applied to 'shortestPath' cannot be applied to each step of the traversal, and can "
            + "only be applied to the entire path. When the query planner detects these special cases, it will "
            + "plan to perform an exhaustive depth-first search if the fast algorithm finds no paths. However, "
            + "the exhaustive search may be orders of magnitude slower than the fast algorithm. If it is critical "
            + "that queries terminate as soon as possible, it is recommended that this option be set to `true`, "
            + "which means that Neo4j will never consider using the exhaustive search for shortestPath queries. "
            + "However, please note that if no paths are found, an error will be thrown at run time, which will "
            + "need to be handled by the application.")
    public static final Setting<Boolean> forbid_exhaustive_shortestpath = newBuilder(
                    "dbms.cypher.forbid_exhaustive_shortestpath", BOOL, false)
            .build();

    @Description("This setting is associated with performance optimization. The shortest path algorithm does not "
            + "work when the start and end nodes are the same. With this setting set to `false` no path will "
            + "be returned when that happens. The default value of `true` will instead throw an exception. "
            + "This can happen if you perform a shortestPath search after a cartesian product that might have "
            + "the same start and end nodes for some of the rows passed to shortestPath. If it is preferable "
            + "to not experience this exception, and acceptable for results to be missing for those rows, then "
            + "set this to `false`. If you cannot accept missing results, and really want the shortestPath "
            + "between two common nodes, then re-write the query using a standard Cypher variable length pattern "
            + "expression followed by ordering by path length and limiting to one result.")
    public static final Setting<Boolean> forbid_shortestpath_common_nodes = newBuilder(
                    "dbms.cypher.forbid_shortestpath_common_nodes", BOOL, true)
            .build();

    @Description(
            "Set this to change the behavior for Cypher create relationship when the start or end node is missing.\n"
                    + "By default this fails the query and stops execution, but by setting this flag the create operation is "
                    + "simply not performed and execution continues.")
    public static final Setting<Boolean> cypher_lenient_create_relationship =
            newBuilder("dbms.cypher.lenient_create_relationship", BOOL, false).build();

    @Deprecated(since = "5.7.0", forRemoval = true)
    @Description("The number of cached queries per database. "
            + "Use `server.memory.query_cache.per_db_cache_num_entries` instead.")
    public static final Setting<Integer> deprecated_query_cache_size = newBuilder(
                    "server.db.query_cache_size", INT, 1000)
            .addConstraint(min(0))
            .build();

    @Description(
            "The number of cached queries per database. "
                    + "The max number of queries that can be kept in a cache is `number of databases` * `server.memory.query_cache.per_db_cache_num_entries`. "
                    + "With 10 databases and `server.memory.query_cache.per_db_cache_num_entries`=1000, the cache can keep 10000 plans in total. "
                    + "This setting is only deciding cache size when `server.memory.query_cache.sharing_enabled` is set to `false`.")
    public static final Setting<Integer> query_cache_size = newBuilder(
                    "server.memory.query_cache.per_db_cache_num_entries", INT, 1000)
            .addConstraint(min(0))
            .dynamic()
            .build();

    @Description("The threshold for statistics above which a plan is considered stale.\n\n"
            + "If any of the underlying statistics used to create the plan have changed more than this value, "
            + "the plan will be considered stale and will be replanned. Change is calculated as "
            + "`abs(a-b)/max(a,b)`.\n\n"
            + "This means that a value of `0.75` requires the database to "
            + "quadruple in size before query replanning. A value of `0` means that the query will be "
            + "replanned as soon as there is any change in statistics and the replan interval has elapsed.\n\n"
            + "This interval is defined by `dbms.cypher.min_replan_interval` and defaults to 10s. After this interval, the "
            + "divergence threshold will slowly start to decline, reaching 10% after about 7h. This will "
            + "ensure that long running databases will still get query replanning on even modest changes, "
            + "while not replanning frequently unless the changes are very large.")
    public static final Setting<Double> query_statistics_divergence_threshold = newBuilder(
                    "dbms.cypher.statistics_divergence_threshold", DOUBLE, 0.75)
            .addConstraint(range(0.0, 1.0))
            .build();

    @Description("The minimum time between possible cypher query replanning events. After this time, the graph "
            + "statistics will be evaluated, and if they have changed by more than the value set by "
            + "dbms.cypher.statistics_divergence_threshold, the query will be replanned. If the statistics have "
            + "not changed sufficiently, the same interval will need to pass before the statistics will be "
            + "evaluated again. Each time they are evaluated, the divergence threshold will be reduced slightly "
            + "until it reaches 10% after 7h, so that even moderately changing databases will see query replanning "
            + "after a sufficiently long time interval.")
    public static final Setting<Duration> cypher_min_replan_interval = newBuilder(
                    "dbms.cypher.min_replan_interval", DURATION, ofSeconds(10))
            .build();

    @Description("Determines if Cypher will allow using file URLs when loading data using `LOAD CSV`. Setting this "
            + "value to `false` will cause Neo4j to fail `LOAD CSV` clauses that load data from the file system.")
    public static final Setting<Boolean> allow_file_urls = newBuilder(
                    "dbms.security.allow_csv_import_from_file_urls", BOOL, true)
            .build();

    @Description(
            "Sets the root directory for file URLs used with the Cypher `LOAD CSV` clause. This should be set to a "
                    + "directory relative to the Neo4j installation path, restricting access to only those files within that directory "
                    + "and its subdirectories. For example the value \"import\" will only enable access to files within the 'import' folder. "
                    + "Removing this setting will disable the security feature, allowing all files in the local system to be imported. "
                    + "Setting this to an empty field will allow access to all files within the Neo4j installation folder.")
    public static final Setting<Path> load_csv_file_url_root = newBuilder("server.directories.import", PATH, null)
            .immutable()
            .setDependency(neo4j_home)
            .build();

    @Description("Selects whether to conform to the standard https://tools.ietf.org/html/rfc4180 for interpreting "
            + "escaped quotation characters in CSV files loaded using `LOAD CSV`. Setting this to `false` will use"
            + " the standard, interpreting repeated quotes '\"\"' as a single in-lined quote, while `true` will "
            + "use the legacy convention originally supported in Neo4j 3.0 and 3.1, allowing a backslash to "
            + "include quotes in-lined in fields.")
    public static final Setting<Boolean> csv_legacy_quote_escaping =
            newBuilder("db.import.csv.legacy_quote_escaping", BOOL, true).build();

    @Description("The size of the internal buffer in bytes used by `LOAD CSV`. If the csv file contains huge fields "
            + "this value may have to be increased.")
    public static final Setting<Long> csv_buffer_size = newBuilder("db.import.csv.buffer_size", LONG, mebiBytes(2))
            .addConstraint(min(1L))
            .build();

    @Description("Enables or disables tracking of how much time a query spends actively executing on the CPU. "
            + "Calling `SHOW TRANSACTIONS` will display the time.")
    public static final Setting<Boolean> track_query_cpu_time =
            newBuilder("db.track_query_cpu_time", BOOL, false).dynamic().build();

    @Description("The maximum number of concurrently running transactions. If set to 0, limit is disabled.")
    public static final Setting<Integer> max_concurrent_transactions =
            newBuilder("db.transaction.concurrent.maximum", INT, 1000).dynamic().build();

    public enum TransactionTracingLevel {
        DISABLED,
        SAMPLE,
        ALL
    }

    @Description("Transaction creation tracing level.")
    public static final Setting<TransactionTracingLevel> transaction_tracing_level = newBuilder(
                    "db.transaction.tracing.level",
                    ofEnum(TransactionTracingLevel.class),
                    TransactionTracingLevel.DISABLED)
            .dynamic()
            .build();

    @Description("Transaction sampling percentage.")
    public static final Setting<Integer> transaction_sampling_percentage = newBuilder(
                    "db.transaction.sampling.percentage", INT, 5)
            .dynamic()
            .addConstraint(range(1, 100))
            .build();

    // @see Status.Transaction#TransactionTimedOut
    @Description("The maximum time interval of a transaction within which it should be completed.")
    public static final Setting<Duration> transaction_timeout = newBuilder(
                    "db.transaction.timeout", DURATION, Duration.ZERO)
            .dynamic()
            .build();

    // @see Status.Transaction#LockAcquisitionTimeout
    @Description(
            "The maximum time interval within which lock should be acquired. Zero (default) means timeout is disabled.")
    public static final Setting<Duration> lock_acquisition_timeout = newBuilder(
                    "db.lock.acquisition.timeout", DURATION, Duration.ZERO)
            .dynamic()
            .build();

    @Description("Configures the time interval between transaction monitor checks. Determines how often "
            + "monitor thread will check transaction for timeout.")
    public static final Setting<Duration> transaction_monitor_check_interval = newBuilder(
                    "db.transaction.monitor.check.interval", DURATION, ofSeconds(2))
            .build();

    @Description("The maximum amount of time to wait for running transactions to complete before allowing "
            + "initiated database shutdown to continue")
    public static final Setting<Duration> shutdown_transaction_end_timeout = newBuilder(
                    "db.shutdown_transaction_end_timeout", DURATION, ofSeconds(10))
            .build();

    @Description("Location of the database plugin directory. Compiled Java JAR files that contain database "
            + "procedures will be loaded if they are placed in this directory.")
    public static final Setting<Path> plugin_dir = newBuilder("server.directories.plugins", PATH, Path.of("plugins"))
            .setDependency(neo4j_home)
            .immutable()
            .build();

    @Description("Database timezone. Among other things, this setting influences the monitoring procedures.")
    public static final Setting<LogTimeZone> db_timezone = newBuilder(
                    "dbms.db.timezone", ofEnum(LogTimeZone.class), LogTimeZone.UTC)
            .build();

    @Description("Database timezone for temporal functions. All Time and DateTime values that are created without "
            + "an explicit timezone will use this configured default timezone.")
    public static final Setting<ZoneId> db_temporal_timezone =
            newBuilder("db.temporal.timezone", TIMEZONE, ZoneOffset.UTC).build();

    public enum CheckpointPolicy {
        PERIODIC,
        CONTINUOUS,
        VOLUME,
        VOLUMETRIC
    }

    @Description(
            """
            Configures the general policy for when checkpoints should occur.
            Possible values are:

            * `PERIODIC` (default) -- it runs a checkpoint as per the interval specified by
              `<<config_db.checkpoint.interval.tx,db.checkpoint.interval.tx>>` and
              `<<config_db.checkpoint.interval.time,db.checkpoint.interval.time>>`.

            * `VOLUME` -- it runs a checkpoint when the size of the transaction logs reaches
              the value specified by the `<<config_db.checkpoint.interval.volume,db.checkpoint.interval.volume>>` setting.
              By default, it is set to `250.00MiB`.

            * `CONTINUOUS` (Enterprise Edition) -- it ignores `<<config_db.checkpoint.interval.tx,db.checkpoint.interval.tx>>`
              and `<<config_db.checkpoint.interval.time,db.checkpoint.interval.time>>` settings and runs the checkpoint process all the time.

            * `VOLUMETRIC` -- it makes the best effort to checkpoint often enough so that the database does not get too far behind on
              deleting old transaction logs as specified in the `<<config_db.tx_log.rotation.retention_policy,db.tx_log.rotation.retention_policy>>` setting.
            """)
    public static final Setting<CheckpointPolicy> check_point_policy = newBuilder(
                    "db.checkpoint", ofEnum(CheckpointPolicy.class), CheckpointPolicy.PERIODIC)
            .build();

    @Description("Configures the transaction interval between checkpoints. The database does not checkpoint more "
            + "often the specified interval (unless checkpointing is triggered by a different event), but might checkpoint "
            + "less often if performing a checkpoint takes longer time than the configured interval. "
            + "A checkpoint is a point in the transaction logs from which recovery starts. "
            + "Longer checkpoint intervals typically mean that recovery takes longer to complete in case "
            + "of a crash. On the other hand, a longer checkpoint interval can also reduce the I/O load that "
            + "the database places on the system, as each checkpoint implies a flushing and forcing of all the "
            + "store files.  The default is `100000` for a checkpoint every 100000 transactions.")
    public static final Setting<Integer> check_point_interval_tx = newBuilder("db.checkpoint.interval.tx", INT, 100000)
            .addConstraint(min(1))
            .build();

    @Description("Configures the time interval between checkpoints. The database does not checkpoint more "
            + "often the specified interval (unless checkpointing is triggered by a different event), but might checkpoint "
            + "less often if performing a checkpoint takes longer time than the configured interval. "
            + "A checkpoint is a point in the transaction logs from which recovery starts. "
            + "Longer checkpoint intervals typically mean that recovery takes longer to complete in case "
            + "of a crash. On the other hand, a longer checkpoint interval can also reduce the I/O load that "
            + "the database places on the system, as each checkpoint implies a flushing and forcing of all the "
            + "store files.")
    public static final Setting<Duration> check_point_interval_time =
            newBuilder("db.checkpoint.interval.time", DURATION, ofMinutes(15)).build();

    @Description("Configures the volume of transaction logs between checkpoints. The database does not checkpoint more "
            + "often the specified interval (unless checkpointing is triggered by a different event), but might checkpoint "
            + "less often if performing a checkpoint takes longer time than the configured interval. "
            + "A checkpoint is a point in the transaction logs from which recovery starts. "
            + "Longer checkpoint intervals typically mean that recovery takes longer to complete in case "
            + "of a crash. On the other hand, a longer checkpoint interval can also reduce the I/O load that "
            + "the database places on the system, as each checkpoint implies a flushing and forcing of all the "
            + "store files.")
    public static final Setting<Long> check_point_interval_volume = newBuilder(
                    "db.checkpoint.interval.volume", BYTES, mebiBytes(250))
            .addConstraint(min(ByteUnit.kibiBytes(1)))
            .build();

    @Description("Limit the number of IOs the background checkpoint process consumes per second. "
            + "This setting is advisory. It is ignored in Neo4j Community Edition and is followed to "
            + "best effort in Enterprise Edition. "
            + "An IO is, in this case, an 8 KiB (mostly sequential) write. Limiting the write IO in "
            + "this way leaves more bandwidth in the IO subsystem to service random-read IOs, "
            + "which is important for the response time of queries when the database cannot fit "
            + "entirely in memory. The only drawback of this setting is that longer checkpoint times "
            + "may lead to slightly longer recovery times in case of a database or system crash. "
            + "A lower number means lower IO pressure and, consequently, longer checkpoint times. "
            + "Set this to -1 to disable the IOPS limit and remove the limitation entirely. "
            + "This lets the checkpointer flush data as fast as the hardware goes. "
            + "Removing or commenting out the setting sets the default value of 600.")
    public static final Setting<Integer> check_point_iops_limit =
            newBuilder("db.checkpoint.iops.limit", INT, 600).dynamic().build();

    // Index sampling
    @Description("Enable or disable background index sampling")
    public static final Setting<Boolean> index_background_sampling_enabled =
            newBuilder("db.index_sampling.background_enabled", BOOL, true).build();

    @Description("Index sampling chunk size limit")
    public static final Setting<Integer> index_sample_size_limit = newBuilder(
                    "db.index_sampling.sample_size_limit", INT, (int) mebiBytes(8))
            .addConstraint(range((int) mebiBytes(1), Integer.MAX_VALUE))
            .build();

    @Description("Percentage of index updates of total index size required before sampling of a given index is "
            + "triggered")
    public static final Setting<Integer> index_sampling_update_percentage = newBuilder(
                    "db.index_sampling.update_percentage", INT, 5)
            .addConstraint(min(0))
            .build();

    // Store settings
    @Description(
            "Tell Neo4j how long logical transaction logs should be kept to backup the database."
                    + "For example, \"10 days\" will prune logical logs that only contain transactions older than 10 days."
                    + "Alternatively, \"100k txs\" will keep the 100k latest transactions from each database and prune any older transactions.")
    public static final Setting<String> keep_logical_logs = newBuilder(
                    "db.tx_log.rotation.retention_policy", STRING, "2 days")
            .dynamic()
            .addConstraint(SettingConstraints.matches(
                    "^(true|keep_all|false|keep_none|(\\d+[KkMmGg]?( (files|size|txs|entries|hours( \\d+[KkMmGg]?)?|days( \\d+[KkMmGg]?)?))))$",
                    "Must be `true` or `keep_all`, `false` or `keep_none`, or of format `<number><optional unit> <type> <optional space restriction>`. "
                            + "Valid units are `K`, `M` and `G`. "
                            + "Valid types are `files`, `size`, `txs`, `entries`, `hours` and `days`. "
                            + "Valid optional space restriction is a logical log space restriction like `100M`. "
                            + "For example, `100M size` will limit logical log space on disk to 100MiB per database, "
                            + "and `200K txs` will limit the number of transactions kept to 200 000 per database."))
            .build();

    @Description("Specifies at which file size the logical log will auto-rotate. Minimum accepted value is 128 KiB. ")
    public static final Setting<Long> logical_log_rotation_threshold = newBuilder(
                    "db.tx_log.rotation.size", BYTES, mebiBytes(256))
            .addConstraint(min(kibiBytes(128)))
            .dynamic()
            .build();

    @Description(
            "On serialization of transaction logs, they will be temporary stored in the byte buffer that will be flushed at the end of the transaction "
                    + "or at any moment when buffer will be full.\n"
                    + "By default, the size of byte buffer is based on the number of available CPUs, with "
                    + "a minimum of 512KB. Every additional 4 CPUs add another 512KB into the buffer size. "
                    + "The maximal buffer size in this default scheme is 4MB taking into account "
                    + "that there can be one transaction log writer per database in a multi-database env."
                    + "For example, runtimes with 4 CPUs will have a buffer size of 1MB; "
                    + "runtimes with 8 CPUs will have buffer size of 1.5MB; "
                    + "runtimes with 12 CPUs will have buffer size of 2MB.")
    public static final Setting<Long> transaction_log_buffer_size = newBuilder(
                    "db.tx_log.buffer.size",
                    LONG,
                    ByteUnit.kibiBytes(Math.min((getRuntime().availableProcessors() / 4) + 1, 8) * 512L))
            .addConstraint(min(kibiBytes(128)))
            .build();

    @Description("Specify if Neo4j should try to preallocate logical log file in advance.")
    public static final Setting<Boolean> preallocate_logical_logs =
            newBuilder("db.tx_log.preallocate", BOOL, true).dynamic().build();

    @Description("Specify if Neo4j should try to preallocate store files as they grow.")
    public static final Setting<Boolean> preallocate_store_files =
            newBuilder("db.store.files.preallocate", BOOL, true).build();

    @Description("If `true`, Neo4j will abort recovery if transaction log files are missing. Setting "
            + "this to `false` will allow Neo4j to create new empty missing files for the already existing  "
            + "database, but the integrity of the database might be compromised.")
    public static final Setting<Boolean> fail_on_missing_files =
            newBuilder("db.recovery.fail_on_missing_files", BOOL, true).build();

    @Description("The amount of memory to use for mapping the store files. If Neo4j is running on a dedicated server, "
            + "then it is generally recommended to leave about 2-4 gigabytes for the operating system, give the "
            + "JVM enough heap to hold all your transaction state and query context, and then leave the rest for "
            + "the page cache. If no page cache memory is configured, then a heuristic setting is computed based "
            + "on available system resources.\n"
            + "By default, the size of page cache is 50% of available RAM minus the max heap size "
            + "(but not larger than 70x the max heap size, due to some overhead of the page cache in the heap).")
    public static final Setting<Long> pagecache_memory =
            newBuilder("server.memory.pagecache.size", BYTES, null).build();

    @Description("The maximum number of worker threads to use for pre-fetching data when doing sequential scans. "
            + "Set to '0' to disable pre-fetching for scans.")
    public static final Setting<Integer> pagecache_scan_prefetch = newBuilder(
                    "server.memory.pagecache.scan.prefetchers", INT, 4)
            .addConstraint(range(0, 255))
            .build();

    @Description(
            "Page cache can be configured to use a temporal buffer for flushing purposes. It is used to combine, if possible, sequence of several "
                    + "cache pages into one bigger buffer to minimize the number of individual IOPS performed and better utilization of available "
                    + "I/O resources, especially when those are restricted.")
    public static final Setting<Boolean> pagecache_buffered_flush_enabled = newBuilder(
                    "server.memory.pagecache.flush.buffer.enabled", BOOL, false)
            .dynamic()
            .build();

    @Description(
            "Page cache can be configured to use a temporal buffer for flushing purposes. It is used to combine, if possible, sequence of several "
                    + "cache pages into one bigger buffer to minimize the number of individual IOPS performed and better utilization of available "
                    + "I/O resources, especially when those are restricted. "
                    + "Use this setting to configure individual file flush buffer size in pages (8KiB). "
                    + "To be able to utilize this buffer during page cache flushing, buffered flush should be enabled.")
    public static final Setting<Integer> pagecache_flush_buffer_size_in_pages = newBuilder(
                    "server.memory.pagecache.flush.buffer.size_in_pages", INT, 128)
            .addConstraint(range(1, 512))
            .dynamic()
            .build();

    @Description("The profiling frequency for the page cache. "
            + "Accurate profiles allow the page cache to do active warmup after a restart, reducing the mean time to performance.\n"
            + "This feature is available in Neo4j Enterprise Edition.")
    public static final Setting<Duration> pagecache_warmup_profiling_interval = newBuilder(
                    "db.memory.pagecache.warmup.profile.interval", DURATION, ofMinutes(1))
            .build();

    @Description(
            "Page cache can be configured to perform usage sampling of loaded pages that can be used to construct active load profile. "
                    + "According to that profile pages can be reloaded on the restart, replication, etc. "
                    + "This setting allows disabling that behavior.\n"
                    + "This feature is available in Neo4j Enterprise Edition.")
    public static final Setting<Boolean> pagecache_warmup_enabled =
            newBuilder("db.memory.pagecache.warmup.enable", BOOL, true).build();

    @Description(
            "Page cache warmup can be configured to prefetch files, preferably when cache size is bigger than store size. "
                    + "Files to be prefetched can be filtered by 'dbms.memory.pagecache.warmup.preload.allowlist'. "
                    + "Enabling this disables warmup by profile ")
    public static final Setting<Boolean> pagecache_warmup_prefetch =
            newBuilder("db.memory.pagecache.warmup.preload", BOOL, false).build();

    @Description("Page cache warmup prefetch file allowlist regex. " + "By default matches all files.")
    public static final Setting<String> pagecache_warmup_prefetch_allowlist = newBuilder(
                    "db.memory.pagecache.warmup.preload.allowlist", STRING, ".*")
            .build();

    @Description(
            "Use direct I/O for page cache. "
                    + "Setting is supported only on Linux and only for a subset of record formats that use platform aligned page size.")
    public static final Setting<Boolean> pagecache_direct_io =
            newBuilder("server.memory.pagecache.directio", BOOL, false).build();

    @Description("Allows the enabling or disabling of the file watcher service. "
            + "This is an auxiliary service but should be left enabled in almost all cases.")
    public static final Setting<Boolean> filewatcher_enabled =
            newBuilder("db.filewatcher.enabled", BOOL, true).build();

    @Description("Relationship count threshold for considering a node to be dense.")
    public static final Setting<Integer> dense_node_threshold = newBuilder(
                    "db.relationship_grouping_threshold", INT, 50)
            .addConstraint(min(1))
            .build();

    @Description(
            """
            Log executed queries. Valid values are `OFF`, `INFO`, or `VERBOSE`.

            `OFF`::  no logging.
            `INFO`:: log queries at the end of execution, that take longer than the configured threshold, `db.logs.query.threshold`.
            `VERBOSE`:: log queries at the start and end of execution, regardless of `db.logs.query.threshold`.

            Log entries are written to the query log.

            This feature is available in the Neo4j Enterprise Edition.""")
    public static final Setting<LogQueryLevel> log_queries = newBuilder(
                    "db.logs.query.enabled", ofEnum(LogQueryLevel.class), LogQueryLevel.VERBOSE)
            .dynamic()
            .build();

    public enum LogQueryLevel {
        OFF,
        INFO,
        VERBOSE
    }

    @Description(
            """
            Log the start and end of a transaction. Valid values are `OFF`, `INFO`, or `VERBOSE`.

            `OFF`::  no logging.
            `INFO`:: log start and end of transactions that take longer than the configured threshold, `db.logs.query.transaction.threshold`.
            `VERBOSE`:: log start and end of all transactions.

            Log entries are written to the query log.
            This feature is available in the Neo4j Enterprise Edition.""")
    public static final Setting<LogQueryLevel> log_queries_transactions_level = newBuilder(
                    "db.logs.query.transaction.enabled", ofEnum(LogQueryLevel.class), LogQueryLevel.OFF)
            .dynamic()
            .build();

    @Deprecated(since = "5.12.0", forRemoval = true)
    @Description(
            """
            Log the annotation data as a JSON strings instead of a cypher map.
            This only have effect when the query log is in JSON format.""")
    public static final Setting<Boolean> log_queries_annotation_data_as_json = newBuilder(
                    "db.logs.query.annotation_data_as_json_enabled", BOOL, false)
            .dynamic()
            .build();

    public enum AnnotationDataFormat {
        CYPHER,
        JSON,
        FLAT_JSON
    }

    @Description(
            """
            The format to use for the JSON annotation data.

            `CYPHER`:: Formatted as a Cypher map. E.g. `{foo: 'bar', baz: {k: 1}}`.
            `JSON`:: Formatted as a JSON map. E.g. `{"foo": "bar", "baz": {"k": 1}}`.
            `FLAT_JSON`:: Formatted as a flattened JSON map. E.g. `{"foo": "bar", "baz.k": 1}`.

            This only have effect when the query log is in JSON format.""")
    public static final Setting<AnnotationDataFormat> log_queries_annotation_data_format = newBuilder(
                    "db.logs.query.annotation_data_format",
                    ofEnum(AnnotationDataFormat.class),
                    AnnotationDataFormat.CYPHER)
            .dynamic()
            .build();

    @Description("Path to the logging configuration for debug, query, http and security logs.")
    public static final Setting<Path> server_logging_config_path = newBuilder(
                    "server.logs.config", PATH, Path.of(DEFAULT_CONFIG_DIR_NAME, "server-logs.xml"))
            .setDependency(neo4j_home)
            .immutable()
            .build();

    @Description("Path to the logging configuration of user logs.")
    public static final Setting<Path> user_logging_config_path = newBuilder(
                    "server.logs.user.config", PATH, Path.of(DEFAULT_CONFIG_DIR_NAME, "user-logs.xml"))
            .setDependency(neo4j_home)
            .immutable()
            .build();

    @Description("Path of the logs directory.")
    public static final Setting<Path> logs_directory = newBuilder("server.directories.logs", PATH, Path.of("logs"))
            .setDependency(neo4j_home)
            .immutable()
            .build();

    @Description("Enable the debug log.")
    public static final Setting<Boolean> debug_log_enabled =
            newBuilder("server.logs.debug.enabled", BOOL, Boolean.TRUE).build();

    @Description("Path of the licenses directory.")
    public static final Setting<Path> licenses_directory = newBuilder(
                    "server.directories.licenses", PATH, Path.of(DEFAULT_LICENSES_DIR_NAME))
            .setDependency(neo4j_home)
            .immutable()
            .build();

    @Description("Log parameters for the executed queries being logged.")
    public static final Setting<Boolean> log_queries_parameter_logging_enabled = newBuilder(
                    "db.logs.query.parameter_logging_enabled", BOOL, true)
            .dynamic()
            .build();

    @Description("Sets a maximum character length use for each parameter in the log. "
            + "This only takes effect if `db.logs.query.parameter_logging_enabled = true`.")
    public static final Setting<Integer> query_log_max_parameter_length = newBuilder(
                    "db.logs.query.max_parameter_length", INT, Integer.MAX_VALUE)
            .dynamic()
            .build();

    @Description("Log query text and parameters without obfuscating passwords. "
            + "This allows queries to be logged earlier before parsing starts.")
    public static final Setting<Boolean> log_queries_early_raw_logging_enabled = newBuilder(
                    "db.logs.query.early_raw_logging_enabled", BOOL, false)
            .dynamic()
            .build();

    @Description("If the execution of query takes more time than this threshold, the query is logged once completed - "
            + "provided query logging is set to INFO. Defaults to 0 seconds, that is all queries are logged.")
    public static final Setting<Duration> log_queries_threshold = newBuilder(
                    "db.logs.query.threshold", DURATION, Duration.ZERO)
            .dynamic()
            .build();

    @Description(
            "If the transaction is open for more time than this threshold, the transaction is logged once completed - "
                    + "provided transaction logging (db.logs.query.transaction.enabled) is set to `INFO`. "
                    + "Defaults to 0 seconds (all transactions are logged).")
    public static final Setting<Duration> log_queries_transaction_threshold = newBuilder(
                    "db.logs.query.transaction.threshold", DURATION, Duration.ZERO)
            .dynamic()
            .build();

    @Description("Obfuscates all literals of the query before writing to the log. "
            + "Note that node labels, relationship types and map property keys are still shown. "
            + "Changing the setting will not affect queries that are cached. So, if you want the switch "
            + "to have immediate effect, you must also call `CALL db.clearQueryCaches()`.")
    public static final Setting<Boolean> log_queries_obfuscate_literals = newBuilder(
                    "db.logs.query.obfuscate_literals", BOOL, false)
            .dynamic()
            .build();

    @Description("Log query plan description table, useful for debugging purposes.")
    public static final Setting<Boolean> log_queries_query_plan = newBuilder(
                    "db.logs.query.plan_description_enabled", BOOL, false)
            .dynamic()
            .build();

    // Security settings

    @Description("Enable auth requirement to access Neo4j.\n" + "Defaults to `true`.")
    public static final Setting<Boolean> auth_enabled =
            newBuilder("dbms.security.auth_enabled", BOOL, false).build();

    @Description("The minimum number of characters required in a password.")
    public static final Setting<Integer> auth_minimum_password_length = newBuilder(
                    "dbms.security.auth_minimum_password_length", INT, 8)
            .addConstraint(min(1))
            .build();

    @Description("The maximum number of unsuccessful authentication attempts before imposing a user lock for  "
            + "the configured amount of time, as defined by `dbms.security.auth_lock_time`."
            + "The locked out user will not be able to log in until the lock period expires, even if correct  "
            + "credentials are provided. "
            + "Setting this configuration option to values less than 3 is not recommended because it might make  "
            + "it easier for an attacker to brute force the password.")
    public static final Setting<Integer> auth_max_failed_attempts = newBuilder(
                    "dbms.security.auth_max_failed_attempts", INT, 3)
            .addConstraint(min(0))
            .build();

    @Description(
            "The amount of time user account should be locked after a configured number of unsuccessful authentication attempts. "
                    + "The locked out user will not be able to log in until the lock period expires, even if correct credentials are provided. "
                    + "Setting this configuration option to a low value is not recommended because it might make it easier for an attacker to "
                    + "brute force the password.")
    public static final Setting<Duration> auth_lock_time = newBuilder(
                    "dbms.security.auth_lock_time", DURATION, ofSeconds(5))
            .addConstraint(min(ofSeconds(0)))
            .build();

    @Description("A list of procedures and user defined functions (comma separated) that are allowed full access to "
            + "the database. The list may contain both fully-qualified procedure names, and partial names with the "
            + "wildcard '*'. Note that this enables these procedures to bypass security. Use with caution.")
    public static final Setting<List<String>> procedure_unrestricted = newBuilder(
                    "dbms.security.procedures.unrestricted", listOf(STRING), emptyList())
            .build();

    @Description("A list of procedures (comma separated) that are to be loaded. "
            + "The list may contain both fully-qualified procedure names, and partial names with the wildcard '*'. "
            + "If this setting is left empty no procedures will be loaded.")
    public static final Setting<List<String>> procedure_allowlist = newBuilder(
                    "dbms.security.procedures.allowlist", listOf(STRING), List.of("*"))
            .build();

    // =========================================================================
    // Procedure security settings
    // =========================================================================

    @Description("Default network interface to listen for incoming connections. "
            + "To listen for connections on all interfaces, use \"0.0.0.0\". ")
    public static final Setting<SocketAddress> default_listen_address = newBuilder(
                    "server.default_listen_address", SOCKET_ADDRESS, new SocketAddress("localhost"))
            .addConstraint(HOSTNAME_ONLY)
            .immutable()
            .build();

    @Description("Default hostname or IP address the server uses to advertise itself.")
    public static final Setting<SocketAddress> default_advertised_address = newBuilder(
                    "server.default_advertised_address", SOCKET_ADDRESS, new SocketAddress("localhost"))
            .addConstraint(HOSTNAME_ONLY)
            .addConstraint(NO_ALL_INTERFACES_ADDRESS)
            .immutable()
            .build();

    @Description("The maximum amount of time to wait for the database state represented by the bookmark.")
    public static final Setting<Duration> bookmark_ready_timeout = newBuilder(
                    "db.transaction.bookmark_ready_timeout", DURATION, ofSeconds(30))
            .addConstraint(min(ofSeconds(1)))
            .dynamic()
            .build();

    @Description("How long callers should cache the response of the routing procedure `dbms.routing.getRoutingTable()`")
    public static final Setting<Duration> routing_ttl = newBuilder("dbms.routing_ttl", DURATION, ofSeconds(300))
            .addConstraint(min(ofSeconds(1)))
            .build();

    @Description(
            "Limit the amount of memory that all of the running transactions can consume, in bytes (or kibibytes with the 'k' "
                    + "suffix, mebibytes with 'm' and gibibytes with 'g'). Zero means 'unlimited'.\n"
                    + "Defaults to 70% of the heap size limit.")
    public static final Setting<Long> memory_transaction_global_max_size = newBuilder(
                    "dbms.memory.transaction.total.max", BYTES, calculateDefaultMaxGlobalTransactionMemorySize())
            .addConstraint(any(min(mebiBytes(10)), is(0L)))
            .dynamic()
            .build();

    private static long calculateDefaultMaxGlobalTransactionMemorySize() {
        long heapLimit =
                ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
        long calculatedLimit = (long) (heapLimit * 0.7);
        // Make sure that the calculated value is not outside the settings constraints.
        return Math.max(calculatedLimit, ByteUnit.mebiBytes(10));
    }

    @Description(
            "Limit the amount of memory that all transactions in one database can consume, in bytes (or kibibytes with the 'k' "
                    + "suffix, mebibytes with 'm' and gibibytes with 'g'). Zero means 'unlimited'.")
    public static final Setting<Long> memory_transaction_database_max_size = newBuilder(
                    "db.memory.transaction.total.max", BYTES, 0L)
            .addConstraint(any(min(mebiBytes(10)), is(0L)))
            .dynamic()
            .build();

    @Description(
            "Limit the amount of memory that a single transaction can consume, in bytes (or kibibytes with the 'k' "
                    + "suffix, mebibytes with 'm' and gibibytes with 'g'). Zero means 'largest possible value'.")
    public static final Setting<Long> memory_transaction_max_size = newBuilder("db.memory.transaction.max", BYTES, 0L)
            .addConstraint(any(min(mebiBytes(1)), is(0L)))
            .dynamic()
            .build();

    @Description("Enable off heap and on heap memory tracking. Should not be set to `false` for clusters.")
    public static final Setting<Boolean> memory_tracking =
            newBuilder("dbms.memory.tracking.enable", BOOL, true).build();

    public enum TransactionStateMemoryAllocation {
        ON_HEAP,
        @Deprecated(since = "5.8.0", forRemoval = true)
        OFF_HEAP
    }

    @Deprecated(since = "5.8.0", forRemoval = true)
    @Description("Defines whether memory for transaction state should be allocated on- or off-heap. "
            + "Note that for small transactions you can gain up to 25% write speed by setting it to `ON_HEAP`.")
    public static final Setting<TransactionStateMemoryAllocation> tx_state_memory_allocation = newBuilder(
                    "db.tx_state.memory_allocation", ofEnum(TransactionStateMemoryAllocation.class), ON_HEAP)
            .build();

    @Deprecated(since = "5.8.0", forRemoval = true)
    @Description(
            "The maximum amount of off-heap memory that can be used to store transaction state data; it's a total amount of memory "
                    + "shared across all active transactions. Zero means 'unlimited'. Used when db.tx_state.memory_allocation is set to 'OFF_HEAP'.")
    public static final Setting<Long> tx_state_max_off_heap_memory = newBuilder(
                    "server.memory.off_heap.transaction_max_size", BYTES, BYTES.parse("2G"))
            .addConstraint(min(0L))
            .build();

    @Description(
            "Defines the maximum size of an off-heap memory block that can be cached to speed up allocations. The value must be a power of 2.")
    public static final Setting<Long> tx_state_off_heap_max_cacheable_block_size = newBuilder(
                    "server.memory.off_heap.max_cacheable_block_size", BYTES, ByteUnit.kibiBytes(512))
            .addConstraint(min(kibiBytes(4)))
            .addConstraint(POWER_OF_2)
            .build();

    @Description(
            "Defines the size of the off-heap memory blocks cache. The cache will contain this number of blocks for each block size "
                    + "that is power of two. Thus, maximum amount of memory used by blocks cache can be calculated as "
                    + "2 * server.memory.off_heap.max_cacheable_block_size * server.memory.off_heap.block_cache_size")
    public static final Setting<Integer> tx_state_off_heap_block_cache_size = newBuilder(
                    "server.memory.off_heap.block_cache_size", INT, 128)
            .addConstraint(min(16))
            .build();

    @Description("Enable server-side routing in clusters using an additional bolt connector.\n"
            + "When configured, this allows requests to be forwarded from one cluster member to another, if the requests can't be "
            + "satisfied by the first member (e.g. write requests received by a non-leader).")
    public static final Setting<Boolean> routing_enabled =
            newBuilder("dbms.routing.enabled", BOOL, true).build();

    @Description("The address the routing connector should bind to")
    public static final Setting<SocketAddress> routing_listen_address = newBuilder(
                    "server.routing.listen_address", SOCKET_ADDRESS, new SocketAddress(DEFAULT_ROUTING_CONNECTOR_PORT))
            .setDependency(default_listen_address)
            .build();

    @Description("Sets level for driver internal logging.")
    public static final Setting<Level> routing_driver_logging_level = newBuilder(
                    "dbms.routing.driver.logging.level", ofEnum(Level.class), Level.INFO)
            .build();

    @Description("Maximum total number of connections to be managed by a connection pool.\n"
            + "The limit is enforced for a combination of a host and user. Negative values are allowed and result in unlimited pool. Value of `0`"
            + "is not allowed.\n"
            + "Defaults to `-1` (unlimited).")
    public static final Setting<Integer> routing_driver_max_connection_pool_size =
            newBuilder("dbms.routing.driver.connection.pool.max_size", INT, -1).build();

    @Description("Pooled connections that have been idle in the pool for longer than this timeout "
            + "will be tested before they are used again, to ensure they are still alive.\n"
            + "If this option is set too low, an additional network call will be incurred when acquiring a connection, which causes a performance hit.\n"
            + "If this is set high, no longer live connections might be used which might lead to errors.\n"
            + "Hence, this parameter tunes a balance between the likelihood of experiencing connection problems and performance.\n"
            + "Normally, this parameter should not need tuning.\n"
            + "Value 0 means connections will always be tested for validity.\n"
            + "No connection liveliness check is done by default.")
    public static final Setting<Duration> routing_driver_idle_time_before_connection_test = newBuilder(
                    "dbms.routing.driver.connection.pool.idle_test", DURATION, null)
            .build();

    @Description("Pooled connections older than this threshold will be closed and removed from the pool.\n"
            + "Setting this option to a low value will cause a high connection churn and might result in a performance hit.\n"
            + "It is recommended to set maximum lifetime to a slightly smaller value than the one configured in network\n"
            + "equipment (load balancer, proxy, firewall, etc. can also limit maximum connection lifetime).\n"
            + "Zero and negative values result in lifetime not being checked.")
    public static final Setting<Duration> routing_driver_max_connection_lifetime = newBuilder(
                    "dbms.routing.driver.connection.max_lifetime", DURATION, Duration.ofHours(1))
            .build();

    @Description("Maximum amount of time spent attempting to acquire a connection from the connection pool.\n"
            + "This timeout only kicks in when all existing connections are being used and no new "
            + "connections can be created because maximum connection pool size has been reached.\n"
            + "Error is raised when connection can't be acquired within configured time.\n"
            + "Negative values are allowed and result in unlimited acquisition timeout. Value of 0 is allowed "
            + "and results in no timeout and immediate failure when connection is unavailable")
    public static final Setting<Duration> routing_driver_connection_acquisition_timeout = newBuilder(
                    "dbms.routing.driver.connection.pool.acquisition_timeout", DURATION, ofSeconds(60))
            .build();

    @Description("Socket connection timeout.\n"
            + "A timeout of zero is treated as an infinite timeout and will be bound by the timeout configured on the\n"
            + "operating system level.")
    public static final Setting<Duration> routing_driver_connect_timeout = newBuilder(
                    "dbms.routing.driver.connection.connect_timeout", DURATION, ofSeconds(5))
            .build();

    @Description("Anonymous Usage Data reporting.")
    public static final Setting<Boolean> udc_enabled =
            newBuilder("dbms.usage_report.enabled", BOOL, true).build();

    /**
     * Default settings for connectors. The default values are assumes to be default for embedded deployments through the code.
     * This map contains default connector settings that you can pass to the builders.
     */
    public static final Map<Setting<?>, Object> SERVER_DEFAULTS = buildDefaults();

    private static Map<Setting<?>, Object> buildDefaults() {
        var entries = new HashSet<>(SERVER_CONNECTOR_DEFAULTS.entrySet());
        entries.add(entry(auth_enabled, true));
        entries.add(entry(cypher_render_plan_descriptions, true));
        return Map.ofEntries(entries.toArray(Map.Entry[]::new));
    }

    public enum RoutingMode {
        SERVER,
        CLIENT
    }
}
