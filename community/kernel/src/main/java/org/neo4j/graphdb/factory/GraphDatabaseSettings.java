/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.graphdb.factory;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.util.List;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.builder.GraphDatabaseBuilder;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.configuration.ConfigurationMigrator;
import org.neo4j.kernel.configuration.GraphDatabaseConfigurationMigrator;
import org.neo4j.kernel.configuration.Group;
import org.neo4j.kernel.configuration.GroupSettingSupport;
import org.neo4j.kernel.configuration.Internal;
import org.neo4j.kernel.configuration.Migrator;
import org.neo4j.kernel.configuration.Title;
import org.neo4j.kernel.impl.cache.MonitorGc;
import org.neo4j.kernel.impl.store.format.InternalRecordFormatSelector;
import org.neo4j.logging.Level;

import static java.lang.String.valueOf;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.kernel.configuration.Settings.ANY;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.BYTES;
import static org.neo4j.kernel.configuration.Settings.DEFAULT;
import static org.neo4j.kernel.configuration.Settings.DOUBLE;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.HOSTNAME_PORT;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.LONG;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.PATH;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.STRING_LIST;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.derivedSetting;
import static org.neo4j.kernel.configuration.Settings.illegalValueMessage;
import static org.neo4j.kernel.configuration.Settings.list;
import static org.neo4j.kernel.configuration.Settings.matches;
import static org.neo4j.kernel.configuration.Settings.max;
import static org.neo4j.kernel.configuration.Settings.min;
import static org.neo4j.kernel.configuration.Settings.options;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Settings for Neo4j. Use this with {@link GraphDatabaseBuilder}.
 */
public abstract class GraphDatabaseSettings
{
    @SuppressWarnings("unused") // accessed by reflection
    @Migrator
    private static final ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator();

    @Title("Read only database")
    @Description("Only allow read operations from this Neo4j instance. " +
                 "This mode still requires write access to the directory for lock purposes.")
    public static final Setting<Boolean> read_only = setting( "dbms.read_only", BOOLEAN, FALSE );

    @Description("Print out the effective Neo4j configuration after startup.")
    @Internal
    public static final Setting<Boolean> dump_configuration = setting("unsupported.dbms.report_configuration", BOOLEAN, FALSE );

    @Description("Whether to allow a store upgrade in case the current version of the database starts against an " +
            "older store version. " +
            "Setting this to `true` does not guarantee successful upgrade, it just " +
            "allows an upgrade to be performed.")
    public static final Setting<Boolean> allow_store_upgrade = setting("dbms.allow_format_migration", BOOLEAN, FALSE );

    // Cypher settings
    // TODO: These should live with cypher
    @Description( "Set this to specify the default parser (language version)." )
    public static final Setting<String> cypher_parser_version = setting(
            "cypher.default_language_version",
            options("2.3", "3.0", DEFAULT ), DEFAULT );

    @Description( "Set this to specify the default planner for the default language version." )
    public static final Setting<String> cypher_planner = setting(
            "cypher.planner",
            options( "COST", "RULE", DEFAULT ), DEFAULT );

    @Description( "Set this to specify the behavior when Cypher planner or runtime hints cannot be fulfilled. "
            + "If true, then non-conformance will result in an error, otherwise only a warning is generated." )
    public static final Setting<Boolean> cypher_hints_error = setting( "cypher.hints_error", BOOLEAN, FALSE );

    @Description( "If set to true it will disable the shortest path fallback. That means that no full path enumeration" +
                  "will be performed in case shortest path algorithms cannot be used. This might happen in case of" +
                  "existential predicates on the path, e.g., when searching for the shortest path containing a node" +
                  "with property 'name=Emil'. The problem is that graph algorithms work only on universal predicates," +
                  "e.g., when searching for the shortest where all nodes have label 'Person'.  Note that disabling " +
                  "shortest path fallback (i.e., setting this property to true) might cause errors at runtime." )
    public static final Setting<Boolean> forbid_exhaustive_shortestpath = setting(
            "cypher.forbid_exhaustive_shortestpath", BOOLEAN, FALSE );

    @Description( "Set this to specify the default runtime for the default language version." )
    @Internal
    public static final Setting<String> cypher_runtime = setting(
            "unsupported.cypher.runtime",
            options( "INTERPRETED", "COMPILED", DEFAULT ), DEFAULT );

    @Description( "Enable tracing of compilation in cypher." )
    @Internal
    public static final Setting<Boolean> cypher_compiler_tracing = setting( "unsupported.cypher.compiler_tracing", BOOLEAN, FALSE );

    @Description( "The number of Cypher query execution plans that are cached." )
    public static Setting<Integer> query_cache_size = setting( "dbms.query_cache_size", INTEGER, "1000", min( 0 ) );

    @Description( "The threshold when a plan is considered stale. If any of the underlying" +
                  " statistics used to create the plan has changed more than this value, " +
                  "the plan is considered stale and will be replanned. " +
                  "A value of 0 means always replan, and 1 means never replan." )
    public static Setting<Double> query_statistics_divergence_threshold = setting(
            "cypher.statistics_divergence_threshold", DOUBLE, "0.5", min( 0.0 ), max(
                    1.0 ) );

    @Description( "The threshold when a warning is generated if a label scan is done after a load csv " +
                  "where the label has no index" )
    @Internal
    public static Setting<Long> query_non_indexed_label_warning_threshold = setting(
            "unsupported.cypher.non_indexed_label_warning_threshold", LONG, "10000" );

    @Description( "To improve IDP query planning time, we can restrict the internal planning table size, " +
                  "triggering compaction of candidate plans. The smaller the threshold the faster the planning, " +
                  "but the higher the risk of sub-optimal plans." )
    @Internal
    public static Setting<Integer> cypher_idp_solver_table_threshold = setting(
            "unsupported.cypher.idp_solver_table_threshold", INTEGER, "128", min( 16 ) );

    @Description( "To improve IDP query planning time, we can restrict the internal planning loop duration, " +
                  "triggering more frequent compaction of candidate plans. The smaller the threshold the " +
                  "faster the planning, but the higher the risk of sub-optimal plans." )
    @Internal
    public static Setting<Long> cypher_idp_solver_duration_threshold = setting(
            "unsupported.cypher.idp_solver_duration_threshold", LONG, "1000", min( 10L ) );

    @Description("The minimum lifetime of a query plan before a query is considered for replanning")
    public static Setting<Long> cypher_min_replan_interval = setting( "cypher.min_replan_interval", DURATION, "1s" );

    @Description( "Determines if Cypher will allow using file URLs when loading data using `LOAD CSV`. Setting this "
                  + "value to `false` will cause Neo4j to fail `LOAD CSV` clauses that load data from the file system." )
    public static Setting<Boolean> allow_file_urls = setting( "dbms.security.allow_csv_import_from_file_urls", BOOLEAN, TRUE );

    @Description( "Sets the root directory for file URLs used with the Cypher `LOAD CSV` clause. This must be set to a single "
                  + "directory, restricting access to only those files within that directory and its subdirectories." )
    public static Setting<File> load_csv_file_url_root = setting( "dbms.directories.import", PATH, NO_DEFAULT );

    @Description( "The maximum amount of time to wait for the database to become available, when " +
                  "starting a new transaction." )
    @Internal
    public static final Setting<Long> transaction_start_timeout =
            setting( "unsupported.dbms.transaction_start_timeout", DURATION, "1s" );

    @Description( "The maximum amount of time to wait for running transactions to complete before allowing "
                  + "initiated database shutdown to continue" )
    @Internal
    public static final Setting<Long> shutdown_transaction_end_timeout =
            setting( "unsupported.dbms.shutdown_transaction_end_timeout", DURATION, "10s" );

    @Description("Location of the database plugin directory. Compiled Java JAR files that contain database " +
                 "procedures will be loaded if they are placed in this directory.")
    public static final Setting<File> plugin_dir = setting("dbms.directories.plugins", PATH, "plugins" );

    @Description( "Threshold for rotation of the debug log." )
    public static final Setting<Long> store_internal_log_rotation_threshold = setting("dbms.logs.debug.rotation.size", BYTES, "20m", min(0L), max( Long.MAX_VALUE ) );

    @Description( "Debug log contexts that should output debug level logging" )
    @Internal
    public static final Setting<List<String>> store_internal_debug_contexts = setting( "unsupported.dbms.logs.debug.debug_loggers",
            list( ",", STRING ), "org.neo4j.diagnostics,org.neo4j.cluster.protocol,org.neo4j.kernel.ha" );

    @Description("Debug log level threshold.")
    public static final Setting<Level> store_internal_log_level = setting( "dbms.logs.debug.level",
            options( Level.class ), "INFO" );

    @Description( "Maximum time to wait for active transaction completion when rotating counts store" )
    @Internal
    public static final Setting<Long> counts_store_rotation_timeout =
            setting( "unsupported.dbms.counts_store_rotation_timeout", DURATION, "10m" );

    @Description( "Minimum time interval after last rotation of the debug log before it may be rotated again." )
    public static final Setting<Long> store_internal_log_rotation_delay =
            setting("dbms.logs.debug.rotation.delay", DURATION, "300s" );

    @Description( "Maximum number of history files for the debug log." )
    public static final Setting<Integer> store_internal_log_max_archives = setting("dbms.logs.debug.rotation.keep_number", INTEGER, "7", min(1) );

    @Description( "Configures the transaction interval between check-points. The database will not check-point more " +
                  "often  than this (unless check pointing is triggered by a different event), but might check-point " +
                  "less often than this interval, if performing a check-point takes longer time than the configured " +
                  "interval. A check-point is a point in the transaction logs, from which recovery would start from. " +
                  "Longer check-point intervals typically means that recovery will take longer to complete in case " +
                  "of a crash. On the other hand, a longer check-point interval can also reduce the I/O load that " +
                  "the database places on the system, as each check-point implies a flushing and forcing of all the " +
                  "store files.  The default is '100000' for a check-point every 100000 transactions." )
    public static final Setting<Integer> check_point_interval_tx = setting( "dbms.checkpoint.interval.tx", INTEGER, "100000", min(1) );

    @Description( "Configures the time interval between check-points. The database will not check-point more often " +
                  "than this (unless check pointing is triggered by a different event), but might check-point less " +
                  "often than this interval, if performing a check-point takes longer time than the configured " +
                  "interval. A check-point is a point in the transaction logs, from which recovery would start from. " +
                  "Longer check-point intervals typically means that recovery will take longer to complete in case " +
                  "of a crash. On the other hand, a longer check-point interval can also reduce the I/O load that " +
                  "the database places on the system, as each check-point implies a flushing and forcing of all the " +
                  "store files. The default is '5m' for a check-point every 5 minutes. Other supported units are 's' " +
                  "for seconds, and 'ms' for milliseconds." )
    public static final Setting<Long> check_point_interval_time = setting( "dbms.checkpoint.interval.time", DURATION, "5m" );

    @Description( "Configures the maximum amount of IO that the background check-point process should consume. " +
                  "This setting is advisory, and is ignored in Neo4j Community Edition, and is followed to best " +
                  "effort in Enterprise Edition. The setting is expressed in IOs per second. Restricting the number " +
                  "of IOPS consumed by the check-point process, will leave more IO bandwidth for things that are " +
                  "critical for transaction response time, such as appending to the transaction log. This only " +
                  "matters if both the store files and the transaction logs are placed on the same logical storage " +
                  "device. If that is the case, then limiting the check-point process to, say, half of the IO " +
                  "bandwidth of your system is a good place to start.")
    public static final Setting<Integer> check_point_iops_limit = setting( "dbms.checkpoint.iops.limit", INTEGER, "-1" );

    // Auto Indexing
    @Description("Controls the auto indexing feature for nodes. Setting it to `false` shuts it down, " +
            "while `true` enables it by default for properties listed in the dbms.auto_index.nodes.keys setting.")
    @Internal
    @Deprecated
    public static final Setting<Boolean> node_auto_indexing = setting("dbms.auto_index.nodes.enabled", BOOLEAN, FALSE);

    @Description("A list of property names (comma separated) that will be indexed by default. This applies to _nodes_ " +
            "only.")
    @Internal
    @Deprecated
    public static final Setting<List<String>> node_keys_indexable = setting("dbms.auto_index.nodes.keys", STRING_LIST, "" );

    @Description("Controls the auto indexing feature for relationships. Setting it to `false` shuts it down, " +
            "while `true` enables it by default for properties listed in the dbms.auto_index.relationships.keys setting.")
    @Internal
    @Deprecated
    public static final Setting<Boolean> relationship_auto_indexing =
            setting("dbms.auto_index.relationships.enabled", BOOLEAN, FALSE );

    @Description("A list of property names (comma separated) that will be indexed by default. This applies to " +
            "_relationships_ only." )
    @Internal
    @Deprecated
    public static final Setting<List<String>> relationship_keys_indexable = setting("dbms.auto_index.relationships.keys", STRING_LIST, "" );

    // Index sampling
    @Description("Enable or disable background index sampling")
    public static final Setting<Boolean> index_background_sampling_enabled =
            setting("dbms.index_sampling.background_enabled", BOOLEAN, TRUE );

    @Description("Size of buffer used by index sampling")
    public static final Setting<Long> index_sampling_buffer_size =
            setting("dbms.index_sampling.buffer_size", BYTES, "64m",
                    min( /* 1m */ 1048576L ), max( (long) Integer.MAX_VALUE ) );

    @Description("Percentage of index updates of total index size required before sampling of a given index is triggered")
    public static final Setting<Integer> index_sampling_update_percentage =
            setting("dbms.index_sampling.update_percentage", INTEGER, "5", min( 0 ) );

    // Lucene settings
    @Description( "The maximum number of open Lucene index searchers." )
    public static Setting<Integer> lucene_searcher_cache_size = setting("dbms.index_searcher_cache_size",INTEGER, Integer.toString( Integer.MAX_VALUE ), min( 1 ));

    // Lucene schema indexes
    @Internal
    public static final Setting<Boolean> multi_threaded_schema_index_population_enabled =
            setting( "unsupported.dbms.multi_threaded_schema_index_population_enabled", BOOLEAN, TRUE );

    // Store settings
    @Description("Make Neo4j keep the logical transaction logs for being able to backup the database. " +
            "Can be used for specifying the threshold to prune logical logs after. For example \"10 days\" will " +
            "prune logical logs that only contains transactions older than 10 days from the current time, " +
            "or \"100k txs\" will keep the 100k latest transactions and prune any older transactions.")
    public static final Setting<String> keep_logical_logs = setting("dbms.tx_log.rotation.retention_policy", STRING, "7 days", illegalValueMessage( "must be `true`/`false` or of format '<number><optional unit> <type>' for example `100M size` for " +
                        "limiting logical log space on disk to 100Mb," +
                        " or `200k txs` for limiting the number of transactions to keep to 200 000", matches(ANY)));

    @Description( "Specifies at which file size the logical log will auto-rotate. " +
                  "`0` means that no rotation will automatically occur based on file size. " )
    public static final Setting<Long> logical_log_rotation_threshold = setting( "dbms.tx_log.rotation.size", BYTES, "250M", min( 1024*1024L /*1Mb*/ ) );

    @Description("Use a quick approach for rebuilding the ID generators. This give quicker recovery time, " +
            "but will limit the ability to reuse the space of deleted entities.")
    @Internal
    public static final Setting<Boolean> rebuild_idgenerators_fast = setting("unsupported.dbms.id_generator_fast_rebuild_enabled", BOOLEAN, TRUE );

    // Store memory settings
    @Description("Target size for pages of mapped memory. If set to 0, then a reasonable default is chosen, " +
                 "depending on the storage device used.")
    @Internal
    public static final Setting<Long> mapped_memory_page_size = setting( "unsupported.dbms.memory.pagecache.pagesize", BYTES, "0" );

    @SuppressWarnings( "unchecked" )
    @Description( "The amount of memory to use for mapping the store files, in bytes (or kilobytes with the 'k' " +
                  "suffix, megabytes with 'm' and gigabytes with 'g'). If Neo4j is running on a dedicated server, " +
                  "then it is generally recommended to leave about 2-4 gigabytes for the operating system, give the " +
                  "JVM enough heap to hold all your transaction state and query context, and then leave the rest for " +
                  "the page cache. The default page cache memory assumes the machine is dedicated to running " +
                  "Neo4j, and is heuristically set to 50% of RAM minus the max Java heap size." )
    public static final Setting<Long> pagecache_memory =
            setting( "dbms.memory.pagecache.size", BYTES, defaultPageCacheMemory(), min( 8192 * 30L ) );

    private static String defaultPageCacheMemory()
    {
        // First check if we have a default override...
        String defaultMemoryOverride = System.getProperty( "dbms.pagecache.memory.default.override" );
        if ( defaultMemoryOverride != null )
        {
            return defaultMemoryOverride;
        }

        double ratioOfFreeMem = 0.50;
        String defaultMemoryRatioOverride = System.getProperty( "dbms.pagecache.memory.ratio.default.override" );
        if ( defaultMemoryRatioOverride != null )
        {
            ratioOfFreeMem = Double.parseDouble( defaultMemoryRatioOverride );
        }

        // Try to compute (RAM - maxheap) * 0.50 if we can get reliable numbers...
        long maxHeapMemory = Runtime.getRuntime().maxMemory();
        if ( 0 < maxHeapMemory && maxHeapMemory < Long.MAX_VALUE )
        {
            try
            {
                OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
                Method getTotalPhysicalMemorySize = os.getClass().getMethod( "getTotalPhysicalMemorySize" );
                getTotalPhysicalMemorySize.setAccessible( true );
                long physicalMemory = (long) getTotalPhysicalMemorySize.invoke( os );
                if ( 0 < physicalMemory && physicalMemory < Long.MAX_VALUE && maxHeapMemory < physicalMemory )
                {
                    long heuristic = (long) ((physicalMemory - maxHeapMemory) * ratioOfFreeMem);
                    long min = ByteUnit.mebiBytes( 32 ); // We'd like at least 32 MiBs.
                    long max = ByteUnit.tebiBytes( 1 ); // Don't heuristically take more than 1 TiB.
                    long memory = Math.min( max, Math.max( min, heuristic ) );
                    return String.valueOf( memory );
                }
            }
            catch ( Exception ignore )
            {
            }
        }
        // ... otherwise we just go with 2 GiBs.
        return "2g";
    }

    @Description( "Specify which page swapper to use for doing paged IO. " +
                  "This is only used when integrating with proprietary storage technology." )
    public static final Setting<String> pagecache_swapper =
            setting( "dbms.memory.pagecache.swapper", STRING, (String) null );

    @Description("Specifies the block size for storing strings. This parameter is only honored when the store is " +
            "created, otherwise it is ignored. " +
            "Note that each character in a string occupies two bytes, meaning that e.g a block size of 120 will hold " +
            "a 60 character long string before overflowing into a second block. " +
            "Also note that each block carries a ~10B of overhead so record size on disk will be slightly larger " +
            "than the configured block size" )
    @Internal
    public static final Setting<Integer> string_block_size = setting("unsupported.dbms.block_size.strings", INTEGER,
            valueOf( dynamicRecordDataSizeForAligningWith( 128 ) ), min( dynamicRecordDataSizeForAligningWith( 16 ) ) );

    @Description("Specifies the block size for storing arrays. This parameter is only honored when the store is " +
            "created, otherwise it is ignored. " +
            "Also note that each block carries a ~10B of overhead so record size on disk will be slightly larger " +
            "than the configured block size" )
    @Internal
    public static final Setting<Integer> array_block_size = setting("unsupported.dbms.block_size.array_properties", INTEGER,
            valueOf( dynamicRecordDataSizeForAligningWith( 128 ) ), min( dynamicRecordDataSizeForAligningWith( 16 ) ) );

    @Description("Specifies the block size for storing labels exceeding in-lined space in node record. " +
    		"This parameter is only honored when the store is created, otherwise it is ignored. " +
            "Also note that each block carries a ~10B of overhead so record size on disk will be slightly larger " +
            "than the configured block size" )
    @Internal
    public static final Setting<Integer> label_block_size = setting("unsupported.dbms.block_size.labels", INTEGER,
            valueOf( dynamicRecordDataSizeForAligningWith( 64 ) ), min( dynamicRecordDataSizeForAligningWith( 16 ) ) );

    @Description("An identifier that uniquely identifies this graph database instance within this JVM. " +
            "Defaults to an auto-generated number depending on how many instance are started in this JVM.")
    @Internal
    public static final Setting<String> forced_kernel_id = setting("unsupported.dbms.kernel_id", STRING, NO_DEFAULT,
            illegalValueMessage("has to be a valid kernel identifier", matches("[a-zA-Z0-9]*")));

    @Internal
    public static final Setting<Boolean> execution_guard_enabled = setting("unsupported.dbms.executiontime_limit.enabled", BOOLEAN, FALSE );

    @Description("Amount of time in ms the GC monitor thread will wait before taking another measurement.")
    @Internal
    public static final Setting<Long> gc_monitor_interval = MonitorGc.Configuration.gc_monitor_wait_time;

    @Description("The amount of time in ms the monitor thread has to be blocked before logging a message it was " +
            "blocked.")
    @Internal
    public static final Setting<Long> gc_monitor_block_threshold = MonitorGc.Configuration.gc_monitor_threshold;

    @Description( "Relationship count threshold for considering a node to be dense" )
    public static final Setting<Integer> dense_node_threshold = setting( "dbms.relationship_grouping_threshold", INTEGER, "50", min(1) );

    @Description( "Log executed queries that takes longer than the configured threshold. "
            + "_NOTE: This feature is only available in the Neo4j Enterprise Edition_." )
    public static final Setting<Boolean> log_queries = setting("dbms.logs.query.enabled", BOOLEAN, FALSE );

    @Description("Path of the logs directory")
    public static final Setting<File> logs_directory = setting("dbms.directories.logs", PATH, "logs");

    @Internal
    public static final Setting<File> log_queries_filename = derivedSetting("dbms.querylog.filename",
            logs_directory,
            ( logs ) -> new File( logs, "query.log" ),
            PATH );

    @Description("If the execution of query takes more time than this threshold, the query is logged - " +
                 "provided query logging is enabled. Defaults to 0 seconds, that is all queries are logged.")
    public static final Setting<Long> log_queries_threshold = setting("dbms.logs.query.threshold", DURATION, "0s");

    @Description( "The file size in bytes at which the query log will auto-rotate. If set to zero then no rotation " +
            "will occur. Accepts a binary suffix `k`, `m` or `g`." )
    public static final Setting<Long> log_queries_rotation_threshold = setting("dbms.logs.query.rotation.size",
            BYTES, "20m",  min( 0L ), max( Long.MAX_VALUE ) );

    @Description( "Maximum number of history files for the query log." )
    public static final Setting<Integer> log_queries_max_archives = setting( "dbms.logs.query.rotation.keep_number",
            INTEGER, "7", min( 1 ) );

    @Description( "Specifies number of operations that batch inserter will try to group into one batch before " +
                  "flushing data into underlying storage.")
    @Internal
    public static final Setting<Integer> batch_inserter_batch_size = setting( "unsupported.tools.batch_inserter.batch_size", INTEGER,
            "10000" );

    @Description("Enable auth requirement to access Neo4j.")
    public static final Setting<Boolean> auth_enabled = setting( "dbms.security.auth_enabled", BOOLEAN, "false" );

    @Internal
    public static final Setting<File> auth_store = setting("unsupported.dbms.security.auth_store.location", PATH, NO_DEFAULT);

    // Bolt Settings

    @Group("dbms.connector")
    public static class Connector
    {
        @Description( "Enable this connector" )
        public final Setting<Boolean> enabled;

        @Description( "Connector type. You should always set this to the connector type you want" )
        public final Setting<ConnectorType> type;

        // Note: Be careful about adding things here that does not apply to all connectors,
        //       consider future options like non-tcp transports, making `address` a bad choice
        //       as a setting that applies to every connector, for instance.

        protected final GroupSettingSupport group;

        // For sub-classes, we provide this protected constructor that allows overriding
        // the default 'type' setting value.
        protected Connector( String key, String typeDefault )
        {
            group = new GroupSettingSupport( Connector.class, key );
            enabled = group.scope( setting( "enabled", BOOLEAN, "false" ) );
            type = group.scope( setting( "type", options( ConnectorType.class ), typeDefault ) );
        }

        public enum ConnectorType
        {
            BOLT, HTTP
        }
    }

    @Description( "Configuration options for Bolt connectors." )
    public static class BoltConnector extends Connector
    {
        @Description( "Encryption level to require this connector to use" )
        public final Setting<EncryptionLevel> encryption_level;

        @Description( "Address the connector should bind to" )
        public final Setting<HostnamePort> address;

        public BoltConnector(String key)
        {
            super(key, ConnectorType.BOLT.name() );
            encryption_level = group.scope(
                    setting( "tls_level", options( EncryptionLevel.class ), OPTIONAL.name() ));
            address = group.scope( setting( "address", HOSTNAME_PORT, "localhost:7687" ) );
        }

        public enum EncryptionLevel
        {
            REQUIRED,
            OPTIONAL,
            DISABLED
        }
    }

    /**
     * Short-hand for creating a new Bolt connector settings group.
     * Use this to configure a new or modify an existing Bolt connector.
     * @param key a unique identifier for this connector
     * @return an object that can be used to set configuration for the Bolt connector with the given key
     */
    public static BoltConnector boltConnector( String key )
    {
        return new BoltConnector( key );
    }

    /**
     * Uses the default selected record format to figure out the correct dynamic record data size to create
     * a store with.
     *
     * @param recordSize the desired record size, which optimally is a power-of-two, e.g. 64 or 128.
     * The dynamic record header size from the selected record format will then be subtracted from this value
     * to get to the data size, which is the value that the configuration will end up having.
     * @return the dynamic record data size based on the desired record size and with the header size in mind.
     */
    private static int dynamicRecordDataSizeForAligningWith( int recordSize )
    {
        return recordSize - InternalRecordFormatSelector.select().dynamic().getRecordHeaderSize();
    }
}
