/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.apache.commons.lang3.SystemUtils;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.util.List;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.configuration.ConfigurationMigrator;
import org.neo4j.kernel.configuration.GraphDatabaseConfigurationMigrator;
import org.neo4j.kernel.configuration.Internal;
import org.neo4j.kernel.configuration.Migrator;
import org.neo4j.kernel.configuration.Obsoleted;
import org.neo4j.kernel.configuration.Title;
import org.neo4j.kernel.impl.cache.MonitorGc;
import org.neo4j.logging.Level;

import static org.neo4j.kernel.configuration.Settings.ANY;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.BYTES;
import static org.neo4j.kernel.configuration.Settings.DEFAULT;
import static org.neo4j.kernel.configuration.Settings.DOUBLE;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.LONG;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.PATH;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.basePath;
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
    @Migrator
    private static final ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator();

    @Title("Read only database")
    @Description("Only allow read operations from this Neo4j instance. " +
                 "This mode still requires write access to the directory for lock purposes.")
    public static final Setting<Boolean> read_only = setting( "read_only", BOOLEAN, FALSE );

    @Deprecated
    @Description( "The type of cache to use for nodes and relationships. " +
                  "This configuration setting is no longer applicable from Neo4j 2.3. " +
                  "Configuration has been simplified to only require tuning of the page cache." )
    public static final Setting<String> cache_type = setting( "cache_type", STRING, "deprecated" );

    @Description("Print out the effective Neo4j configuration after startup.")
    public static final Setting<Boolean> dump_configuration = setting("dump_configuration", BOOLEAN, FALSE );

    @Description("Whether to allow a store upgrade in case the current version of the database starts against an " +
            "older store version. " +
            "Setting this to `true` does not guarantee successful upgrade, it just " +
            "allows an upgrade to be performed.")
    public static final Setting<Boolean> allow_store_upgrade = setting("allow_store_upgrade", BOOLEAN, FALSE );

    @Description("Determines whether any TransactionInterceptors loaded will intercept " +
            "externally received transactions (for example in HA) before they reach the " +
            "logical log and are applied to the store.")
    @Internal
    // used in commented-out code in TestKernelPanic
    public static final Setting<Boolean> intercept_deserialized_transactions = setting("intercept_deserialized_transactions", BOOLEAN, FALSE);

    // Cypher settings
    // TODO: These should live with cypher
    @Description( "Set this to specify the default parser (language version)." )
    public static final Setting<String> cypher_parser_version = setting(
            "cypher_parser_version",
            options( "1.9", "2.2", "2.3", DEFAULT ), DEFAULT );

    @Description( "Set this to specify the default planner for the default language version." )
    public static final Setting<String> cypher_planner = setting(
            "dbms.cypher.planner",
            options( "COST", "RULE", DEFAULT ), DEFAULT );

    @Description( "Set this to specify the behavior when Cypher planner or runtime hints cannot be fulfilled. "
            + "If true, then non-conformance will result in an error, otherwise only a warning is generated." )
    public static final Setting<Boolean> cypher_hints_error = setting( "dbms.cypher.hints.error", BOOLEAN, FALSE );

    @Description( "Set this to specify the default runtime for the default language version." )
    @Internal
    public static final Setting<String> cypher_runtime = setting(
            "dbms.cypher.runtime",
            options( "INTERPRETED", DEFAULT ), DEFAULT );

    @Description( "Enable tracing of compilation in cypher." )
    @Internal
    public static final Setting<Boolean> cypher_compiler_tracing = setting( "dbms.cypher.compiler_tracing", BOOLEAN, FALSE );

    @Description( "The number of Cypher query execution plans that are cached." )
    public static Setting<Integer> query_cache_size = setting( "query_cache_size", INTEGER, "1000", min( 0 ) );

    @Description( "The threshold when a plan is considered stale. If any of the underlying" +
                  " statistics used to create the plan has changed more than this value, " +
                  "the plan is considered stale and will be replanned. " +
                  "A value of 0 means always replan, and 1 means never replan." )
    public static Setting<Double> query_statistics_divergence_threshold = setting(
            "dbms.cypher.statistics_divergence_threshold", DOUBLE, "0.75", min( 0.0 ), max(
                    1.0 ) );

    @Description( "The threshold when a warning is generated if a label scan is done after a load csv " +
                  "where the label has no index" )
    @Internal
    public static Setting<Long> query_non_indexed_label_warning_threshold = setting(
            "dbms.cypher.non_indexed_label_warning_threshold", LONG, "10000" );

    @Description( "To improve IDP query planning time, we can restrict the internal planning table size, " +
                  "triggering compaction of candidate plans. The smaller the threshold the faster the planning, " +
                  "but the higher the risk of sub-optimal plans." )
    @Internal
    public static Setting<Integer> cypher_idp_solver_table_threshold = setting(
            "dbms.cypher.idp_solver_table_threshold", INTEGER, "128", min( 16 ) );

    @Description( "To improve IDP query planning time, we can restrict the internal planning loop duration, " +
                  "triggering more frequent compaction of candidate plans. The smaller the threshold the " +
                  "faster the planning, but the higher the risk of sub-optimal plans." )
    @Internal
    public static Setting<Long> cypher_idp_solver_duration_threshold = setting(
            "dbms.cypher.idp_solver_duration_threshold", LONG, "1000", min( 10L ) );

    @Description("The minimum lifetime of a query plan before a query is considered for replanning")
    public static Setting<Long> cypher_min_replan_interval =
            setting( "dbms.cypher.min_replan_interval", DURATION, "10s" );

    @Description( "Determines if Cypher will allow using file URLs when loading data using `LOAD CSV`. Setting this "
                  + "value to `false` will cause Neo4j to fail `LOAD CSV` clauses that load data from the file system." )
    public static Setting<Boolean> allow_file_urls = setting( "allow_file_urls", BOOLEAN, TRUE );

    @Description( "Sets the root directory for file URLs used with the Cypher `LOAD CSV` clause. This must be set to a single "
                  + "directory, restricting access to only those files within that directory and its subdirectories." )
    public static Setting<File> load_csv_file_url_root = setting( "dbms.security.load_csv_file_url_root", PATH, NO_DEFAULT );

    @Deprecated
    @Obsoleted( "This is no longer used" )
    @Description("The directory where the database files are located.")
    public static final Setting<File> store_dir = setting("store_dir", PATH, NO_DEFAULT );

    @Description( "The maximum amount of time to wait for the database to become available, when " +
                  "starting a new transaction." )
    @Internal
    public static final Setting<Long> transaction_start_timeout =
            setting( "transaction_start_timeout", DURATION, "1s" );

    @Description("The location of the internal diagnostics log.")
    @Internal
    public static final Setting<File> store_internal_log_location = setting("store.internal_log.location", PATH, NO_DEFAULT );

    @Description( "Threshold for rotation of the internal log." )
    public static final Setting<Long> store_internal_log_rotation_threshold = setting("store.internal_log.rotation_threshold", BYTES, "20m", min(0L), max( Long.MAX_VALUE ) );

    @Description( "Internal log contexts that should output debug level logging" )
    @Internal
    public static final Setting<List<String>> store_internal_debug_contexts = setting( "store.internal_log.debug_contexts",
            list( ",", STRING ), "org.neo4j.diagnostics,org.neo4j.cluster.protocol,org.neo4j.kernel.ha" );

    @Description("Log level threshold.")
    public static final Setting<Level> store_internal_log_level = setting( "store.internal_log.level",
            options( Level.class ), "INFO" );

    @Description( "Maximum time interval for log rotation to wait for active transaction completion" )
    @Internal
    public static final Setting<Long> store_interval_log_rotation_wait_time =
            setting( "store.interval.log.rotation", DURATION, "10m" );

    @Description( "Minimum time interval after last rotation of the internal log before it may be rotated again." )
    public static final Setting<Long> store_internal_log_rotation_delay =
            setting("store.internal_log.rotation_delay", DURATION, "300s" );

    @Description( "Maximum number of history files for the internal log." )
    public static final Setting<Integer> store_internal_log_max_archives = setting("store.internal_log.max_archives", INTEGER, "7", min(1) );

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

    // Auto Indexing
    @Description("Controls the auto indexing feature for nodes. Setting it to `false` shuts it down, " +
            "while `true` enables it by default for properties "
            + "listed in the node_keys_indexable setting.")
    @Internal
    @Deprecated
    public static final Setting<Boolean> node_auto_indexing = setting("node_auto_indexing", BOOLEAN, FALSE);

    @Description("A list of property names (comma separated) that will be indexed by default. This applies to _nodes_ " +
            "only.")
    @Internal
    @Deprecated
    public static final Setting<String> node_keys_indexable = setting("node_keys_indexable", STRING, NO_DEFAULT, illegalValueMessage( "must be a comma-separated list of keys to be indexed", matches( ANY ) ) );

    @Description("Controls the auto indexing feature for relationships. Setting it to `false` shuts it down, " +
            "while `true` enables it by default for properties "
            + "listed in the relationship_keys_indexable setting.")
    @Internal
    @Deprecated
    public static final Setting<Boolean> relationship_auto_indexing =
            setting("relationship_auto_indexing", BOOLEAN, FALSE );

    @Description("A list of property names (comma separated) that will be indexed by default. This applies to " +
            "_relationships_ only." )
    @Internal
    @Deprecated
    public static final Setting<String> relationship_keys_indexable = setting("relationship_keys_indexable", STRING, NO_DEFAULT, illegalValueMessage( "must be a comma-separated list of keys to be indexed", matches( ANY ) ) );

    // Index sampling
    @Description("Enable or disable background index sampling")
    public static final Setting<Boolean> index_background_sampling_enabled =
            setting("index_background_sampling_enabled", BOOLEAN, TRUE );

    @Description("Size of buffer used by index sampling")
    public static final Setting<Long> index_sampling_buffer_size =
            setting("index_sampling_buffer_size", BYTES, "64m",
                    min( /* 1m */ 1048576l ), max( (long) Integer.MAX_VALUE ) );

    @Description("Percentage of index updates of total index size required before sampling of a given index is triggered")
    public static final Setting<Integer> index_sampling_update_percentage =
            setting("index_sampling_update_percentage", INTEGER, "5", min( 0 ) );

    // Lucene settings
    @Description( "The maximum number of open Lucene index searchers." )
    public static Setting<Integer> lucene_searcher_cache_size = setting("lucene_searcher_cache_size",INTEGER, Integer.toString( Integer.MAX_VALUE ), min( 1 ));

    // Store settings
    @Description("Make Neo4j keep the logical transaction logs for being able to backup the database. " +
            "Can be used for specifying the threshold to prune logical logs after. For example \"10 days\" will " +
            "prune logical logs that only contains transactions older than 10 days from the current time, " +
            "or \"100k txs\" will keep the 100k latest transactions and prune any older transactions.")
    public static final Setting<String> keep_logical_logs = setting("keep_logical_logs", STRING, "7 days", illegalValueMessage( "must be `true`/`false` or of format '<number><optional unit> <type>' for example `100M size` for " +
                        "limiting logical log space on disk to 100Mb," +
                        " or `200k txs` for limiting the number of transactions to keep to 200 000", matches(ANY)));

    @Description( "Specifies at which file size the logical log will auto-rotate. " +
                  "`0` means that no rotation will automatically occur based on file size. " )
    public static final Setting<Long> logical_log_rotation_threshold = setting( "logical_log_rotation_threshold", BYTES, "250M", min( 1024*1024L /*1Mb*/ ) );

    @Description("Use a quick approach for rebuilding the ID generators. This give quicker recovery time, " +
            "but will limit the ability to reuse the space of deleted entities.")
    @Internal
    public static final Setting<Boolean> rebuild_idgenerators_fast = setting("rebuild_idgenerators_fast", BOOLEAN, TRUE );

    // Store memory settings
    /**
     * @deprecated This configuration has been obsoleted. Neo4j no longer relies on the memory-mapping capabilities of the operating system.
     */
    @Deprecated
    @Obsoleted( "This setting has been obsoleted. Neo4j no longer relies on the memory-mapping capabilities of the operating system." )
    @Description( "Use memory mapped buffers for accessing the native storage layer." )
    public static final Setting<Boolean> use_memory_mapped_buffers = setting( "use_memory_mapped_buffers", BOOLEAN, Boolean.toString(!SystemUtils.IS_OS_WINDOWS));

    @Description("Target size for pages of mapped memory. If set to 0, then a reasonable default is chosen, " +
                 "depending on the storage device used.")
    @Internal
    public static final Setting<Long> mapped_memory_page_size = setting( "dbms.pagecache.pagesize", BYTES, "0" );

    @SuppressWarnings( "unchecked" )
    @Description( "The amount of memory to use for mapping the store files, in bytes (or kilobytes with the 'k' " +
                  "suffix, megabytes with 'm' and gigabytes with 'g'). If Neo4j is running on a dedicated server, " +
                  "then it is generally recommended to leave about 2-4 gigabytes for the operating system, give the " +
                  "JVM enough heap to hold all your transaction state and query context, and then leave the rest for " +
                  "the page cache. The default page cache memory assumes the machine is dedicated to running " +
                  "Neo4j, and is heuristically set to 50% of RAM minus the max Java heap size." )
    public static final Setting<Long> pagecache_memory =
            setting( "dbms.pagecache.memory", BYTES, defaultPageCacheMemory(), min( 8192 * 30L ) );

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
            setting( "dbms.pagecache.swapper", STRING, (String) null );

    @Deprecated
    @Obsoleted( "This is no longer used" )
    @Description( "Log memory mapping statistics regularly." )
    public static final Setting<Boolean> log_mapped_memory_stats = setting( "log_mapped_memory_stats", BOOLEAN, FALSE );

    @Deprecated
    @Obsoleted( "This is no longer used" )
    @Description( "The file where memory mapping statistics will be recorded." )
    public static final Setting<File> log_mapped_memory_stats_filename = setting( "log_mapped_memory_stats_filename",
            PATH, "mapped_memory_stats.log", basePath(store_dir) );

    @Deprecated
    @Obsoleted( "This is no longer used" )
    @Description( "The number of records to be loaded between regular logging of memory mapping statistics." )
    public static final Setting<Integer> log_mapped_memory_stats_interval = setting("log_mapped_memory_stats_interval", INTEGER, "1000000");

    /**
     * @deprecated Replaced by the pagecache_memory setting.
     */
    @Deprecated
    @Obsoleted( "Replaced by the dbms.pagecache.memory setting." )
    @Description( "The size to allocate for memory mapping the node store.")
    public static final Setting<Long> nodestore_mapped_memory_size = setting( "neostore.nodestore.db.mapped_memory",
            BYTES, NO_DEFAULT );

    /**
     * @deprecated Replaced by the pagecache_memory setting.
     */
    @Deprecated
    @Obsoleted( "Replaced by the dbms.pagecache.memory setting." )
    @Description("The size to allocate for memory mapping the property value store.")
    public static final Setting<Long> nodestore_propertystore_mapped_memory_size = setting("neostore.propertystore.db.mapped_memory", BYTES, NO_DEFAULT );

    /**
     * @deprecated Replaced by the pagecache_memory setting.
     */
    @Deprecated
    @Obsoleted( "Replaced by the dbms.pagecache.memory setting." )
    @Description("The size to allocate for memory mapping the store for property key indexes.")
    public static final Setting<Long> nodestore_propertystore_index_mapped_memory_size = setting("neostore.propertystore.db.index.mapped_memory", BYTES, NO_DEFAULT );

    /**
     * @deprecated Replaced by the pagecache_memory setting.
     */
    @Obsoleted( "Replaced by the dbms.pagecache.memory setting." )
    @Deprecated
    @Description("The size to allocate for memory mapping the store for property key strings.")
    public static final Setting<Long> nodestore_propertystore_index_keys_mapped_memory_size = setting("neostore.propertystore.db.index.keys.mapped_memory", BYTES, NO_DEFAULT );

    /**
     * @deprecated Replaced by the pagecache_memory setting.
     */
    @Deprecated
    @Obsoleted( "Replaced by the dbms.pagecache.memory setting." )
    @Description("The size to allocate for memory mapping the string property store.")
    public static final Setting<Long> strings_mapped_memory_size = setting("neostore.propertystore.db.strings.mapped_memory", BYTES, NO_DEFAULT );

    /**
     * @deprecated Replaced by the pagecache_memory setting.
     */
    @Deprecated
    @Obsoleted( "Replaced by the dbms.pagecache.memory setting." )
    @Description("The size to allocate for memory mapping the array property store.")
    public static final Setting<Long> arrays_mapped_memory_size = setting("neostore.propertystore.db.arrays.mapped_memory", BYTES, NO_DEFAULT );

    /**
     * @deprecated Replaced by the pagecache_memory setting.
     */
    @Deprecated
    @Obsoleted( "Replaced by the dbms.pagecache.memory setting." )
    @Description("The size to allocate for memory mapping the relationship store.")
    public static final Setting<Long> relationshipstore_mapped_memory_size = setting("neostore.relationshipstore.db.mapped_memory", BYTES, NO_DEFAULT );


    @Description("How many relationships to read at a time during iteration")
    public static final Setting<Integer> relationship_grab_size = setting("relationship_grab_size", INTEGER, "100", min( 1 ));

    @Description("Specifies the block size for storing strings. This parameter is only honored when the store is " +
            "created, otherwise it is ignored. " +
            "Note that each character in a string occupies two bytes, meaning that a block size of 120 (the default " +
            "size) will hold a 60 character " +
            "long string before overflowing into a second block. Also note that each block carries an overhead of 8 " +
            "bytes. " +
            "This means that if the block size is 120, the size of the stored records will be 128 bytes.")
    @Internal
    public static final Setting<Integer> string_block_size = setting("string_block_size", INTEGER, "120",min(1));

    @Description("Specifies the block size for storing arrays. This parameter is only honored when the store is " +
            "created, otherwise it is ignored. " +
            "The default block size is 120 bytes, and the overhead of each block is the same as for string blocks, " +
            "i.e., 8 bytes.")
    @Internal
    public static final Setting<Integer> array_block_size = setting("array_block_size", INTEGER, "120",min(1));

    @Description("Specifies the block size for storing labels exceeding in-lined space in node record. " +
    		"This parameter is only honored when the store is created, otherwise it is ignored. " +
            "The default block size is 60 bytes, and the overhead of each block is the same as for string blocks, " +
            "i.e., 8 bytes.")
    @Internal
    public static final Setting<Integer> label_block_size = setting("label_block_size", INTEGER, "60",min(1));

    @Description("An identifier that uniquely identifies this graph database instance within this JVM. " +
            "Defaults to an auto-generated number depending on how many instance are started in this JVM.")
    @Internal
    public static final Setting<String> forced_kernel_id = setting("forced_kernel_id", STRING, NO_DEFAULT, illegalValueMessage("has to be a valid kernel identifier", matches("[a-zA-Z0-9]*")));

    @Internal
    public static final Setting<Boolean> execution_guard_enabled = setting("execution_guard_enabled", BOOLEAN, FALSE );

    @Description("Amount of time in ms the GC monitor thread will wait before taking another measurement.")
    @Internal
    public static final Setting<Long> gc_monitor_interval = MonitorGc.Configuration.gc_monitor_wait_time;

    @Description("The amount of time in ms the monitor thread has to be blocked before logging a message it was " +
            "blocked.")
    @Internal
    public static final Setting<Long> gc_monitor_block_threshold = MonitorGc.Configuration.gc_monitor_threshold;

    @Description( "Relationship count threshold for considering a node to be dense" )
    public static final Setting<Integer> dense_node_threshold = setting( "dense_node_threshold", INTEGER, "50", min(1) );

    @Deprecated
    @Description("Whether or not transactions are appended to the log in batches")
    @Obsoleted( "Write batching can no longer be turned off" )
    public static final Setting<Boolean> batched_writes = setting( "batched_writes", BOOLEAN, Boolean.TRUE.toString() );

    @Description( "Log executed queries that takes longer than the configured threshold. "
            + "_NOTE: This feature is only available in the Neo4j Enterprise Edition_." )
    public static final Setting<Boolean> log_queries = setting("dbms.querylog.enabled", BOOLEAN, FALSE );

    @Description( "Log executed queries that take longer than the configured threshold" )
    public static final Setting<File> log_queries_filename = setting("dbms.querylog.filename", PATH, NO_DEFAULT );

    @Description( "Log parameters for executed queries that took longer than the configured threshold." )
    public static final Setting<Boolean> log_queries_parameter_logging_enabled = setting( "dbms.querylog.parameter_logging_enabled", BOOLEAN, TRUE );

    @Description("If the execution of query takes more time than this threshold, the query is logged - " +
                 "provided query logging is enabled. Defaults to 0 seconds, that is all queries are logged.")
    public static final Setting<Long> log_queries_threshold = setting("dbms.querylog.threshold", DURATION, "0s");

    @Description( "Specifies at which file size the query log will auto-rotate. " +
                  "`0` means that no rotation will automatically occur based on file size." )
    public static final Setting<Long> log_queries_rotation_threshold = setting("dbms.querylog.rotation.threshold",
            BYTES, "20m",  min( 0L ), max( Long.MAX_VALUE ) );

    @Description( "Maximum number of history files for the query log." )
    public static final Setting<Integer> log_queries_max_archives = setting( "dbms.querylog.max_archives", INTEGER,
            "7", min( 1 ) );

    @Description( "Specifies number of operations that batch inserter will try to group into one batch before " +
                  "flushing data into underlying storage.")
    @Internal
    public static final Setting<Integer> batch_inserter_batch_size = setting( "batch_inserter_batch_size", INTEGER,
            "10000" );

    @Description( "Create an archive of an index before re-creating it if failing to load on startup." )
    @Internal
    public static final Setting<Boolean> archive_failed_index = setting(
            "unsupported.dbms.index.archive_failed", BOOLEAN, "false" );
}
