/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.configuration.ConfigurationMigrator;
import org.neo4j.kernel.configuration.GraphDatabaseConfigurationMigrator;
import org.neo4j.kernel.configuration.Internal;
import org.neo4j.kernel.configuration.Migrator;
import org.neo4j.kernel.configuration.Obsoleted;
import org.neo4j.kernel.configuration.Title;
import org.neo4j.kernel.impl.api.store.CacheLayer;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.MonitorGc;

import static org.neo4j.helpers.Settings.ANY;
import static org.neo4j.helpers.Settings.BOOLEAN;
import static org.neo4j.helpers.Settings.BYTES;
import static org.neo4j.helpers.Settings.DURATION;
import static org.neo4j.helpers.Settings.DirectMemoryUsage.directMemoryUsage;
import static org.neo4j.helpers.Settings.FALSE;
import static org.neo4j.helpers.Settings.INTEGER;
import static org.neo4j.helpers.Settings.NO_DEFAULT;
import static org.neo4j.helpers.Settings.PATH;
import static org.neo4j.helpers.Settings.STRING;
import static org.neo4j.helpers.Settings.TRUE;
import static org.neo4j.helpers.Settings.basePath;
import static org.neo4j.helpers.Settings.illegalValueMessage;
import static org.neo4j.helpers.Settings.matches;
import static org.neo4j.helpers.Settings.max;
import static org.neo4j.helpers.Settings.min;
import static org.neo4j.helpers.Settings.options;
import static org.neo4j.helpers.Settings.port;
import static org.neo4j.helpers.Settings.setting;

/**
 * Settings for Neo4j. Use this with {@link GraphDatabaseBuilder}.
 */
public abstract class GraphDatabaseSettings
{
    @Migrator
    private static final ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator();

    @Title("Read only database")
    @Description("Only allow read operations from this Neo4j instance. "
            + "This mode still requires write access to the directory for lock purposes.")
    public static final Setting<Boolean> read_only = setting( "read_only", BOOLEAN, FALSE );

    @Description("The type of cache to use for nodes and relationships. "
                  + "Note that the Neo4j Enterprise Edition has the additional `hpc` cache type (High-Performance Cache). "
            + "See the chapter on caches in the manual for more information.")
    public static final Setting<String> cache_type = setting( "cache_type", options( availableCaches() ), availableCaches()[0] );

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
    @Description( "Set this to specify the default parser." )
    public static final Setting<String> cypher_parser_version = setting(
            "cypher_parser_version",
            options( "1.9", "2.0", "2.1", "2.2"), NO_DEFAULT );

    @Description( "Set this to specify the default planner." )
    public static final Setting<String> query_planner_version = setting(
            "query.planner.version",
            options( "COST", "RULE"), NO_DEFAULT );

    @Description( "The number of Cypher query execution plans that are cached." )
    public static Setting<Integer> query_cache_size = setting( "query_cache_size", INTEGER, "100", min( 0 ) );

    @Description("The minimum lifetime of a query plan before a query is considered for replanning")
    public static Setting<Long> query_plan_ttl = setting( "query_plan_ttl", DURATION, "1s" );

    @Description( "Determines if Cypher will allow using file URLs when loading data using `LOAD CSV`. Setting this "
                  + "value to `false` will cause Neo4j to fail `LOAD CSV` clauses that load data from the file system." )
    public static Setting<Boolean> allow_file_urls = setting( "allow_file_urls", BOOLEAN, TRUE );



    // Store files
    @Description("The directory where the database files are located.")
    public static final Setting<File> store_dir = setting("store_dir", PATH, NO_DEFAULT );

    @Description( "The maximum amount of time to wait for the database to become available, when " +
                  "starting a new transaction." )
    @Internal
    public static final Setting<Long> transaction_start_timeout =
            setting( "transaction_start_timeout", DURATION, "1s" );

    @Description("The base name for the Neo4j Store files, either an absolute path or relative to the store_dir " +
            "setting. This should generally not be changed.")
    @Internal
    public static final Setting<File> neo_store = setting("neo_store", PATH, "neostore", basePath(store_dir) );

    // Remote logging
    @Description("Whether to enable logging to a remote server or not.")
    public static final Setting<Boolean> remote_logging_enabled = setting("remote_logging_enabled", BOOLEAN, FALSE );

    @Description( "Host for remote logging using Logback SocketAppender." )
    public static final Setting<String> remote_logging_host = setting("remote_logging_host", STRING, "127.0.0.1", illegalValueMessage( "must be a valid hostname", matches( ANY ) ) );

    @Description( "Port for remote logging using Logback SocketAppender." )
    public static final Setting<Integer> remote_logging_port = setting("remote_logging_port", INTEGER, "4560", port );

    @Description( "Maximum number of history files for messages.log." )
    public static final Setting<Integer> log_history_size = setting("logging.history", INTEGER, "7", min(1) );

    // Indexing
    @Description("Controls the auto indexing feature for nodes. Setting it to `false` shuts it down, " +
            "while `true` enables it by default for properties "
            + "listed in the node_keys_indexable setting.")
    public static final Setting<Boolean> node_auto_indexing = setting("node_auto_indexing", BOOLEAN, FALSE);

    @Description("A list of property names (comma separated) that will be indexed by default. This applies to _nodes_ " +
            "only.")
    public static final Setting<String> node_keys_indexable = setting("node_keys_indexable", STRING, NO_DEFAULT, illegalValueMessage( "must be a comma-separated list of keys to be indexed", matches( ANY ) ) );

    @Description("Controls the auto indexing feature for relationships. Setting it to `false` shuts it down, " +
            "while `true` enables it by default for properties "
            + "listed in the relationship_keys_indexable setting.")
    public static final Setting<Boolean> relationship_auto_indexing =
            setting("relationship_auto_indexing", BOOLEAN, FALSE );

    @Description("A list of property names (comma separated) that will be indexed by default. This applies to " +
            "_relationships_ only." )
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

    // NeoStore settings
    @Description("Make Neo4j keep the logical transaction logs for being able to backup the database. " +
            "Can be used for specifying the threshold to prune logical logs after. For example \"10 days\" will " +
            "prune logical logs that only contains transactions older than 10 days from the current time, " +
            "or \"100k txs\" will keep the 100k latest transactions and prune any older transactions.")
    public static final Setting<String> keep_logical_logs = setting("keep_logical_logs", STRING, "7 days", illegalValueMessage( "must be `true`/`false` or of format '<number><optional unit> <type>' for example `100M size` for " +
                        "limiting logical log space on disk to 100Mb," +
                        " or `200k txs` for limiting the number of transactions to keep to 200 000", matches(ANY)));

    @Description( "Specifies at which file size the logical log will auto-rotate. " +
                  "`0` means that no rotation will automatically occur based on file size. " )
    public static final Setting<Long> logical_log_rotation_threshold = setting( "logical_log_rotation_threshold", BYTES, "25M" );

    @Description("Use a quick approach for rebuilding the ID generators. This give quicker recovery time, " +
            "but will limit the ability to reuse the space of deleted entities.")
    @Internal
    public static final Setting<Boolean> rebuild_idgenerators_fast = setting("rebuild_idgenerators_fast", BOOLEAN, TRUE );

    // NeoStore memory settings
    /**
     * @deprecated This configuration has been obsoleted. Neo4j no longer relies on the memory-mapping capabilities of the operating system.
     */
    @Deprecated
    @Obsoleted( "This setting has been obsoleted. Neo4j no longer relies on the memory-mapping capabilities of the operating system." )
    @Description( "Use memory mapped buffers for accessing the native storage layer." )
    public static final Setting<Boolean> use_memory_mapped_buffers = setting( "use_memory_mapped_buffers", BOOLEAN, Boolean.toString(!Settings.osIsWindows()));

    @Description("Target size for pages of mapped memory.")
    @Internal
    public static final Setting<Long> mapped_memory_page_size = setting("mapped_memory_page_size", BYTES, "8192" );

    @Description("The amount of memory to use for mapping the store files, either in bytes or" +
            " as a percentage of available memory. This will be clipped at the amount of" +
            " free memory observed when the database starts, and automatically be rounded" +
            " down to the nearest whole page. For example, if `500MB` is configured, but" +
            " only 450MB of memory is free when the database starts, then the database will" +
            " map at most 450MB. If `50%` is configured, and the system has a capacity of" +
            " 4GB, then at most 2GB of memory will be mapped, unless the database observes" +
            " that less than 2GB of memory is free when it starts.")
    public static final Setting<Long> mapped_memory_total_size = setting("mapped_memory_total_size", directMemoryUsage(), "50%" );

    @Description( "Log memory mapping statistics regularly." )
    public static final Setting<Boolean> log_mapped_memory_stats = setting("log_mapped_memory_stats", BOOLEAN, FALSE );

    @Description( "The file where memory mapping statistics will be recorded." )
    public static final Setting<File> log_mapped_memory_stats_filename = setting("log_mapped_memory_stats_filename", PATH, "mapped_memory_stats.log", basePath(store_dir) );

    @Description("The number of records to be loaded between regular logging of memory mapping statistics.")
    public static final Setting<Integer> log_mapped_memory_stats_interval = setting("log_mapped_memory_stats_interval", INTEGER, "1000000");

    /**
     * @deprecated Replaced by the mapped_memory_total_size setting.
     */
    @Deprecated
    @Obsoleted( "Replaced by the mapped_memory_total_size setting." )
    @Description("The size to allocate for memory mapping the node store.")
    public static final Setting<Long> nodestore_mapped_memory_size = setting("neostore.nodestore.db.mapped_memory", BYTES, NO_DEFAULT );

    /**
     * @deprecated Replaced by the mapped_memory_total_size setting.
     */
    @Deprecated
    @Obsoleted( "Replaced by the mapped_memory_total_size setting." )
    @Description("The size to allocate for memory mapping the property value store.")
    public static final Setting<Long> nodestore_propertystore_mapped_memory_size = setting("neostore.propertystore.db.mapped_memory", BYTES, NO_DEFAULT );

    /**
     * @deprecated Replaced by the mapped_memory_total_size setting.
     */
    @Deprecated
    @Obsoleted( "Replaced by the mapped_memory_total_size setting." )
    @Description("The size to allocate for memory mapping the store for property key indexes.")
    public static final Setting<Long> nodestore_propertystore_index_mapped_memory_size = setting("neostore.propertystore.db.index.mapped_memory", BYTES, NO_DEFAULT );

    /**
     * @deprecated Replaced by the mapped_memory_total_size setting.
     */
    @Obsoleted( "Replaced by the mapped_memory_total_size setting." )
    @Deprecated
    @Description("The size to allocate for memory mapping the store for property key strings.")
    public static final Setting<Long> nodestore_propertystore_index_keys_mapped_memory_size = setting("neostore.propertystore.db.index.keys.mapped_memory", BYTES, NO_DEFAULT );

    /**
     * @deprecated Replaced by the mapped_memory_total_size setting.
     */
    @Deprecated
    @Obsoleted( "Replaced by the mapped_memory_total_size setting." )
    @Description("The size to allocate for memory mapping the string property store.")
    public static final Setting<Long> strings_mapped_memory_size = setting("neostore.propertystore.db.strings.mapped_memory", BYTES, NO_DEFAULT );

    /**
     * @deprecated Replaced by the mapped_memory_total_size setting.
     */
    @Deprecated
    @Obsoleted( "Replaced by the mapped_memory_total_size setting." )
    @Description("The size to allocate for memory mapping the array property store.")
    public static final Setting<Long> arrays_mapped_memory_size = setting("neostore.propertystore.db.arrays.mapped_memory", BYTES, NO_DEFAULT );

    /**
     * @deprecated Replaced by the mapped_memory_total_size setting.
     */
    @Deprecated
    @Obsoleted( "Replaced by the mapped_memory_total_size setting." )
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

    @Description("Whether or not transactions are appended to the log in batches")
    public static final Setting<Boolean> batched_writes = setting( "batched_writes", BOOLEAN, Boolean.TRUE.toString() );

    @Description( "Log executed queries that takes longer than the configured threshold." )
    public static final Setting<Boolean> log_queries = setting("dbms.querylog.enabled", BOOLEAN, FALSE );

    @Description( "The file where queries will be recorded." )
    public static final Setting<File> log_queries_filename = setting("dbms.querylog.filename", PATH, "queries.log", basePath(store_dir) );

    @Description("If the execution of query takes more time than this threshold, the query is logged - " +
            "provided query logging is enabled. Defaults to 0 seconds, that is all queries are logged.")
    public static final Setting<Long> log_queries_threshold = setting("dbms.querylog.threshold", DURATION, "0s");

    private static String[] availableCaches()
    {
        List<String> available = new ArrayList<>();
        for ( CacheProvider cacheProvider : Service.load( CacheProvider.class ) )
        {
            available.add( cacheProvider.getName() );
        }

        // Temporary hidden config to turn off cache layer entirely
        available.add( CacheLayer.EXPERIMENTAL_OFF );

                                           // --- higher prio ---->
        for ( String prioritized : new String[] { "soft", "hpc" } )
        {
            if ( available.remove( prioritized ) )
            {
                available.add( 0, prioritized );
            }
        }
        return available.toArray( new String[available.size()] );
    }
}
