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
import org.neo4j.kernel.configuration.Migrator;
import org.neo4j.kernel.configuration.Title;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.MonitorGc;

import static org.neo4j.helpers.Settings.ANY;
import static org.neo4j.helpers.Settings.BOOLEAN;
import static org.neo4j.helpers.Settings.BYTES;
import static org.neo4j.helpers.Settings.FALSE;
import static org.neo4j.helpers.Settings.INTEGER;
import static org.neo4j.helpers.Settings.NO_DEFAULT;
import static org.neo4j.helpers.Settings.PATH;
import static org.neo4j.helpers.Settings.STRING;
import static org.neo4j.helpers.Settings.TRUE;
import static org.neo4j.helpers.Settings.basePath;
import static org.neo4j.helpers.Settings.illegalValueMessage;
import static org.neo4j.helpers.Settings.matches;
import static org.neo4j.helpers.Settings.min;
import static org.neo4j.helpers.Settings.options;
import static org.neo4j.helpers.Settings.port;
import static org.neo4j.helpers.Settings.setting;

/**
 * Settings for Neo4j. Use this with {@link GraphDatabaseBuilder}.
 */
@Description("Settings for the Community edition of Neo4j")
public abstract class GraphDatabaseSettings
{
    @Migrator
    private static final ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator();

    @Title("Read only database")
    @Description("Only allow read operations from this Neo4j instance. This mode still requires write access to the directory for lock purposes")
    public static final Setting<Boolean> read_only = setting( "read_only", BOOLEAN, FALSE );

    @Description("The type of cache to use for nodes and relationships. "
                  + "Note that the Neo4j Enterprise Edition has the additional 'hpc' cache type (High-Performance Cache). "
            + "See the chapter on caches in the manual for more information.")
    public static final Setting<String> cache_type = setting( "cache_type", options( availableCaches() ), availableCaches()[0] );

    @Description("Print out the effective Neo4j configuration after startup.")
    public static final Setting<Boolean> dump_configuration = setting("dump_configuration", BOOLEAN, FALSE );

    @Description("The name of the Transaction Manager service to use as defined in the TM service provider " +
            "constructor.")
    public static final Setting<String> tx_manager_impl = setting("tx_manager_impl", STRING, "native",
            illegalValueMessage( "Must be class name of TransactionManager implementation", matches( ANY )));

    @Description("Whether to allow a store upgrade in case the current version of the database starts against an " +
            "older store version. " +
            "Setting this to true does not guarantee successful upgrade, just " +
            "that it allows an attempt at it.")
    public static final Setting<Boolean> allow_store_upgrade = setting("allow_store_upgrade", BOOLEAN, FALSE );

    @Description("Determines whether any TransactionInterceptors loaded will intercept " +
            "externally received transactions (e.g. in HA) before they reach the " +
            "logical log and are applied to the store.")
    public static final Setting<Boolean> intercept_deserialized_transactions = setting("intercept_deserialized_transactions", BOOLEAN, FALSE);

    // Cypher settings
    // TODO: These should live with cypher
    @Description("Enable this to specify a parser other than the default one.")
    public static final Setting<String> cypher_parser_version = setting(
            "cypher_parser_version",
            options( "1.9", "2.0" ), NO_DEFAULT );

    @Description("Used to set the number of Cypher query execution plans that are cached.")
    public static Setting<Integer> query_cache_size = setting( "query_cache_size", INTEGER, "100", min( 0 ) );

    // Store files
    @Description("The directory where the database files are located.")
    public static final Setting<File> store_dir = setting("store_dir", PATH, NO_DEFAULT );

    @Description("The base name for the Neo4j Store files, either an absolute path or relative to the store_dir " +
            "setting. This should generally not be changed.")
    public static final Setting<File> neo_store = setting("neo_store", PATH, "neostore", basePath(store_dir) );

    @Description("The base name for the logical log files, either an absolute path or relative to the store_dir " +
            "setting. This should generally not be changed.")
    public static final Setting<File> logical_log = setting("logical_log", PATH, "nioneo_logical.log", basePath(store_dir));

    // Remote logging
    @Description("Whether to enable logging to a remote server or not.")
    public static final Setting<Boolean> remote_logging_enabled = setting("remote_logging_enabled", BOOLEAN, FALSE );

    @Description("Host for remote logging using LogBack SocketAppender.")
    public static final Setting<String> remote_logging_host = setting("remote_logging_host", STRING, "127.0.0.1", illegalValueMessage( "Must be a valid hostname", matches( ANY ) ) );

    @Description("Port for remote logging using LogBack SocketAppender.")
    public static final Setting<Integer> remote_logging_port = setting("remote_logging_port", INTEGER, "4560", port );

    // TODO: Turn this into ByteSizeSetting, and make sure this applies to all logging providers
    @Description("Threshold in bytes for when database logs (text logs, for debugging, that is) are rotated.")
    public static final Setting<Integer> threshold_for_logging_rotation = setting("logging.threshold_for_rotation", INTEGER, "" + (100 * 1024 * 1024), min(1) );

    // Indexing
    @Description("Controls the auto indexing feature for nodes. Setting to false shuts it down, " +
            "while true enables it by default for properties "
            + "listed in the node_keys_indexable setting.")
    public static final Setting<Boolean> node_auto_indexing = setting("node_auto_indexing", BOOLEAN, FALSE);

    @Description("A list of property names (comma separated) that will be indexed by default. This applies to Nodes " +
            "only.")
    public static final Setting<String> node_keys_indexable = setting("node_keys_indexable", STRING, NO_DEFAULT, illegalValueMessage( "Must be a comma-separated list of keys to be indexed", matches( ANY ) ) );

    @Description("Controls the auto indexing feature for relationships. Setting to false shuts it down, " +
            "while true enables it by default for properties "
            + "listed in the relationship_keys_indexable setting.")
    public static final Setting<Boolean> relationship_auto_indexing = setting("relationship_auto_indexing",BOOLEAN, FALSE );

    @Description(" A list of property names (comma separated) that will be indexed by default. This applies to " +
            "Relationships only.")
    public static final Setting<String> relationship_keys_indexable = setting("relationship_keys_indexable", STRING, NO_DEFAULT, illegalValueMessage( "Must be a comma-separated list of keys to be indexed", matches( ANY ) ) );

    // Lucene settings
    @Description("Integer value that sets the maximum number of open lucene index searchers.")
    public static Setting<Integer> lucene_searcher_cache_size = setting("lucene_searcher_cache_size",INTEGER, Integer.toString( Integer.MAX_VALUE ), min( 1 ));

    // NeoStore settings
    @Description("Determines whether any TransactionInterceptors loaded will intercept prepared transactions before " +
            "they reach the logical log.")
    public static final Setting<Boolean> intercept_committing_transactions = setting("intercept_committing_transactions", BOOLEAN, FALSE );

    @Description("Make Neo4j keep the logical transaction logs for being able to backup the database." +
            "Can be used for specifying the threshold to prune logical logs after. For example \"10 days\" will " +
            "prune logical logs that only contains transactions older than 10 days from the current time, " +
            "or \"100k txs\" will keep the 100k latest transactions and prune any older transactions.")
    public static final Setting<String> keep_logical_logs = setting("keep_logical_logs", STRING, "7 days", illegalValueMessage( "Must be 'true'/'false' or of format '<number><optional unit> <type>' for example '100M size' for " +
                        "limiting logical log space on disk to 100Mb," +
                        " or '200k txs' for limiting the number of transactions to keep to 200 000.", matches(ANY)));

    @Description( "Specifies at which file size the logical log will auto-rotate. " +
                  "0 means that no rotation will automatically occur based on file size. " +
                  "Default is 25M" )
    public static final Setting<Long> logical_log_rotation_threshold = setting( "logical_log_rotation_threshold", BYTES, "25M" );

    @Description("Use a quick approach for rebuilding the ID generators. This give quicker recovery time, " +
            "but will limit the ability to reuse the space of deleted entities.")
    public static final Setting<Boolean> rebuild_idgenerators_fast = setting("rebuild_idgenerators_fast", BOOLEAN, TRUE );

    // NeoStore memory settings
    @Description("Tell Neo4j to use memory mapped buffers for accessing the native storage layer.")
    public static final Setting<Boolean> use_memory_mapped_buffers = setting( "use_memory_mapped_buffers", BOOLEAN, Boolean.toString(!Settings.osIsWindows()));

    @Description("Target size for pages of mapped memory.")
    public static final Setting<Long> mapped_memory_page_size = setting("mapped_memory_page_size", BYTES, "1M" );

    @Description("The size to allocate for a memory mapping pool to be shared between all stores.")
    public static final Setting<Long> all_stores_total_mapped_memory_size = setting("all_stores_total_mapped_memory_size", BYTES, "500M" );

    @Description("Tell Neo4j to regularly log memory mapping statistics.")
    public static final Setting<Boolean> log_mapped_memory_stats = setting("log_mapped_memory_stats", BOOLEAN, FALSE );

    @Description("The file where Neo4j will record memory mapping statistics.")
    public static final Setting<File> log_mapped_memory_stats_filename = setting("log_mapped_memory_stats_filename", PATH, "mapped_memory_stats.log", basePath(store_dir) );

    @Description("The number of records to be loaded between regular logging of memory mapping statistics.")
    public static final Setting<Integer> log_mapped_memory_stats_interval = setting("log_mapped_memory_stats_interval", INTEGER, "1000000");

    @Description("The size to allocate for memory mapping the node store.")
    public static final Setting<Long> nodestore_mapped_memory_size = setting("neostore.nodestore.db.mapped_memory", BYTES, "20M" );

    @Description("The size to allocate for memory mapping the property value store.")
    public static final Setting<Long> nodestore_propertystore_mapped_memory_size = setting("neostore.propertystore.db.mapped_memory", BYTES, "90M" );

    @Description("The size to allocate for memory mapping the store for property key indexes.")
    public static final Setting<Long> nodestore_propertystore_index_mapped_memory_size = setting("neostore.propertystore.db.index.mapped_memory", BYTES, "1M" );

    @Description("The size to allocate for memory mapping the store for property key strings.")
    public static final Setting<Long> nodestore_propertystore_index_keys_mapped_memory_size = setting("neostore.propertystore.db.index.keys.mapped_memory", BYTES, "1M" );

    @Description("The size to allocate for memory mapping the string property store.")
    public static final Setting<Long> strings_mapped_memory_size = setting("neostore.propertystore.db.strings.mapped_memory", BYTES, "130M" );

    @Description("The size to allocate for memory mapping the array property store.")
    public static final Setting<Long> arrays_mapped_memory_size = setting("neostore.propertystore.db.arrays.mapped_memory", BYTES, "130M" );

    @Description("The size to allocate for memory mapping the relationship store.")
    public static final Setting<Long> relationshipstore_mapped_memory_size = setting("neostore.relationshipstore.db.mapped_memory", BYTES, "100M" );


    @Description("How many relationships to read at a time during iteration")
    public static final Setting<Integer> relationship_grab_size = setting("relationship_grab_size", INTEGER, "100", min( 1 ));

    @Description("Specifies the block size for storing strings. This parameter is only honored when the store is " +
            "created, otherwise it is ignored. " +
            "Note that each character in a string occupies two bytes, meaning that a block size of 120 (the default " +
            "size) will hold a 60 character " +
            "long string before overflowing into a second block. Also note that each block carries an overhead of 8 " +
            "bytes. " +
            "This means that if the block size is 120, the size of the stored records will be 128 bytes.")
    public static final Setting<Integer> string_block_size = setting("string_block_size", INTEGER, "120",min(1));

    @Description("Specifies the block size for storing arrays. This parameter is only honored when the store is " +
            "created, otherwise it is ignored. " +
            "The default block size is 120 bytes, and the overhead of each block is the same as for string blocks, " +
            "i.e., 8 bytes.")
    public static final Setting<Integer> array_block_size = setting("array_block_size", INTEGER, "120",min(1));

    @Description("Specifies the block size for storing labels exceeding in-lined space in node record. " +
    		"This parameter is only honored when the store is created, otherwise it is ignored. " +
            "The default block size is 60 bytes, and the overhead of each block is the same as for string blocks, " +
            "i.e., 8 bytes.")
    public static final Setting<Integer> label_block_size = setting("label_block_size", INTEGER, "60",min(1));

    @Description("Mark this database as a backup slave.")
    public static final Setting<Boolean> backup_slave = setting("backup_slave", BOOLEAN, FALSE );

    @Description("An identifier that uniquely identifies this graph database instance within this JVM. " +
            "Defaults to an auto-generated number depending on how many instance are started in this JVM.")
    public static final Setting<String> forced_kernel_id = setting("forced_kernel_id", STRING, NO_DEFAULT, illegalValueMessage( "invalid kernel identifier", matches( "[a-zA-Z0-9]*" ) ));

    public static final Setting<Boolean> execution_guard_enabled = setting("execution_guard_enabled", BOOLEAN, FALSE );

    @Description("Amount of time in ms the GC monitor thread will wait before taking another measurement.")
    public static final Setting<Long> gc_monitor_interval = MonitorGc.Configuration.gc_monitor_wait_time;

    @Description("The amount of time in ms the monitor thread has to be blocked before logging a message it was " +
            "blocked.")
    public static final Setting<Long> gc_monitor_block_threshold = MonitorGc.Configuration.gc_monitor_threshold;

    private static String[] availableCaches()
    {
        List<String> available = new ArrayList<>();
        for ( CacheProvider cacheProvider : Service.load( CacheProvider.class ) )
        {
            available.add( cacheProvider.getName() );
        }
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
