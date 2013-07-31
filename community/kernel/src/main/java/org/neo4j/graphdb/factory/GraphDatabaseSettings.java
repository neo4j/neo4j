/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.BooleanSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.FloatSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.IntegerSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.NumberOfBytesSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.OptionsSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.PortSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.StringSetting;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.configuration.ConfigurationMigrator;
import org.neo4j.kernel.configuration.GraphDatabaseConfigurationMigrator;
import org.neo4j.kernel.configuration.Migrator;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.MonitorGc;

import static org.neo4j.helpers.Settings.ANY;
import static org.neo4j.helpers.Settings.BOOLEAN;
import static org.neo4j.helpers.Settings.BYTES;
import static org.neo4j.helpers.Settings.DURATION_FORMAT;
import static org.neo4j.helpers.Settings.FALSE;
import static org.neo4j.helpers.Settings.FLOAT;
import static org.neo4j.helpers.Settings.INTEGER;
import static org.neo4j.helpers.Settings.NO_DEFAULT;
import static org.neo4j.helpers.Settings.PATH;
import static org.neo4j.helpers.Settings.SIZE_FORMAT;
import static org.neo4j.helpers.Settings.STRING;
import static org.neo4j.helpers.Settings.TRUE;
import static org.neo4j.helpers.Settings.basePath;
import static org.neo4j.helpers.Settings.illegalValueMessage;
import static org.neo4j.helpers.Settings.matches;
import static org.neo4j.helpers.Settings.min;
import static org.neo4j.helpers.Settings.options;
import static org.neo4j.helpers.Settings.port;
import static org.neo4j.helpers.Settings.range;
import static org.neo4j.helpers.Settings.setting;

/**
 * Settings for Neo4j. Use this with {@link GraphDatabaseBuilder}.
 */
@Description("Settings for the Community edition of Neo4j")
@SuppressWarnings("deprecation"/*although it might be a good idea to go through and check these every once in a while*/)
public abstract class GraphDatabaseSettings
{
    @Migrator
    private static final ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator();

    @Title("Read only database")
    @Description("Only allow read operations from this Neo4j instance. This mode still requires write access to the directory for lock purposes")
    public static final BooleanSetting read_only = new BooleanSetting( setting( "read_only", BOOLEAN, FALSE ));

    @Description("The type of cache to use for nodes and relationships. "
            + "Note that the Neo4j Enterprise Edition has the additional 'gcr' cache type. "
            + "See the chapter on caches in the manual for more information.")
    public static final CacheTypeSetting cache_type = new CacheTypeSetting( setting( "cache_type",
            options( CacheTypeSetting.availableCaches() ), CacheTypeSetting.availableCaches()[0] ) );

    @Description( "Enable loading kernel extensions" )
    public static final BooleanSetting load_kernel_extensions = new BooleanSetting( setting("load_kernel_extensions", BOOLEAN, TRUE ));

    @Description("Print out the effective Neo4j configuration after startup.")
    public static final BooleanSetting dump_configuration = new BooleanSetting( setting("dump_configuration", BOOLEAN, FALSE ));

    @Description("The name of the Transaction Manager service to use as defined in the TM service provider " +
            "constructor.")
    public static final StringSetting tx_manager_impl = new StringSetting( setting("tx_manager_impl", STRING, "native",
            illegalValueMessage( "Must be class name of TransactionManager implementation", matches( ANY ))));

    @Description("Whether to allow a store upgrade in case the current version of the database starts against an " +
            "older store version. " +
            "Setting this to true does not guarantee successful upgrade, just " +
            "that it allows an attempt at it.")
    public static final BooleanSetting allow_store_upgrade = new BooleanSetting( setting("allow_store_upgrade", BOOLEAN, FALSE) );

    @Description("Determines whether any TransactionInterceptors loaded will intercept " +
            "externally received transactions (e.g. in HA) before they reach the " +
            "logical log and are applied to the store.")
    public static final BooleanSetting intercept_deserialized_transactions = new BooleanSetting( setting("intercept_deserialized_transactions", BOOLEAN, FALSE) );

    // Cypher settings
    // TODO: These should live with cypher
    @Description("Enable this to specify a parser other than the default one.")
    public static final OptionsSetting cypher_parser_version = new CypherParserSetting( setting(
            "cypher_parser_version",
            options( CypherParserSetting.v1_9, CypherParserSetting.v2_0 ), NO_DEFAULT ) );

    @Description("Used to set the number of Cypher query execution plans that are cached.")
    public static GraphDatabaseSetting<Integer> query_cache_size = new IntegerSetting( setting("query_cache_size", INTEGER, "100", min( 0 ) ));

    // Store files
    @Description("The directory where the database files are located.")
    public static final GraphDatabaseSetting.DirectorySetting store_dir = new GraphDatabaseSetting.DirectorySetting(setting("store_dir", PATH, NO_DEFAULT ));

    @Description("The base name for the Neo4j Store files, either an absolute path or relative to the store_dir " +
            "setting. This should generally not be changed.")
    public static final GraphDatabaseSetting.FileSetting neo_store = new GraphDatabaseSetting.FileSetting(setting("neo_store", PATH, "neostore", basePath(store_dir) ));

    @Description("The base name for the logical log files, either an absolute path or relative to the store_dir " +
            "setting. This should generally not be changed.")
    public static final GraphDatabaseSetting.FileSetting logical_log = new GraphDatabaseSetting.FileSetting(setting("logical_log", PATH, "nioneo_logical.log", basePath(store_dir)));

    // Remote logging
    @Description("Whether to enable logging to a remote server or not.")
    public static final GraphDatabaseSetting<Boolean> remote_logging_enabled = new BooleanSetting(setting("remote_logging_enabled", BOOLEAN, FALSE ));

    @Description("Host for remote logging using LogBack SocketAppender.")
    public static final StringSetting remote_logging_host = new StringSetting( setting("remote_logging_host", STRING, "127.0.0.1", illegalValueMessage( "Must be a valid hostname", matches( ANY ) ) ));

    @Description("Port for remote logging using LogBack SocketAppender.")
    public static final PortSetting remote_logging_port = new PortSetting( setting("remote_logging_port", INTEGER, "4560", port ));

    // TODO: Turn this into ByteSizeSetting, and make sure this applies to all logging providers
    @Description("Threshold in bytes for when database logs (text logs, for debugging, that is) are rotated.")
    public static final GraphDatabaseSetting.IntegerSetting threshold_for_logging_rotation =
            new GraphDatabaseSetting.IntegerSetting( setting("logging.threshold_for_rotation", INTEGER, "" + (100 * 1024 * 1024), min(1) ));

    // Indexing
    @Description("Controls the auto indexing feature for nodes. Setting to false shuts it down, " +
            "while true enables it by default for properties "
            + "listed in the node_keys_indexable setting.")
    public static final BooleanSetting node_auto_indexing = new BooleanSetting( setting("node_auto_indexing", BOOLEAN, FALSE ));

    @Description("A list of property names (comma separated) that will be indexed by default. This applies to Nodes " +
            "only.")
    public static final StringSetting node_keys_indexable = new StringSetting( setting("node_keys_indexable", STRING, NO_DEFAULT, illegalValueMessage( "Must be a comma-separated list of keys to be indexed", matches( ANY ) ) ));

    @Description("Controls the auto indexing feature for relationships. Setting to false shuts it down, " +
            "while true enables it by default for properties "
            + "listed in the relationship_keys_indexable setting.")
    public static final BooleanSetting relationship_auto_indexing = new BooleanSetting( setting("relationship_auto_indexing",BOOLEAN, FALSE ));

    @Description(" A list of property names (comma separated) that will be indexed by default. This applies to " +
            "Relationships only.")
    public static final StringSetting relationship_keys_indexable = new StringSetting( setting("relationship_keys_indexable", STRING, NO_DEFAULT, illegalValueMessage( "Must be a comma-separated list of keys to be indexed", matches( ANY ) ) ));

    // Lucene settings
    @Description("Integer value that sets the maximum number of open lucene index searchers.")
    public static IntegerSetting lucene_searcher_cache_size =
            new IntegerSetting( setting("lucene_searcher_cache_size",INTEGER, Integer.toString( Integer.MAX_VALUE ), min( 1 )));

    @Description("NOTE: This no longer has any effect. Integer value that sets the maximum number of open lucene " +
            "index writers.")
    @Deprecated
    public static IntegerSetting lucene_writer_cache_size = new IntegerSetting( setting("lucene_writer_cache_size", INTEGER, Integer.toString( Integer.MAX_VALUE), min(1) ));

    // NeoStore settings
    @Description("Determines whether any TransactionInterceptors loaded will intercept prepared transactions before " +
            "they reach the logical log.")
    public static final BooleanSetting intercept_committing_transactions = new BooleanSetting(setting("intercept_committing_transactions", BOOLEAN, FALSE ));

    @Description("Make Neo4j keep the logical transaction logs for being able to backup the database." +
            "Can be used for specifying the threshold to prune logical logs after. For example \"10 days\" will " +
            "prune logical logs that only contains transactions older than 10 days from the current time, " +
            "or \"100k txs\" will keep the 100k latest transactions and prune any older transactions.")
    public static final StringSetting keep_logical_logs = new StringSetting( setting("keep_logical_logs", STRING, TRUE, illegalValueMessage( "Must be 'true'/'false' or of format '<number><optional unit> <type>' for example '100M size' for " +
                        "limiting logical log space on disk to 100Mb," +
                        " or '200k txs' for limiting the number of transactions to keep to 200 000.", matches(ANY))));
    
    @Description( "Specifies at which file size the logical log will auto-rotate. " +
                  "0 means that no rotation will automatically occur based on file size. " +
                  "Default is 25M" )
    public static final Setting<Long> logical_log_rotation_threshold = setting( "logical_log_rotation_threshold",
            Settings.LONG_WITH_OPTIONAL_UNIT, "25M" );

    @Description("Use a quick approach for rebuilding the ID generators. This give quicker recovery time, " +
            "but will limit the ability to reuse the space of deleted entities.")
    public static final BooleanSetting rebuild_idgenerators_fast = new BooleanSetting( setting("rebuild_idgenerators_fast",BOOLEAN, TRUE ));

    // NeoStore memory settings
    @Description("Tell Neo4j to use memory mapped buffers for accessing the native storage layer.")
    public static final UseMemoryMappedBuffers use_memory_mapped_buffers = new UseMemoryMappedBuffers(setting( "use_memory_mapped_buffers", BOOLEAN, NO_DEFAULT ));

    @Description("Target size for pages of mapped memory.")
    public static final GraphDatabaseSetting<Long> mapped_memory_page_size = new NumberOfBytesSetting(setting("mapped_memory_page_size", BYTES, "1M" ));

    @Description("The size to allocate for a memory mapping pool to be shared between all stores.")
    public static final GraphDatabaseSetting<Long> all_stores_total_mapped_memory_size = new NumberOfBytesSetting(setting("all_stores_total_mapped_memory_size", BYTES, "500M" ));

    @Description("Tell Neo4j to regularly log memory mapping statistics.")
    public static final GraphDatabaseSetting<Boolean> log_mapped_memory_stats = new BooleanSetting(setting("log_mapped_memory_stats", BOOLEAN, FALSE ));

    @Description("The file where Neo4j will record memory mapping statistics.")
    public static final GraphDatabaseSetting.FileSetting log_mapped_memory_stats_filename =
            new GraphDatabaseSetting.FileSetting( setting("log_mapped_memory_stats_filename", PATH, "mapped_memory_stats.log", basePath(store_dir) ));

    @Description("The number of records to be loaded between regular logging of memory mapping statistics.")
    public static final GraphDatabaseSetting<Integer> log_mapped_memory_stats_interval = new IntegerSetting(setting("log_mapped_memory_stats_interval", INTEGER, "1000000"));

    @Description("The size to allocate for memory mapping the node store.")
    public static final GraphDatabaseSetting<Long> nodestore_mapped_memory_size =
            new NumberOfBytesSetting( setting("neostore.nodestore.db.mapped_memory", BYTES, "20M" ));

    @Description("The size to allocate for memory mapping the property value store.")
    public static final GraphDatabaseSetting<Long> nodestore_propertystore_mapped_memory_size =
            new NumberOfBytesSetting( setting("neostore.propertystore.db.mapped_memory", BYTES, "90M" ));

    @Description("The size to allocate for memory mapping the store for property key indexes.")
    public static final GraphDatabaseSetting<Long> nodestore_propertystore_index_mapped_memory_size =
            new NumberOfBytesSetting( setting("neostore.propertystore.db.index.mapped_memory", BYTES, "1M" ));

    @Description("The size to allocate for memory mapping the store for property key strings.")
    public static final GraphDatabaseSetting<Long> nodestore_propertystore_index_keys_mapped_memory_size =
            new NumberOfBytesSetting( setting("neostore.propertystore.db.index.keys.mapped_memory", BYTES, "1M" ));

    @Description("The size to allocate for memory mapping the string property store.")
    public static final GraphDatabaseSetting<Long> strings_mapped_memory_size =
            new NumberOfBytesSetting( setting("neostore.propertystore.db.strings.mapped_memory", BYTES, "130M" ));

    @Description("The size to allocate for memory mapping the array property store.")
    public static final GraphDatabaseSetting<Long> arrays_mapped_memory_size =
            new NumberOfBytesSetting( setting("neostore.propertystore.db.arrays.mapped_memory", BYTES, "130M" ));

    @Description("The size to allocate for memory mapping the relationship store.")
    public static final GraphDatabaseSetting<Long> relationshipstore_mapped_memory_size =
            new NumberOfBytesSetting(setting("neostore.relationshipstore.db.mapped_memory", BYTES, "100M" ));

    // Deprecated memory settings (these use String rather than NumberOfBytes)

    @Description("The size to allocate for memory mapping the node store.")
    @Deprecated
    public static final StringSetting nodestore_mapped_memory =
            new StringSetting( setting("neostore.nodestore.db.mapped_memory", STRING, "20M", matches( SIZE_FORMAT) ));

    @Description("The size to allocate for memory mapping the property value store.")
    @Deprecated
    public static final StringSetting nodestore_propertystore_mapped_memory =
            new StringSetting( setting("neostore.propertystore.db.mapped_memory", STRING, "90M", matches(SIZE_FORMAT)));

    @Description("The size to allocate for memory mapping the store for property key indexes.")
    @Deprecated
    public static final StringSetting nodestore_propertystore_index_mapped_memory =
            new StringSetting( setting("neostore.propertystore.db.index.mapped_memory", STRING, "1M", matches(SIZE_FORMAT) ));

    @Description("The size to allocate for memory mapping the store for property key strings.")
    @Deprecated
    public static final StringSetting nodestore_propertystore_index_keys_mapped_memory =
            new StringSetting( setting("neostore.propertystore.db.index.keys.mapped_memory", STRING, "1M", matches(SIZE_FORMAT) ));

    @Description("The size to allocate for memory mapping the string property store.")
    @Deprecated
    public static final StringSetting strings_mapped_memory =
            new StringSetting( setting("neostore.propertystore.db.strings.mapped_memory", STRING, "130M", matches(SIZE_FORMAT)));

    @Description("The size to allocate for memory mapping the array property store.")
    @Deprecated
    public static final StringSetting arrays_mapped_memory =
            new StringSetting( setting("neostore.propertystore.db.arrays.mapped_memory", STRING, "130M", matches(SIZE_FORMAT)));

    @Description("The size to allocate for memory mapping the relationship store.")
    @Deprecated
    public static final StringSetting relationshipstore_mapped_memory =
            new StringSetting( setting("neostore.relationshipstore.db.mapped_memory", STRING, "100M", matches(SIZE_FORMAT)));

    @Description("How many relationships to read at a time during iteration")
    public static final IntegerSetting relationship_grab_size =
            new IntegerSetting( setting("relationship_grab_size", INTEGER, "100", min( 1 )));

    @Description("Whether to grab locks on files or not.")
    @Deprecated
    public static final BooleanSetting grab_file_lock = new BooleanSetting( setting("grab_file_lock", BOOLEAN, TRUE ));

    @Description("Specifies the block size for storing strings. This parameter is only honored when the store is " +
            "created, otherwise it is ignored. " +
            "Note that each character in a string occupies two bytes, meaning that a block size of 120 (the default " +
            "size) will hold a 60 character " +
            "long string before overflowing into a second block. Also note that each block carries an overhead of 8 " +
            "bytes. " +
            "This means that if the block size is 120, the size of the stored records will be 128 bytes.")
    public static final IntegerSetting string_block_size =
            new IntegerSetting( setting("string_block_size", INTEGER, "120",min(1)));

    @Description("Specifies the block size for storing arrays. This parameter is only honored when the store is " +
            "created, otherwise it is ignored. " +
            "The default block size is 120 bytes, and the overhead of each block is the same as for string blocks, " +
            "i.e., 8 bytes.")
    public static final IntegerSetting array_block_size =
            new IntegerSetting( setting("array_block_size", INTEGER, "120",min(1)));

    @Description("Specifies the block size for storing labels exceeding in-lined space in node record. " +
    		"This parameter is only honored when the store is created, otherwise it is ignored. " +
            "The default block size is 60 bytes, and the overhead of each block is the same as for string blocks, " +
            "i.e., 8 bytes.")
    public static final IntegerSetting label_block_size =
            new IntegerSetting( setting("label_block_size", INTEGER, "60",min(1)));

    @Description("Mark this database as a backup slave.")
    public static final BooleanSetting backup_slave = new BooleanSetting( setting("backup_slave", BOOLEAN, FALSE ));

    @Description("An identifier that uniquely identifies this graph database instance within this JVM. " +
            "Defaults to an auto-generated number depending on how many instance are started in this JVM.")
    public static final GraphDatabaseSetting<String> forced_kernel_id =
            new StringSetting( setting("forced_kernel_id", STRING, NO_DEFAULT, illegalValueMessage( "invalid kernel identifier", matches( "[a-zA-Z0-9]*" ) )));

    public static final BooleanSetting execution_guard_enabled = new BooleanSetting( setting("execution_guard_enabled", BOOLEAN, FALSE ));

    @Description("Amount of time in ms the GC monitor thread will wait before taking another measurement.")
    public static final GraphDatabaseSetting<Long> gc_monitor_interval = new GraphDatabaseSetting.LongSetting( MonitorGc.Configuration.gc_monitor_wait_time );

    @Description("The amount of time in ms the monitor thread has to be blocked before logging a message it was " +
            "blocked.")
    public static final GraphDatabaseSetting<Long> gc_monitor_block_threshold = new GraphDatabaseSetting.LongSetting( MonitorGc.Configuration.gc_monitor_threshold );

    // Deprecated GC monitor settings (old type)
    @Description("Amount of time in ms the GC monitor thread will wait before taking another measurement.")
    @Deprecated
    public static StringSetting gc_monitor_wait_time =
            new StringSetting( setting("gc_monitor_wait_time", STRING, "100ms", matches( DURATION_FORMAT )));

    @Description("The amount of time in ms the monitor thread has to be blocked before logging a message it was " +
            "blocked.")
    @Deprecated
    public static StringSetting gc_monitor_threshold =
            new StringSetting( setting("gc_monitor_threshold", STRING, "200ms",matches( DURATION_FORMAT)));

    // Old GCR size settings, using string values

    /**
     * Use GcrSettings.gcr_node_cache_size instead.
     */
    @Description("The amount of memory to use for the node cache (when using the 'gcr' cache).")
    @Deprecated
    public static final StringSetting node_cache_size =
            new StringSetting( setting("node_cache_size", STRING, NO_DEFAULT, matches( SIZE_FORMAT ) ));

    /**
     * Use GcrSettings.gcr_relationship_cache_size instead.
     */
    @Description("The amount of memory to use for the relationship cache (when using the 'gcr' cache).")
    @Deprecated
    public static final StringSetting relationship_cache_size =
            new StringSetting( setting("relationship_cache_size", STRING, NO_DEFAULT, matches( SIZE_FORMAT )));

    @Description("The fraction of the heap (1%-10%) to use for the base array in the node cache (when using the 'gcr'" +
            " cache).")
    @Deprecated
    public static final FloatSetting node_cache_array_fraction =
            new FloatSetting( setting("node_cache_array_fraction", FLOAT, "1.0", range( 1.0f, 10.0f )));

    @Description("The fraction of the heap (1%-10%) to use for the base array in the relationship cache (when using " +
            "the 'gcr' cache).")
    @Deprecated
    public static final FloatSetting relationship_cache_array_fraction =
            new FloatSetting(setting("relationship_cache_array_fraction", FLOAT, "1.0", range( 1.0f, 10.0f )));

    /**
     * Use GcrSettings.gcr_cache_log_interval instead.
     */
    @Description("The minimal time that must pass in between logging statistics from the cache (when using the 'gcr' " +
            "cache).")
    @Deprecated
    public static final StringSetting gcr_cache_min_log_interval =
            new StringSetting( setting("gcr_cache_min_log_interval",STRING, "60s", matches( DURATION_FORMAT) ));

    // Specialized settings
    public static class CacheTypeSetting
            extends GraphDatabaseSetting.SettingWrapper<String>
    {
        public CacheTypeSetting( Setting<String> setting )
        {
            super( setting );
        }

        @Description("Use weak reference cache.")
        public static final String weak = "weak";

        @Description("Provides optimal utilization of the available memory. Suitable for high performance traversal. " +
                "\n" +
                "May run into GC issues under high load if the frequently accessed parts of the graph does not fit in" +
                " the cache.")
        public static final String soft = "soft";

        @Description("Don't use caching.")
        public static final String none = "none";

        @Description("Use strong references.")
        public static final String strong = "strong";

        @Description("GC resistant cache. Gets assigned a configurable amount of space in the JVM heap \n" +
                "and will evict objects whenever it grows bigger than that, instead of relying on GC for eviction. \n" +
                "It has got the fastest insert/lookup times and should be optimal for most use cases. \n" +
                "This is the default cache setting.")
        public static final String gcr = "gcr";

        public static String[] availableCaches()
        {
            List<String> available = new ArrayList<String>();
            for ( CacheProvider cacheProvider : Service.load( CacheProvider.class ) )
                available.add( cacheProvider.getName() );
                                               // --- higher prio ---->
            for ( String prioritized : new String[] { "soft", "gcr" } )
                if ( available.remove( prioritized ) )
                    available.add( 0, prioritized );
            return available.toArray( new String[available.size()] );
        }
    }

    public static class CypherParserSetting
            extends OptionsSetting
    {
        @Description("Cypher v1.9 syntax.")
        public static final String v1_9 = "1.9";

        @Description("Cypher v2.0 syntax.")
        public static final String v2_0 = "2.0";

        public CypherParserSetting( Setting<String> setting )
        {
            super( setting );
        }
    }

    public static class UseMemoryMappedBuffers
            extends BooleanSetting
    {
        public UseMemoryMappedBuffers( Setting<Boolean> setting )
        {
            super( setting );
        }

        /**
         * Default for this setting is null, so wrap access with this call
         * to figure out actual value at runtime. If on Windows, don't do
         * memory mapping, and for other platforms do memory mapping
         */
        public static boolean shouldMemoryMap( Boolean useMemoryMapped )
        {
            if ( useMemoryMapped != null )
            {
                return useMemoryMapped;
            }
            else
            {
                // if on windows, default no memory mapping
                return !Settings.osIsWindows();
            }
        }
    }
}
