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

import static org.neo4j.graphdb.factory.GraphDatabaseSetting.ANY;
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.FALSE;
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.TRUE;

import org.neo4j.graphdb.factory.GraphDatabaseSetting.BooleanSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.DefaultValue;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.FloatSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.IntegerSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.NumberOfBytesSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.OptionsSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.PortSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting.StringSetting;
import org.neo4j.kernel.configuration.ConfigurationMigrator;
import org.neo4j.kernel.configuration.GraphDatabaseConfigurationMigrator;
import org.neo4j.kernel.configuration.Migrator;
import org.neo4j.kernel.impl.cache.MonitorGc;

/**
 * Settings for the Community edition of Neo4j. Use this with GraphDatabaseBuilder.
 */
/*
 For devs: This is rather messy right now, covered in @Deprecated stuff. The reason for that is that the majority
 of this API is due to be moved out of the public (org.neo4j.graphdb) namespace in 1.11. We can't just move them, since
 people might be using them.

 If you add any new settings here, please make sure that you use the (not deprecated) interface Setting as the field
 type, rather than the specific settings type you are using (eg. StringSetting or something).

 In 1.11, all of the deprecated special settings types will be made internal, and all the field types here will be changed
 to be the general Setting type rather than the specialized types most fields are now.

 Because Setting does not contain information about what type a setting string is converted to, we can't use the settings
 listed below to read from configuration. This is a weigh-off between duplication and the difficulty to change imposed by
 exposing type information to users (eg. we need to retain backwards compatibility with those types).

 The idea is that we use component-specific settings classes that define settings using GraphDatabaseSetting<T>, and then,
 and then we add references to those settings here. This class becomes outwards-facing user API, the internal
 one becomes the API we use. This has the added benefit of allowing internal settings that are not directly exposed to
 the user.
 */
@Description( "Settings for the Community edition of Neo4j" )
public abstract class GraphDatabaseSettings
{
    @Migrator
    private static final ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator();

    @Title( "Read only database" )
    @Description("Only allow read operations from this Neo4j instance.")
    @Default( FALSE)
    public static final BooleanSetting read_only = new BooleanSetting("read_only");

    @Description( "The type of cache to use for nodes and relationships. "
                  + "Note that the Neo4j Enterprise Edition has the additional 'gcr' cache type. "
                  + "See the chapter on caches in the manual for more information." )
    @Default( CacheTypeSetting.soft )
    public static final CacheTypeSetting cache_type = new CacheTypeSetting();

    @Default( TRUE)
    public static final BooleanSetting load_kernel_extensions = new BooleanSetting("load_kernel_extensions");

    @Description( "Print out the effective Neo4j configuration after startup." )
    @Default(FALSE)
    public static final BooleanSetting dump_configuration = new BooleanSetting( "dump_configuration" );

    @Description( "The name of the Transaction Manager service to use as defined in the TM service provider constructor, defaults to native." )
    public static final StringSetting tx_manager_impl = new StringSetting("tx_manager_impl",ANY,"Must be class name of TransactionManager implementation");

    @Description( "Whether to allow a store upgrade in case the current version of the database starts against an older store version. "+
                  "Setting this to true does not guarantee successful upgrade, just" +
                  "that it allows an attempt at it." )
    @Default( FALSE )
    public static BooleanSetting allow_store_upgrade = new BooleanSetting( "allow_store_upgrade" );

    @Description( "Determines whether any TransactionInterceptors loaded will intercept "+
                  "externally received transactions (e.g. in HA) before they reach the " +
                  "logical log and are applied to the store." )
    @Default(FALSE)
    public static final BooleanSetting intercept_deserialized_transactions = new BooleanSetting( "intercept_deserialized_transactions" );

    // Cypher settings
    // TODO: These should live with cypher
    @Description( "Enable this to specify a parser other than the default one." )
    public static final OptionsSetting cypher_parser_version = new CypherParserSetting();

    @Description( "Used to set the number of Cypher query execution plans that are cached." )
    @Default( "100" )
    public static GraphDatabaseSetting<Integer> query_cache_size = new IntegerSetting( "query_cache_size",
            "Query cache size must be at least 0.", 0, Integer.MAX_VALUE  );


    // Store files

    @Description("The directory where the database files are located.")
    public static final GraphDatabaseSetting.DirectorySetting store_dir = new GraphDatabaseSetting.DirectorySetting( "store_dir", true, true);

    @Description("The base name for the Neo4j Store files, either an absolute path or relative to the store_dir setting. This should generally not be changed.")
    @Default("neostore")
    public static final GraphDatabaseSetting.FileSetting neo_store = new GraphDatabaseSetting.FileSetting( "neo_store", store_dir, true, true);

    @Description("The base name for the logical log files, either an absolute path or relative to the store_dir setting. This should generally not be changed.")
    @Default("nioneo_logical.log")
    public static final GraphDatabaseSetting.FileSetting logical_log = new GraphDatabaseSetting.FileSetting( "logical_log", store_dir, true, true);

    // Remote logging
    @Description( "Whether to enable logging to a remote server or not." )
    @Default(FALSE)
    public static GraphDatabaseSetting<Boolean> remote_logging_enabled = new BooleanSetting( "remote_logging_enabled" );

    @Description( "Host for remote logging using LogBack SocketAppender." )
    @Default("127.0.0.1")
    public static final StringSetting remote_logging_host = new StringSetting( "remote_logging_host", ANY, "Must be a valid hostname" );

    @Description( "Port for remote logging using LogBack SocketAppender." )
    @Default("4560")
    public static final PortSetting remote_logging_port = new PortSetting( "remote_logging_port" );

    // TODO: Turn this into ByteSizeSetting, and make sure this applies to all logging providers
    @Description( "Threshold in bytes for when database logs (text logs, for debugging, that is) are rotated." )
    @Default(""+(100 * 1024 * 1024))
    public static final GraphDatabaseSetting.IntegerSetting threshold_for_logging_rotation = new GraphDatabaseSetting.IntegerSetting( "logging.threshold_for_rotation", "Must be valid number", 1, null );
    
    // Indexing
    @Description( "Controls the auto indexing feature for nodes. Setting to false shuts it down, while true enables it by default for properties "
                  + "listed in the node_keys_indexable setting." )
    @Default(FALSE)
    public static final BooleanSetting node_auto_indexing = new BooleanSetting( "node_auto_indexing" );

    @Description( "A list of property names (comma separated) that will be indexed by default. This applies to Nodes only." )
    public static final StringSetting node_keys_indexable = new StringSetting("node_keys_indexable",ANY,"Must be a comma-separated list of keys to be indexed");

    @Description( "Controls the auto indexing feature for relationships. Setting to false shuts it down, while true enables it by default for properties "
                  + "listed in the relationship_keys_indexable setting." )
    @Default(FALSE)
    public static final BooleanSetting relationship_auto_indexing = new BooleanSetting( "relationship_auto_indexing" );

    @Description( " A list of property names (comma separated) that will be indexed by default. This applies to Relationships only." )
    public static final StringSetting relationship_keys_indexable = new StringSetting("relationship_keys_indexable",ANY,"Must be a comma-separated list of keys to be indexed");

    // Lucene settings
    @Description( "Integer value that sets the maximum number of open lucene index searchers." )
    @Default(Integer.MAX_VALUE+"")
    public static IntegerSetting lucene_searcher_cache_size = new IntegerSetting( "lucene_searcher_cache_size", "Must be a number", 1, null );

    @Description( "NOTE: This no longer has any effect. Integer value that sets the maximum number of open lucene index writers." )
    @Default(Integer.MAX_VALUE+"")
    public static IntegerSetting lucene_writer_cache_size = new IntegerSetting( "lucene_writer_cache_size", "Must be a number", 1,null );

    // NeoStore settings
    @Description( "Determines whether any TransactionInterceptors loaded will intercept prepared transactions before they reach the logical log." )
    @Default(FALSE)
    public static final BooleanSetting intercept_committing_transactions = new BooleanSetting( "intercept_committing_transactions" );

    @Description( "Make Neo4j keep the logical transaction logs for being able to backup the database." +
            "Can be used for specifying the threshold to prune logical logs after. For example \"10 days\" will " +
            "prune logical logs that only contains transactions older than 10 days from the current time, " +
            "or \"100k txs\" will keep the 100k latest transactions and prune any older transactions." )
    @Default("true")
    public static final StringSetting keep_logical_logs = new StringSetting( "keep_logical_logs", ANY,
            "Must be 'true'/'false' or of format '<number><optional unit> <type>' for example '100M size' for limiting logical log space on disk to 100Mb," +
                    " or '200k txs' for limiting the number of transactions to keep to 200 000." );

    @Description( "Use a quick approach for rebuilding the ID generators. This give quicker recovery time, but will limit the ability to reuse the space of deleted entities." )
    @Default(TRUE)
    public static final BooleanSetting rebuild_idgenerators_fast = new BooleanSetting( "rebuild_idgenerators_fast" );

    // NeoStore memory settings
    @Description( "Tell Neo4j to use memory mapped buffers for accessing the native storage layer." )
    public static final UseMemoryMappedBuffers use_memory_mapped_buffers = new UseMemoryMappedBuffers();

    @Description( "Target size for pages of mapped memory." )
    @Default("1M")
    public static final GraphDatabaseSetting<Long> mapped_memory_page_size = new NumberOfBytesSetting("mapped_memory_page_size");

    @Description( "The size to allocate for a memory mapping pool to be shared between all stores." )
    @Default("500M")
    public static final GraphDatabaseSetting<Long> all_stores_total_mapped_memory_size = new NumberOfBytesSetting("all_stores_total_mapped_memory_size");

    @Description( "Tell Neo4j to regularly log memory mapping statistics." )
    @Default("false")
    public static final GraphDatabaseSetting<Boolean> log_mapped_memory_stats = new BooleanSetting("log_mapped_memory_stats");

    @Description("The file where Neo4j will record memory mapping statistics.")
    @Default("mapped_memory_stats.log")
    public static final GraphDatabaseSetting.FileSetting log_mapped_memory_stats_filename = new GraphDatabaseSetting.FileSetting( "log_mapped_memory_stats_filename", store_dir, true, true );

    @Description( "The number of records to be loaded between regular logging of memory mapping statistics." )
    @Default("1000000")
    public static final GraphDatabaseSetting<Integer> log_mapped_memory_stats_interval = new IntegerSetting("log_mapped_memory_stats_interval", "Must be a number");

    @Description( "The size to allocate for memory mapping the node store." )
    @Default("20M")
    public static final GraphDatabaseSetting<Long> nodestore_mapped_memory_size = new NumberOfBytesSetting("neostore.nodestore.db.mapped_memory");

    @Description( "The size to allocate for memory mapping the property value store." )
    @Default("90M")
    public static final GraphDatabaseSetting<Long> nodestore_propertystore_mapped_memory_size = new NumberOfBytesSetting("neostore.propertystore.db.mapped_memory");

    @Description( "The size to allocate for memory mapping the store for property key indexes." )
    @Default("1M")
    public static final GraphDatabaseSetting<Long> nodestore_propertystore_index_mapped_memory_size = new NumberOfBytesSetting("neostore.propertystore.db.index.mapped_memory");

    @Description( "The size to allocate for memory mapping the store for property key strings." )
    @Default("1M")
    public static final GraphDatabaseSetting<Long> nodestore_propertystore_index_keys_mapped_memory_size = new NumberOfBytesSetting("neostore.propertystore.db.index.keys.mapped_memory");

    @Description( "The size to allocate for memory mapping the string property store." )
    @Default("130M")
    public static final GraphDatabaseSetting<Long> strings_mapped_memory_size = new NumberOfBytesSetting("neostore.propertystore.db.strings.mapped_memory");

    @Description( "The size to allocate for memory mapping the array property store." )
    @Default("130M")
    public static final GraphDatabaseSetting<Long> arrays_mapped_memory_size = new NumberOfBytesSetting("neostore.propertystore.db.arrays.mapped_memory");

    @Description( "The size to allocate for memory mapping the relationship store." )
    @Default("100M")
    public static final GraphDatabaseSetting<Long> relationshipstore_mapped_memory_size = new NumberOfBytesSetting("neostore.relationshipstore.db.mapped_memory");

    // Deprecated memory settings (these use String rather than NumberOfBytes)

    @Description( "The size to allocate for memory mapping the node store." )
    @Default("20M")
    @Deprecated
    public static final StringSetting nodestore_mapped_memory = new StringSetting("neostore.nodestore.db.mapped_memory", GraphDatabaseSetting.ANY, "Memory mapping setting must be a valid size.");

    @Description( "The size to allocate for memory mapping the property value store." )
    @Default("90M")
    @Deprecated
    public static final StringSetting nodestore_propertystore_mapped_memory = new StringSetting("neostore.propertystore.db.mapped_memory", GraphDatabaseSetting.ANY, "Memory mapping setting must be a valid size.");

    @Description( "The size to allocate for memory mapping the store for property key indexes." )
    @Default("1M")
    @Deprecated
    public static final StringSetting nodestore_propertystore_index_mapped_memory = new StringSetting("neostore.propertystore.db.index.mapped_memory", GraphDatabaseSetting.ANY, "Memory mapping setting must be a valid size.");

    @Description( "The size to allocate for memory mapping the store for property key strings." )
    @Default("1M")
    @Deprecated
    public static final StringSetting nodestore_propertystore_index_keys_mapped_memory = new StringSetting("neostore.propertystore.db.index.keys.mapped_memory", GraphDatabaseSetting.ANY, "Memory mapping setting must be a valid size.");

    @Description( "The size to allocate for memory mapping the string property store." )
    @Default("130M")
    @Deprecated
    public static final StringSetting strings_mapped_memory = new StringSetting("neostore.propertystore.db.strings.mapped_memory", GraphDatabaseSetting.ANY, "Memory mapping setting must be a valid size.");

    @Description( "The size to allocate for memory mapping the array property store." )
    @Default("130M")
    @Deprecated
    public static final StringSetting arrays_mapped_memory = new StringSetting("neostore.propertystore.db.arrays.mapped_memory", GraphDatabaseSetting.ANY, "Memory mapping setting must be a valid size.");

    @Description( "The size to allocate for memory mapping the relationship store." )
    @Default("100M")
    @Deprecated
    public static final StringSetting relationshipstore_mapped_memory = new StringSetting("neostore.relationshipstore.db.mapped_memory", GraphDatabaseSetting.ANY, "Memory mapping setting must be a valid size.");



    @Default("100")
    public static final IntegerSetting relationship_grab_size = new IntegerSetting( "relationship_grab_size", "Must be a number" );

    @Description( "Whether to grab locks on files or not." )
    @Default(TRUE)
    public static final BooleanSetting grab_file_lock = new BooleanSetting( "grab_file_lock" );

    @Description( "Specifies the block size for storing strings. This parameter is only honored when the store is created, otherwise it is ignored. "+
                  "Note that each character in a string occupies two bytes, meaning that a block size of 120 (the default size) will hold a 60 character "+
                  "long string before overflowing into a second block. Also note that each block carries an overhead of 8 bytes. "+
                  "This means that if the block size is 120, the size of the stored records will be 128 bytes." )
    @Default("120")
    public static final IntegerSetting string_block_size = new IntegerSetting( "string_block_size", "Must be a number", 1, null );

    @Description( "Specifies the block size for storing arrays. This parameter is only honored when the store is created, otherwise it is ignored. "+
                  "The default block size is 120 bytes, and the overhead of each block is the same as for string blocks, i.e., 8 bytes." )
    @Default("120")
    public static final IntegerSetting array_block_size = new IntegerSetting( "array_block_size", "Must be a number", 1, null );

    @Description( "Mark this database as a backup slave." )
    @Default( FALSE )
    public static final BooleanSetting backup_slave = new BooleanSetting( "backup_slave" );

    @Description("An identifier that uniquely identifies this graph database instance within this JVM. " +
                         "Defaults to an auto-generated number depending on how many instance are started in this JVM.")
    @Default("")
    public static final GraphDatabaseSetting<String> forced_kernel_id = new StringSetting( "forced_kernel_id", "[a-zA-Z0-9]*",
                                            "Invalid kernel identifier." );
    
    // TODO: This should be in enterprise, but we currently have code depending on this in community
    @Default( FALSE)
    public static final GraphDatabaseSetting.BooleanSetting online_backup_enabled = new GraphDatabaseSetting.BooleanSetting( "online_backup_enabled" );

    // TODO: This should be in enterprise, but we currently have code depending on this in community
    @Default("6362")
    public static final GraphDatabaseSetting.PortSetting online_backup_port = new GraphDatabaseSetting.PortSetting( "online_backup_port" );

    @Default( FALSE )
    public static final BooleanSetting execution_guard_enabled = new BooleanSetting( "execution_guard_enabled" );

    @Description( "Amount of time in ms the GC monitor thread will wait before taking another measurement." )
    @Default( "100ms" )
    public static final GraphDatabaseSetting<Long> gc_monitor_interval = MonitorGc.Configuration.gc_monitor_wait_time;

    @Description( "The amount of time in ms the monitor thread has to be blocked before logging a message it was blocked." )
    @Default( "200ms" )
    public static final GraphDatabaseSetting<Long> gc_monitor_block_threshold = MonitorGc.Configuration.gc_monitor_threshold;

    // Deprecated GC monitor settings (old type)

    @Description( "Amount of time in ms the GC monitor thread will wait before taking another measurement." )
    @Default( "100ms" )
    @Deprecated
    public static StringSetting gc_monitor_wait_time = new StringSetting( "gc_monitor_wait_time", GraphDatabaseSetting.ANY, "Must be non-empty." );

    @Description( "The amount of time in ms the monitor thread has to be blocked before logging a message it was blocked." )
    @Default( "200ms" )
    @Deprecated
    public static StringSetting gc_monitor_threshold = new StringSetting( "gc_monitor_threshold", GraphDatabaseSetting.ANY, "Must be non-empty." );

    // Old GCR size settings, using string values

    /**
     * Use {@link #gcr_node_cache_size} instead.
     */
    @Description( "The amount of memory to use for the node cache (when using the 'gcr' cache)." )
    @Deprecated
    public static final StringSetting node_cache_size = new StringSetting( "node_cache_size", GraphDatabaseSetting.ANY, "Must be valid size.");

    /**
     * Use {@link #gcr_relationship_cache_size} instead.
     */
    @Description( "The amount of memory to use for the relationship cache (when using the 'gcr' cache)." )
    @Deprecated
    public static final StringSetting relationship_cache_size = new StringSetting( "relationship_cache_size", GraphDatabaseSetting.ANY, "Must be valid size.");

    @Description( "The fraction of the heap (1%-10%) to use for the base array in the node cache (when using the 'gcr' cache)." )
    @Default( "1.0" )
    @Deprecated
    public static final FloatSetting node_cache_array_fraction = new FloatSetting( "node_cache_array_fraction", "Must be a valid fraction", 1.0f, 10.0f);

    @Description( "The fraction of the heap (1%-10%) to use for the base array in the relationship cache (when using the 'gcr' cache)." )
    @Default( "1.0" )
    @Deprecated
    public static final FloatSetting relationship_cache_array_fraction = new FloatSetting( "relationship_cache_array_fraction", "Must be a valid fraction", 1.0f, 10.0f);

    /**
     * Please use {@link #gcr_cache_log_interval} instead.
     */
    @Description( "The minimal time that must pass in between logging statistics from the cache (when using the 'gcr' cache)." )
    @Default( "60s" )
    @Deprecated
    public static final StringSetting gcr_cache_min_log_interval = new StringSetting( "gcr_cache_min_log_interval", GraphDatabaseSetting.DURATION, "Must be a valid duration.");

    /**
     * Configuration key for enabling the UDC extension. Set to "false"
     * to disable; any other value is considered false.
     */
    @Default( TRUE)
    public static final GraphDatabaseSetting<Boolean> udc_enabled = new GraphDatabaseSetting.BooleanSetting("neo4j.ext.udc.enabled");

    /**
     * Configuration key for the first delay, expressed
     * in milliseconds.
     */
    @Default( ""+10 * 1000 * 60 )
    public static final GraphDatabaseSetting<Integer> first_delay = new GraphDatabaseSetting.IntegerSetting("neo4j.ext.udc.first_delay", "Must be nr of milliseconds to delay", 1, null);

    /**
     * Configuration key for the interval for regular updates,
     * expressed in milliseconds.
     */
    @Default(""+1000 * 60 * 60 * 24)
    public static final GraphDatabaseSetting<Integer> interval = new GraphDatabaseSetting.IntegerSetting("neo4j.ext.udc.interval", "Must be nr of milliseconds of the interval for checking", 1, null);

    /**
     * The host address to which UDC updates will be sent.
     * Should be of the form hostname[:port].
     */
    @Default( "udc.neo4j.org" )
    public static final GraphDatabaseSetting<String> udc_host = new GraphDatabaseSetting.StringSetting(  "neo4j.ext.udc.host", ANY, "Must be a valid hostname");

    /**
     * Configuration key for overriding the source parameter in UDC
     */
    public static final GraphDatabaseSetting<String> udc_source = new GraphDatabaseSetting.StringSetting("neo4j.ext.udc.source", ANY, "Must be a valid source");

    /**
     * Unique registration id
     */
    @Default( "unreg" )
    public static final GraphDatabaseSetting<String> udc_registration_key = new GraphDatabaseSetting.StringSetting( "neo4j.ext.udc.reg", ANY, "Must be a valid registration id" );


    // Specialized settings
    @Deprecated // To be made internal
    public static class CacheTypeSetting
        extends OptionsSetting
    {
        @Description("Use weak reference cache.")
        public static final String weak = "weak";

        @Description("Provides optimal utilization of the available memory. Suitable for high performance traversal. \n"+
                     "May run into GC issues under high load if the frequently accessed parts of the graph does not fit in the cache." )
        public static final String soft = "soft";

        @Description("Don't use caching.")
        public static final String none = "none";

        @Description("Use strong references.")
        public static final String strong = "strong";

        @Description("GC resistant cache. Gets assigned a configurable amount of space in the JVM heap \n" +
                "and will evict objects whenever it grows bigger than that, instead of relying on GC for eviction. \n" +
                "It has got the fastest insert/lookup times and should be optimal for most use cases. \n" +
                "This is the default cache setting." )
        public static final String gcr = "gcr";

        public CacheTypeSetting()
        {
            super( "cache_type", availableCaches() );
        }

        private static String[] availableCaches()
        {
            try
            {
                GraphDatabaseSettings.class.getClassLoader().loadClass( "org.neo4j.kernel.impl.cache.GCResistantCacheProvider" );
                return new String[]{gcr,soft,weak,strong,none};
            } catch( ClassNotFoundException e )
            {
                return new String[]{soft,weak,strong,none};
            }
        }
    }

    @Deprecated // To be made internal
    public static class CypherParserSetting
        extends OptionsSetting
    {
        @Description( "Cypher v1.7 syntax." )
        public static final String v1_7 = "1.7";

        @Description( "Cypher v1.8 syntax." )
        public static final String v1_8 = "1.8";

        public CypherParserSetting( )
        {
            super( "cypher_parser_version", v1_7, v1_8 );
        }
    }

    @Deprecated // To be made internal
    public static class UseMemoryMappedBuffers
        extends BooleanSetting
        implements DefaultValue
    {
        public UseMemoryMappedBuffers( )
        {
            super( "use_memory_mapped_buffers" );
        }

        @Override
        public String getDefaultValue()
        {
            // if on windows, default no memory mapping
            if ( osIsWindows() )
            {
                return FALSE;
            }
            else
            {
                // If not on win, default use memory mapping
                return TRUE;
            }
        }
    }
}