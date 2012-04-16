/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.graphdb.factory.GraphDatabaseSetting.*;

/**
 * Settings for the Community edition of Neo4j. Use this with GraphDatabaseBuilder.
 */
@Description( "Settings for the Community edition of Neo4j" )
public abstract class GraphDatabaseSettings
{
    @Title( "Read only database" )
    @Description("Only allow read operations from this Neo4j instance")
    @Default( FALSE)
    public static final BooleanSetting read_only = new BooleanSetting("read_only");

    @Description("The type of cache to use for nodes and relationships")
    @Default( CacheTypeSetting.soft )
    public static final CacheTypeSetting cache_type = new CacheTypeSetting();

    @Default( TRUE)
    public static final BooleanSetting load_kernel_extensions = new BooleanSetting("load_kernel_extensions");

    @Description( "Print out the effective Neo4j configuration after startup" )
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

    // Cypher version
    @Description( "Enable this to specify a parser other than the default one. 1.5 and 1.6 are available" )
    public static final OptionsSetting cypher_parser_version = new CypherParserSetting();

    // Remote logging
    @Description( "Whether to enable logging to a remote server or not" )
    @Default(FALSE)
    public static GraphDatabaseSetting remote_logging_enabled = new BooleanSetting( "remote_logging_enabled" );

    @Description( "Host for remote logging using LogBack SocketAppender" )
    @Default("127.0.0.1")
    public static final StringSetting remote_logging_host = new StringSetting( "remote_logging_host", ANY, "Must be a valid hostname" );

    @Description( "Port for remote logging using LogBack SocketAppender" )
    @Default("4560")
    public static final PortSetting remote_logging_port = new PortSetting( "remote_logging_port" );

    // Indexing
    @Description( "Controls the auto indexing feature for nodes. Setting to false shuts it down unconditionally, while true enables it for every property, "+
                  "subject to restrictions in the configuration." )
    @Default(FALSE)
    public static final BooleanSetting node_auto_indexing = new BooleanSetting( "node_auto_indexing" );

    @Description( "A list of property names (comma separated) that will be indexed by default. This applies to Nodes only." )
    public static final StringSetting node_keys_indexable = new StringSetting("node_keys_indexable",ANY,"Must be a comma-separated list of keys to be indexed");

    @Description( "Controls the auto indexing feature for relationships. Setting to false shuts it down unconditionally, while true enables it for every property, "+
                  "subject to restrictions in the configuration." )
    @Default(FALSE)
    public static final BooleanSetting relationship_auto_indexing = new BooleanSetting( "relationship_auto_indexing" );

    @Description( " A list of property names (comma separated) that will be indexed by default. This applies to Relationships only." )
    public static final StringSetting relationship_keys_indexable = new StringSetting("relationship_keys_indexable",ANY,"Must be a comma-separated list of keys to be indexed");

    // Lucene settings
    @Description( "Integer value that sets the maximum number of open lucene index searchers" )
    @Default(Integer.MAX_VALUE+"")
    public static IntegerSetting lucene_searcher_cache_size = new IntegerSetting( "lucene_searcher_cache_size", "Must be a number", 1, null );

    @Description( "Integer value that sets the maximum number of open lucene index writers." )
    @Default(Integer.MAX_VALUE+"")
    public static IntegerSetting lucene_writer_cache_size = new IntegerSetting( "lucene_writer_cache_size", "Must be a number", 1, null );

    // NeoStore settings
    @Description( "Determines whether any TransactionInterceptors loaded will intercept prepared transactions before they reach the logical log" )
    @Default(FALSE)
    public static final BooleanSetting intercept_committing_transactions = new BooleanSetting( "intercept_committing_transactions" );

    @Description( "Make Neo4j keep the logical transaction logs for being able to backup the database" )
    public static final StringSetting keep_logical_logs = new StringSetting( "keep_logical_logs", ANY, "No value=don't store,true=store all logs,comma separated list=store logs from listed sources" );

    @Description( "Use a quick approach for rebuilding the ID generators. This give quicker recovery time, but will limit the ability to reuse the space of deleted entities." )
    @Default(TRUE)
    public static final BooleanSetting rebuild_idgenerators_fast = new BooleanSetting( "rebuild_idgenerators_fast" );

    // NeoStore memory settings
    @Description( "Tell Neo4j to use memory mapped buffers for accessing the native storage layer" )
    public static final UseMemoryMappedBuffers use_memory_mapped_buffers = new UseMemoryMappedBuffers();

    @Description( "The size to allocate for memory mapping the node store" )
    @Default("20M")
    public static final StringSetting nodestore_mapped_memory = new StringSetting("neostore.nodestore.db.mapped_memory",SIZE,"Invalid value %s, must be e.g. 20M");

    @Description( "The size to allocate for memory mapping the property value store" )
    @Default("90M")
    public static final StringSetting nodestore_propertystore_mapped_memory = new StringSetting("neostore.propertystore.db.mapped_memory",SIZE,"Invalid value %s, must be e.g. 20M");

    @Description( "The size to allocate for memory mapping the store for property key indexes" )
    @Default("1M")
    public static final StringSetting nodestore_propertystore_index_mapped_memory = new StringSetting("neostore.propertystore.db.index.mapped_memory",SIZE,"Invalid value %s, must be e.g. 20M");

    @Description( "The size to allocate for memory mapping the store for property key strings" )
    @Default("1M")
    public static final StringSetting nodestore_propertystore_index_keys_mapped_memory = new StringSetting("neostore.propertystore.db.index.keys.mapped_memory",SIZE,"Invalid value %s, must be e.g. 20M");

    @Description( "The size to allocate for memory mapping the string property store" )
    @Default("130M")
    public static final StringSetting strings_mapped_memory = new StringSetting("neostore.propertystore.db.strings.mapped_memory",SIZE,"Invalid value %s, must be e.g. 20M");

    @Description( "The size to allocate for memory mapping the array property store" )
    @Default("130M")
    public static final StringSetting arrays_mapped_memory = new StringSetting("neostore.propertystore.db.arrays.mapped_memory",SIZE,"Invalid value %s, must be e.g. 20M");

    @Description( "The size to allocate for memory mapping the relationship store" )
    @Default("100M")
    public static final StringSetting relationshipstore_mapped_memory = new StringSetting("neostore.relationshipstore.db.mapped_memory",SIZE,"Invalid value %s, must be e.g. 20M");

    @Default("100")
    public static final IntegerSetting relationship_grab_size = new IntegerSetting( "relationship_grab_size", "Must be a number" );

    @Description( "Whether to grab locks on files or not" )
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

    @Description( "Mark this database as a backup slave" )
    @Default( FALSE )
    public static final BooleanSetting backup_slave = new BooleanSetting( "backup_slave" );

    // GCR settings
    @Description( "The amount of memory to use for the node cache (when using the 'gcr' cache)" )
    public static final StringSetting node_cache_size = new StringSetting( "node_cache_size",SIZE,"Must be a valid size" );

    @Description( "The amount of memory to use for the relationship cache (when using the 'gcr' cache)" )
    public static final StringSetting relationship_cache_size = new StringSetting( "relationship_cache_size",SIZE,"Must be a valid size" );

    @Description( "The fraction of the heap (1%-10%) to use for the base array in the node cache (when using the 'gcr' cache)" )
    @Default( "1.0" )
    public static final FloatSetting node_cache_array_fraction = new FloatSetting( "node_cache_array_fraction", "Must be a valid fraction", 1.0f, 10.0f);

    @Description( "The fraction of the heap (1%-10%) to use for the base array in the relationship cache (when using the 'gcr' cache)" )
    @Default( "1.0" )
    public static final FloatSetting relationship_cache_array_fraction = new FloatSetting( "relationship_cache_array_fraction", "Must be a valid fraction", 1.0f, 10.0f);

    @Description( "The minimal time that must pass in between logging statistics from the cache (when using the 'gcr' cache)" )
    @Default( "60s" )
    public static final StringSetting gcr_cache_min_log_interval = new StringSetting( "gcr_cache_min_log_interval", DURATION, "Must be a valid interval" );

    @Default( FALSE )
    public static BooleanSetting execution_guard_enabled = new BooleanSetting( "execution_guard_enabled" );

    @Description( "Amount of time in ms the GC monitor thread will wait before taking another measurement." )
    @Default( "100ms" )
    public static StringSetting gc_monitor_wait_time = new StringSetting( "gc_monitor_wait_time", DURATION, "Must be a valid duration" );

    @Description( "The amount of time in ms the monitor thread has to be blocked before logging a message it was blocked." )
    @Default( "200ms" )
    public static StringSetting gc_monitor_threshold = new StringSetting( "gc_monitor_threshold", DURATION, "Must be a valid duration" );

    // Specialized settings
    public static class CacheTypeSetting
        extends OptionsSetting
    {
        @Description("Use weak reference cache")
        public static final String weak = "weak";

        @Description("Provides optimal utilization of the available memory. Suitable for high performance traversal. \n"+
                     "May run into GC issues under high load if the frequently accessed parts of the graph does not fit in the cache." )
        public static final String soft = "soft";

        @Description("Don't use caching")
        public static final String none = "none";

        @Description("Use strong references")
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

    public static class CypherParserSetting
        extends OptionsSetting
    {
        @Description( "Cypher v1.5 syntax" )
        public static final String v1_5 = "1.5";

        @Description( "Cypher v1.6 syntax" )
        public static final String v1_6 = "1.6";

        @Description( "Cypher v1.7 syntax" )
        public static final String v1_7 = "1.7";

        public CypherParserSetting( )
        {
            super( "cypher_parser_version", v1_5, v1_6, v1_7);
        }
    }

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
