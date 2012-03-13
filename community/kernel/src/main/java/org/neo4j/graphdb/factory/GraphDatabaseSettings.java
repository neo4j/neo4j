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

import static org.neo4j.graphdb.factory.GraphDatabaseSetting.*;

/**
 * Settings for the Community edition of Neo4j. Use this with GraphDatabaseBuilder.
 */
public abstract class GraphDatabaseSettings
{
    @Default( FALSE)
    public static final BooleanSetting read_only = new BooleanSetting("read_only");

    @Default( CacheTypeSetting.soft)
    public static final CacheTypeSetting cache_type = new CacheTypeSetting();

    @Default( TRUE)
    public static final BooleanSetting load_kernel_extensions = new BooleanSetting("load_kernel_extensions");

    @Default(FALSE)
    public static final BooleanSetting dump_configuration = new BooleanSetting( "dump_configuration" );
    
    public static final StringSetting tx_manager_impl = new StringSetting("tx_manager_impl",".*","Must be class name of TransactionManager implementation");

    @Default( FALSE )
    public static BooleanSetting allow_store_upgrade = new BooleanSetting( "allow_store_upgrade" );
    
    @Default(FALSE)
    public static final BooleanSetting intercept_deserialized_transactions = new BooleanSetting( "intercept_deserialized_transactions" );

    // NodeManager settings
    @Default(FALSE)
    public static final BooleanSetting use_adaptive_cache = new BooleanSetting( "use_adaptive_cache" );

    @Default("0.77")
    public static final FloatSetting adaptive_cache_heap_ratio = new FloatSetting("adaptive_cache_heap_ratio", "Must be a number",0.1f,0.95f);

    @Default("0")
    public static final IntegerSetting min_node_cache_size = new IntegerSetting("min_node_cache_size", "Must be an integer");

    @Default("0")
    public static final IntegerSetting min_relationship_cache_size = new IntegerSetting("min_node_cache_size", "Must be an integer");

    @Default("1500")
    public static final IntegerSetting max_node_cache_size = new IntegerSetting("max_node_cache_size", "Must be an integer");

    @Default("3500")
    public static final IntegerSetting max_relationship_cache_size = new IntegerSetting("max_node_cache_size", "Must be an integer");

    // Indexing
    @Default(FALSE)
    public static final BooleanSetting node_auto_indexing = new BooleanSetting( "node_auto_indexing" );

    public static final StringSetting node_keys_indexable = new StringSetting("node_keys_indexable",".*","Must be a comma-separated list of keys to be indexed");

    @Default(FALSE)
    public static final BooleanSetting relationship_auto_indexing = new BooleanSetting( "relationship_auto_indexing" );

    public static final StringSetting relationship_keys_indexable = new StringSetting("relationship_keys_indexable",".*","Must be a comma-separated list of keys to be indexed");

    // Adaptive cache settings
    @Default("3000")
    public static final IntegerSetting adaptive_cache_worker_sleep_time = new IntegerSetting("adaptive_cache_worker_sleep_time", "Must be nr of milliseconds to sleep between checks");
    
    @Default("1.15")
    public static final FloatSetting adaptive_cache_manager_decrease_ratio = new FloatSetting("adaptive_cache_manager_decrease_ratio", "Must be a number",1.0f,null);

    @Default("1.1")
    public static final FloatSetting adaptive_cache_manager_increase_ratio = new FloatSetting("adaptive_cache_manager_increase_ratio", "Must be a number",1.0f,null);
    
    // Lucene settings
    @Default(Integer.MAX_VALUE+"")
    public static IntegerSetting lucene_searcher_cache_size = new IntegerSetting( "lucene_searcher_cache_size", "Must be a number", 1, null );

    @Default(Integer.MAX_VALUE+"")
    public static IntegerSetting lucene_writer_cache_size = new IntegerSetting( "lucene_writer_cache_size", "Must be a number", 1, null );

    // NeoStore settings
    @Default(FALSE)
    public static final BooleanSetting intercept_committing_transactions = new BooleanSetting( "intercept_committing_transactions" );
    
    public static final StringSetting keep_logical_logs = new StringSetting( "keep_logical_logs", ".*","No value=don't store,true=store all logs,comma separated list=store logs from listed sources" );

    @Default(FALSE)
    public static final BooleanSetting online_backup_enabled = new BooleanSetting( "online_backup_enabled" );

    @Default(TRUE)
    public static final BooleanSetting rebuild_idgenerators_fast = new BooleanSetting( "rebuild_idgenerators_fast" );

    // NeoStore memory settings
    public static final UseMemoryMappedBuffers use_memory_mapped_buffers = new UseMemoryMappedBuffers();
    
    @Default("20M")
    public static final StringSetting nodestore_mapped_memory = new StringSetting("neostore.nodestore.db.mapped_memory","\\d+[MG]","Invalid value {0}, must be e.g. 20M");

    @Default("90M")
    public static final StringSetting nodestore_propertystore_mapped_memory = new StringSetting("neostore.propertystore.db.mapped_memory","\\d+[MG]","Invalid value {0}, must be e.g. 20M");

    @Default("1M")
    public static final StringSetting nodestore_propertystore_index_mapped_memory = new StringSetting("neostore.propertystore.db.index.mapped_memory","\\d+[MG]","Invalid value {0}, must be e.g. 20M");

    @Default("1M")
    public static final StringSetting nodestore_propertystore_index_keys_mapped_memory = new StringSetting("neostore.propertystore.db.index.keys.mapped_memory","\\d+[MG]","Invalid value {0}, must be e.g. 20M");

    @Default("130M")
    public static final StringSetting strings_mapped_memory = new StringSetting("neostore.propertystore.db.strings.mapped_memory","\\d+[MG]","Invalid value {0}, must be e.g. 20M");

    @Default("130M")
    public static final StringSetting arrays_mapped_memory = new StringSetting("neostore.propertystore.db.arrays.mapped_memory","\\d+[MG]","Invalid value {0}, must be e.g. 20M");

    @Default("100M")
    public static final StringSetting relationshipstore_mapped_memory = new StringSetting("neostore.relationshipstore.db.mapped_memory","\\d+[MG]","Invalid value {0}, must be e.g. 20M");

    @Default("100")
    public static final IntegerSetting relationship_grab_size = new IntegerSetting( "relationship_grab_size", "Must be a number" );

    @Default(TRUE)
    public static final BooleanSetting grab_file_lock = new BooleanSetting( "grab_file_lock" );

    @Default( FALSE )
    public static final BooleanSetting backup_slave = new BooleanSetting( "backup_slave" );

    @Default("120")
    public static final IntegerSetting string_block_size = new IntegerSetting( "string_block_size", "Must be a number", 1, null );

    @Default("120")
    public static final IntegerSetting array_block_size = new IntegerSetting( "array_block_size", "Must be a number", 1, null );
}
