/*
 * Copyright (c) 2002-2010 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.cache.AdaptiveCacheManager;
import org.neo4j.kernel.impl.core.GraphDbModule;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.management.Description;
import org.neo4j.kernel.impl.persistence.IdGeneratorModule;
import org.neo4j.kernel.impl.persistence.PersistenceModule;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxModule;

/**
 * A non-standard configuration object.
 */
public class Config
{
    static final String NIO_NEO_DB_CLASS =
        "org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource";
    public static final String DEFAULT_DATA_SOURCE_NAME = "nioneodb";

    static final String LUCENE_DS_CLASS =
        "org.neo4j.index.lucene.LuceneDataSource";
    static final String LUCENE_FULLTEXT_DS_CLASS =
        "org.neo4j.index.lucene.LuceneFulltextDataSource";

    @Description( "Tell Neo4j to use memory mapped buffers for accessing the native storage layer" )
    public static final String USE_MEMORY_MAPPED_BUFFERS = "use_memory_mapped_buffers";
    @Description( "Print out the effective Neo4j configuration after startup" )
    public static final String DUMP_CONFIGURATION = "dump_configuration";
    @Description( "Make Neo4j keep the logical transaction logs for being able to backup the database" )
    public static final String KEEP_LOGICAL_LOGS = "keep_logical_logs";
    @Description( "Only allow read operations from this Neo4j instance" )
    public static final String READ_ONLY = "read_only";
    @Description( "Relative path for where the Neo4j storage directory is located" )
    public static final String STORAGE_DIRECTORY = "store_dir";
    @Description( "Use a quick approach for rebuilding the ID generators. "
                  + "This give quicker recovery time, but will limit the ability to reuse the space of deleted entities." )
    public static final String REBUILD_IDGENERATORS_FAST = "rebuild_idgenerators_fast";
    @Description( "The size to allocate for memory mapping the node store" )
    public static final String NODE_STORE_MMAP_SIZE = "neostore.nodestore.db.mapped_memory";
    @Description( "The size to allocate for memory mapping the array property store" )
    public static final String ARRAY_PROPERTY_STORE_MMAP_SIZE = "neostore.propertystore.db.arrays.mapped_memory";
    @Description( "The size to allocate for memory mapping the store for property key strings" )
    public static final String PROPERTY_INDEX_KEY_STORE_MMAP_SIZE = "neostore.propertystore.db.index.keys.mapped_memory";
    @Description( "The size to allocate for memory mapping the store for property key indexes" )
    public static final String PROPERTY_INDEX_STORE_MMAP_SIZE = "neostore.propertystore.db.index.mapped_memory";
    @Description( "The size to allocate for memory mapping the property value store" )
    public static final String PROPERTY_STORE_MMAP_SIZE = "neostore.propertystore.db.mapped_memory";
    @Description( "The size to allocate for memory mapping the string property store" )
    public static final String STRING_PROPERTY_STORE_MMAP_SIZE = "neostore.propertystore.db.strings.mapped_memory";
    @Description( "The size to allocate for memory mapping the relationship store" )
    public static final String RELATIONSHIP_STORE_MMAP_SIZE = "neostore.relationshipstore.db.mapped_memory";
    @Description( "Relative path for where the Neo4j logical log is located" )
    public static final String LOGICAL_LOG = "logical_log";
    @Description( "Relative path for where the Neo4j storage information file is located" )
    public static final String NEO_STORE = "neo_store";
    @Description( "The type of cache to use for nodes and relationships, one of [weak, soft, none]" )
    public static final String CACHE_TYPE = "cache_type";

    private AdaptiveCacheManager cacheManager;
    private TxModule txModule;
    private LockManager lockManager;
    private LockReleaser lockReleaser;
    private PersistenceModule persistenceModule;
    private boolean create = false;
    private String persistenceSourceName;
    private IdGeneratorModule idGeneratorModule;
    private GraphDbModule graphDbModule;
    private String storeDir;
    private final Map<Object, Object> params;

    private final boolean readOnly;
    private final boolean backupSlave;

    Config( GraphDatabaseService graphDb, String storeDir, Map<Object, Object> params,
            KernelPanicEventGenerator kpe )
    {
        this.storeDir = storeDir;
        this.params = params;
        String readOnlyStr = (String) params.get( READ_ONLY );
        if ( readOnlyStr != null && readOnlyStr.toLowerCase().equals( "true" ) )
        {
            readOnly = true;
        }
        else
        {
            readOnly = false;
        }
        String backupSlaveStr = (String) params.get( "backup_slave" );
        if ( backupSlaveStr != null &&
                backupSlaveStr.toLowerCase().equals( "true" ) )
        {
            backupSlave = true;
        }
        else
        {
            backupSlave = false;
        }
        params.put( "read_only", readOnly );
        cacheManager = new AdaptiveCacheManager();
        if ( !readOnly )
        {
            txModule = new TxModule( this.storeDir, kpe );
        }
        else
        {
            txModule = new TxModule( true, kpe );
        }
        lockManager = new LockManager( txModule.getTxManager() );
        lockReleaser = new LockReleaser( lockManager, txModule.getTxManager() );
        persistenceModule = new PersistenceModule();
        idGeneratorModule = new IdGeneratorModule();
        graphDbModule = new GraphDbModule( graphDb, cacheManager, lockManager,
                txModule.getTxManager(), idGeneratorModule.getIdGenerator(),
                readOnly );
    }

    void setPersistenceSource( String name, boolean create )
    {
        persistenceSourceName = name;
        this.create = create;
    }

    String getPersistenceSource()
    {
        return persistenceSourceName;
    }

    boolean getCreatePersistenceSource()
    {
        return create;
    }

    public TxModule getTxModule()
    {
        return txModule;
    }

    public GraphDbModule getGraphDbModule()
    {
        return graphDbModule;
    }

    public PersistenceModule getPersistenceModule()
    {
        return persistenceModule;
    }

    IdGeneratorModule getIdGeneratorModule()
    {
        return idGeneratorModule;
    }

    public LockManager getLockManager()
    {
        return lockManager;
    }

    public LockReleaser getLockReleaser()
    {
        return lockReleaser;
    }

    public Map<Object, Object> getParams()
    {
        return this.params;
    }

    boolean isReadOnly()
    {
        return readOnly;
    }

    boolean isBackupSlave()
    {
        return backupSlave;
    }
}
