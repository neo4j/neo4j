/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.util.HashMap;
import java.util.Map;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.cache.AdaptiveCacheManager;
import org.neo4j.kernel.impl.core.GraphDbModule;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.LastCommittedTxIdSetter;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.core.RelationshipTypeCreator;
import org.neo4j.kernel.impl.core.RelationshipTypeHolder;
import org.neo4j.kernel.impl.core.TxEventSyncHookFactory;
import org.neo4j.kernel.impl.management.Description;
import org.neo4j.kernel.impl.persistence.IdGenerator;
import org.neo4j.kernel.impl.persistence.IdGeneratorModule;
import org.neo4j.kernel.impl.persistence.PersistenceModule;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxModule;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;

/**
 * A non-standard configuration object.
 */
public class Config
{
    static final String NIO_NEO_DB_CLASS = "org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource";
    public static final String DEFAULT_DATA_SOURCE_NAME = "nioneodb";

    static final String LUCENE_DS_CLASS = "org.neo4j.index.lucene.LuceneDataSource";
    static final String LUCENE_FULLTEXT_DS_CLASS = "org.neo4j.index.lucene.LuceneFulltextDataSource";

    @Description( "Tell Neo4j to use memory mapped buffers for accessing the native storage layer" )
    public static final String USE_MEMORY_MAPPED_BUFFERS = "use_memory_mapped_buffers";
    @Description( "Print out the effective Neo4j configuration after startup" )
    public static final String DUMP_CONFIGURATION = "dump_configuration";
    @Description( "Make Neo4j keep the logical transaction logs for being able to backup the database" )
    public static final String KEEP_LOGICAL_LOGS = "keep_logical_logs";
    @Description( "Enable a remote shell server which shell clients can log in to" )
    public static final String ENABLE_REMOTE_SHELL = "enable_remote_shell";

    public static final String BACKUP_SLAVE = "backup_slave";

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
    private final IdGeneratorModule idGeneratorModule;
    private final GraphDbModule graphDbModule;
    private final String storeDir;
    private IndexStore indexStore;
    private final Map<Object, Object> params;
    private final Map inputParams;
    private final TxEventSyncHookFactory syncHookFactory;
    private final RelationshipTypeCreator relTypeCreator;

    private final boolean readOnly;
    private final boolean backupSlave;
    private final IdGeneratorFactory idGeneratorFactory;
    private final TxIdGenerator txIdGenerator;

    Config( GraphDatabaseService graphDb, String storeDir,
            Map<String, String> inputParams, KernelPanicEventGenerator kpe,
            TxModule txModule, LockManager lockManager,
            LockReleaser lockReleaser, IdGeneratorFactory idGeneratorFactory,
            TxEventSyncHookFactory txSyncHookFactory,
            RelationshipTypeCreator relTypeCreator, TxIdGenerator txIdGenerator,
            LastCommittedTxIdSetter lastCommittedTxIdSetter )
    {
        this.storeDir = storeDir;
        this.inputParams = inputParams;
        this.idGeneratorFactory = idGeneratorFactory;
        this.relTypeCreator = relTypeCreator;
        this.txIdGenerator = txIdGenerator;
        this.params = getDefaultParams();
        this.txModule = txModule;
        this.lockManager = lockManager;
        this.lockReleaser = lockReleaser;
        this.idGeneratorModule = new IdGeneratorModule( new IdGenerator() );
        this.readOnly = Boolean.parseBoolean( (String) params.get( READ_ONLY ) );
        this.backupSlave = Boolean.parseBoolean( (String) params.get( BACKUP_SLAVE ) );
        this.syncHookFactory = txSyncHookFactory;
        this.persistenceModule = new PersistenceModule();
        this.cacheManager = new AdaptiveCacheManager();
        graphDbModule = new GraphDbModule( graphDb, cacheManager, lockManager,
                txModule.getTxManager(), idGeneratorModule.getIdGenerator(),
                readOnly );
        indexStore = new IndexStore( storeDir );
        params.put( IndexStore.class, indexStore );

        params.put( IdGeneratorFactory.class, idGeneratorFactory );
        params.put( TxIdGenerator.class, txIdGenerator );
        params.put( TransactionManager.class, txModule.getTxManager() );
        params.put( LastCommittedTxIdSetter.class, lastCommittedTxIdSetter );
        params.put( GraphDbModule.class, graphDbModule );
    }

    private static Map<Object, Object> getDefaultParams()
    {
        Map<Object, Object> params = new HashMap<Object, Object>();
        params.put( "neostore.nodestore.db.mapped_memory", "20M" );
        params.put( "neostore.propertystore.db.mapped_memory", "90M" );
        params.put( "neostore.propertystore.db.index.mapped_memory", "1M" );
        params.put( "neostore.propertystore.db.index.keys.mapped_memory", "1M" );
        params.put( "neostore.propertystore.db.strings.mapped_memory", "130M" );
        params.put( "neostore.propertystore.db.arrays.mapped_memory", "130M" );
        params.put( "neostore.relationshipstore.db.mapped_memory", "100M" );
        // if on windows, default no memory mapping
        String nameOs = System.getProperty( "os.name" );
        if ( nameOs.startsWith( "Windows" ) )
        {
            params.put( Config.USE_MEMORY_MAPPED_BUFFERS, "false" );
        }
        return params;
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

    public IdGeneratorModule getIdGeneratorModule()
    {
        return idGeneratorModule;
    }

    public LockManager getLockManager()
    {
        return lockManager;
    }
    
    public IndexStore getIndexStore()
    {
        return indexStore;
    }

    public LockReleaser getLockReleaser()
    {
        return lockReleaser;
    }

    public Map<Object, Object> getParams()
    {
        return this.params;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    boolean isBackupSlave()
    {
        return backupSlave;
    }

    Map<Object, Object> getInputParams()
    {
        return inputParams;
    }

    TxEventSyncHookFactory getSyncHookFactory()
    {
        return syncHookFactory;
    }
    
    public RelationshipTypeCreator getRelationshipTypeCreator()
    {
        return relTypeCreator;
    }
    
    public IdGeneratorFactory getIdGeneratorFactory()
    {
        return idGeneratorFactory;
    }
    
    public RelationshipTypeHolder getRelationshipTypeHolder()
    {
        return graphDbModule.getNodeManager().getRelationshipTypeHolder();
    }

    public static void dumpConfiguration( Map<?, ?> config )
    {
        for ( Object key : config.keySet() )
        {
            if ( key instanceof String )
            {
                Object value = config.get( key );
                if ( value instanceof String )
                {
                    System.out.println( key + "=" + value );
                }
            }
        }
    }

    public static Object getFromConfig( Map<?, ?> config, Object key,
            Object defaultValue )
    {
        Object result = config != null ? config.get( key ) : defaultValue;
        return result != null ? result : defaultValue;
    }
}
