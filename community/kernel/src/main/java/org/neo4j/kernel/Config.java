/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel;

import static java.util.regex.Pattern.quote;

import java.util.HashMap;
import java.util.Map;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.cache.AdaptiveCacheManager;
import org.neo4j.kernel.impl.core.GraphDbModule;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.LastCommittedTxIdSetter;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.core.RelationshipTypeCreator;
import org.neo4j.kernel.impl.core.RelationshipTypeHolder;
import org.neo4j.kernel.impl.core.TxEventSyncHookFactory;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.persistence.IdGenerator;
import org.neo4j.kernel.impl.persistence.IdGeneratorModule;
import org.neo4j.kernel.impl.persistence.PersistenceModule;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxHook;
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

    /**
     * Tell Neo4j to use memory mapped buffers for accessing the native storage
     * layer
     */
    @Documented
    public static final String USE_MEMORY_MAPPED_BUFFERS = "use_memory_mapped_buffers";
    /** Print out the effective Neo4j configuration after startup */
    @Documented
    public static final String DUMP_CONFIGURATION = "dump_configuration";
    /**
     * Make Neo4j keep the logical transaction logs for being able to backup the
     * database
     */
    @Documented
    public static final String KEEP_LOGICAL_LOGS = "keep_logical_logs";
    /** Enable a remote shell server which shell clients can log in to */
    @Documented
    public static final String ENABLE_REMOTE_SHELL = "enable_remote_shell";
    /** Enable a support for running online backups */
    @Documented
    public static final String ENABLE_ONLINE_BACKUP = "enable_online_backup";
    /** Mark this database as a backup slave. */
    @Documented
    public static final String BACKUP_SLAVE = "backup_slave";

    /** Only allow read operations from this Neo4j instance. */
    @Documented
    public static final String READ_ONLY = "read_only";
    /** Relative path for where the Neo4j storage directory is located */
    @Documented
    public static final String STORAGE_DIRECTORY = "store_dir";
    /**
     * Use a quick approach for rebuilding the ID generators. This give quicker
     * recovery time, but will limit the ability to reuse the space of deleted
     * entities.
     */
    @Documented
    public static final String REBUILD_IDGENERATORS_FAST = "rebuild_idgenerators_fast";
    /** The size to allocate for memory mapping the node store */
    @Documented
    public static final String NODE_STORE_MMAP_SIZE = "neostore.nodestore.db.mapped_memory";
    /** The size to allocate for memory mapping the array property store */
    @Documented
    public static final String ARRAY_PROPERTY_STORE_MMAP_SIZE = "neostore.propertystore.db.arrays.mapped_memory";
    /**
     * The size to allocate for memory mapping the store for property key
     * strings
     */
    @Documented
    public static final String PROPERTY_INDEX_KEY_STORE_MMAP_SIZE = "neostore.propertystore.db.index.keys.mapped_memory";
    /**
     * The size to allocate for memory mapping the store for property key
     * indexes
     */
    @Documented
    public static final String PROPERTY_INDEX_STORE_MMAP_SIZE = "neostore.propertystore.db.index.mapped_memory";
    /** The size to allocate for memory mapping the property value store */
    @Documented
    public static final String PROPERTY_STORE_MMAP_SIZE = "neostore.propertystore.db.mapped_memory";
    /** The size to allocate for memory mapping the string property store */
    @Documented
    public static final String STRING_PROPERTY_STORE_MMAP_SIZE = "neostore.propertystore.db.strings.mapped_memory";
    /** The size to allocate for memory mapping the relationship store */
    @Documented
    public static final String RELATIONSHIP_STORE_MMAP_SIZE = "neostore.relationshipstore.db.mapped_memory";
    /** Relative path for where the Neo4j logical log is located */
    @Documented
    public static final String LOGICAL_LOG = "logical_log";
    /** Relative path for where the Neo4j storage information file is located */
    @Documented
    public static final String NEO_STORE = "neo_store";

    /**
     * The type of cache to use for nodes and relationships, one of [weak, soft,
     * none]
     */
    @Documented
    public static final String CACHE_TYPE = "cache_type";

    /**
     * The name of the Transaction Manager service to use as defined in the TM
     * service provider constructor, defaults to native.
     */
    @Documented
    public static final String TXMANAGER_IMPLEMENTATION = "tx_manager_impl";

    /**
     * Determines whether any TransactionInterceptors loaded will intercept
     * prepared transactions before they reach the logical log. Defaults to
     * false.
     */
    @Documented
    public static final String INTERCEPT_COMMITTING_TRANSACTIONS = "intercept_committing_transactions";

    /**
     * Determines whether any TransactionInterceptors loaded will intercept
     * externally received transactions (e.g. in HA) before they reach the
     * logical log and are applied to the store. Defaults to false.
     */
    @Documented
    public static final String INTERCEPT_DESERIALIZED_TRANSACTIONS = "intercept_deserialized_transactions";

    /**
     * Boolean (one of true,false) defining whether to allow a store upgrade
     * in case the current version of the database starts against an older store
     * version. Setting this to true does not guarantee successful upgrade, just
     * allows an attempt at it.
     */
    @Documented
    public static final String ALLOW_STORE_UPGRADE = "allow_store_upgrade";
    public static final String STRING_BLOCK_SIZE = "string_block_size";
    public static final String ARRAY_BLOCK_SIZE = "array_block_size";
    /**
     * A list of property names (comma separated) that will be indexed by
     * default.
     * This applies to Nodes only.
     */
    @Documented
    public static final String NODE_KEYS_INDEXABLE = "node_keys_indexable";
    /**
     * A list of property names (comma separated) that will be indexed by
     * default.
     * This applies to Relationships only.
     */
    @Documented
    public static final String RELATIONSHIP_KEYS_INDEXABLE = "relationship_keys_indexable";

    /**
     * Boolean value (one of true, false) that controls the auto indexing
     * feature for nodes. Setting to false shuts it down unconditionally,
     * while true enables it for every property, subject to restrictions
     * in the configuration.
     * The default is false.
     */
    @Documented
    public static final String NODE_AUTO_INDEXING = "node_auto_indexing";

    /**
     * Boolean value (one of true, false) that controls the auto indexing
     * feature for relationships. Setting to false shuts it down
     * unconditionally, while true enables it for every property, subject
     * to restrictions in the configuration.
     * The default is false.
     */
    @Documented
    public static final String RELATIONSHIP_AUTO_INDEXING = "relationship_auto_indexing";

    static final String LOAD_EXTENSIONS = "load_kernel_extensions";

    private final AdaptiveCacheManager cacheManager;
    private final TxModule txModule;
    private final LockManager lockManager;
    private final LockReleaser lockReleaser;
    private final PersistenceModule persistenceModule;
    private boolean create = false;
    private String persistenceSourceName;
    private final IdGeneratorModule idGeneratorModule;
    private final GraphDbModule graphDbModule;
    private final String storeDir;
    private final IndexStore indexStore;
    private final Map<Object, Object> params;
    private final Map inputParams;
    private final TxEventSyncHookFactory syncHookFactory;
    private final RelationshipTypeCreator relTypeCreator;

    private final boolean readOnly;
    private final boolean backupSlave;
    private final IdGeneratorFactory idGeneratorFactory;
    private final TxIdGenerator txIdGenerator;

    Config( GraphDatabaseService graphDb, String storeDir, StoreId storeId,
            Map<String, String> inputParams, KernelPanicEventGenerator kpe,
            TxModule txModule, LockManager lockManager,
            LockReleaser lockReleaser, IdGeneratorFactory idGeneratorFactory,
            TxEventSyncHookFactory txSyncHookFactory,
            RelationshipTypeCreator relTypeCreator, TxIdGenerator txIdGenerator,
            LastCommittedTxIdSetter lastCommittedTxIdSetter,
            FileSystemAbstraction fileSystem )
    {
        this.storeDir = storeDir;
        this.inputParams = inputParams;
        // Get the default params and override with the user supplied values
        this.params = getDefaultParams();
        this.params.putAll( inputParams );

        this.idGeneratorFactory = idGeneratorFactory;
        this.relTypeCreator = relTypeCreator;
        this.txIdGenerator = txIdGenerator;
        this.txModule = txModule;
        this.lockManager = lockManager;
        this.lockReleaser = lockReleaser;
        this.idGeneratorModule = new IdGeneratorModule( new IdGenerator() );
        this.readOnly = Boolean.parseBoolean( (String) params.get( READ_ONLY ) );
        this.backupSlave = Boolean.parseBoolean( (String) params.get( BACKUP_SLAVE ) );
        this.syncHookFactory = txSyncHookFactory;
        this.persistenceModule = new PersistenceModule();
        this.cacheManager = new AdaptiveCacheManager();
        this.params.put( FileSystemAbstraction.class, fileSystem );
        graphDbModule = new GraphDbModule( graphDb, cacheManager, lockManager,
                txModule.getTxManager(), idGeneratorModule.getIdGenerator(),
                readOnly );
        indexStore = new IndexStore( storeDir );
        params.put( IndexStore.class, indexStore );

        if ( storeId != null ) params.put( StoreId.class, storeId );
        params.put( IdGeneratorFactory.class, idGeneratorFactory );
        params.put( TxIdGenerator.class, txIdGenerator );
        params.put( TransactionManager.class, txModule.getTxManager() );
        params.put( LastCommittedTxIdSetter.class, lastCommittedTxIdSetter );
        params.put( GraphDbModule.class, graphDbModule );
        params.put( TxHook.class, txModule.getTxHook() );
    }

    public static Map<Object, Object> getDefaultParams()
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
        if ( osIsWindows() )
        {
            params.put( Config.USE_MEMORY_MAPPED_BUFFERS, "false" );
        }
        else
        {
            // If not on win, default use memory mapping
            params.put( Config.USE_MEMORY_MAPPED_BUFFERS, "true" );
        }
        params.put( NODE_AUTO_INDEXING, "false" );
        params.put( RELATIONSHIP_AUTO_INDEXING, "false" );
        return params;
    }

    public static boolean osIsWindows()
    {
        String nameOs = System.getProperty( "os.name" );
        return nameOs.startsWith( "Windows" );
    }

    public static boolean osIsMacOS()
    {
        String nameOs = System.getProperty( "os.name" );
        return nameOs.equalsIgnoreCase( "Mac OS X" );
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

    public static boolean configValueContainsMultipleParameters( String configValue )
    {
        return configValue != null && configValue.contains( "=" );
    }

    public static Args parseMapFromConfigValue( String name, String configValue )
    {
        Map<String, String> result = new HashMap<String, String>();
        for ( String part : configValue.split( quote( "," ) ) )
        {
            String[] tokens = part.split( quote( "=" ) );
            if ( tokens.length != 2 )
            {
                throw new RuntimeException( "Invalid configuration value '" + configValue +
                        "' for " + name + ". The format is [true/false] or [key1=value1,key2=value2...]" );
            }
            result.put( tokens[0], tokens[1] );
        }
        return new Args( result );
    }

    public static Object getFromConfig( Map<?, ?> config, Object key,
            Object defaultValue )
    {
        Object result = config != null ? config.get( key ) : defaultValue;
        return result != null ? result : defaultValue;
    }
}
