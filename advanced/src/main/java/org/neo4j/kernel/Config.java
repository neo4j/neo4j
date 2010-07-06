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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.cache.AdaptiveCacheManager;
import org.neo4j.kernel.impl.core.GraphDbModule;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.core.TxEventSyncHookFactory;
import org.neo4j.kernel.impl.persistence.IdGenerator;
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

    public static final String USE_MEMORY_MAPPED_BUFFERS =
        "use_memory_mapped_buffers";
    public static final String DUMP_CONFIGURATION = "dump_configuration";
    public static final String KEEP_LOGICAL_LOGS = "keep_logical_logs";
    public static final String READ_ONLY = "read_only";
    public static final String BACKUP_SLAVE = "backup_slave";

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
    private final Map<Object, Object> params;
    private final Map inputParams;
    private final TxEventSyncHookFactory syncHookFactory;

    private final KernelPanicEventGenerator kpe;

    private final boolean readOnly;
    private final boolean backupSlave;

    Config( GraphDatabaseService graphDb, String storeDir, Map<String, String> inputParams,
            KernelPanicEventGenerator kpe, TxModule txModule, LockManager lockManager,
            LockReleaser lockReleaser, IdGenerator idGenerator,
            TxEventSyncHookFactory txSyncHookFactory )
    {
        this.kpe = kpe;
        this.storeDir = storeDir;
        this.inputParams = inputParams;
        this.params = getDefaultParams();
        this.txModule = txModule;
        this.lockManager = lockManager;
        this.lockReleaser = lockReleaser;
        this.idGeneratorModule = new IdGeneratorModule( idGenerator );
        this.readOnly = Boolean.parseBoolean( (String) params.get( READ_ONLY ) );
        this.backupSlave = Boolean.parseBoolean( (String) params.get( READ_ONLY ) );
        this.syncHookFactory = txSyncHookFactory;
        this.persistenceModule = new PersistenceModule();
        this.cacheManager = new AdaptiveCacheManager();
        graphDbModule = new GraphDbModule( graphDb, cacheManager, lockManager,
                txModule.getTxManager(), idGeneratorModule.getIdGenerator(),
                Boolean.parseBoolean( (String) params.get( READ_ONLY ) ) );
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

    Map<Object, Object> getInputParams()
    {
        return inputParams;
    }

    TxEventSyncHookFactory getSyncHookFactory()
    {
        return syncHookFactory;
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
}
