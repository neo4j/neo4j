/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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
package org.neo4j.api.core;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.TransactionManager;

import org.neo4j.impl.cache.AdaptiveCacheManager;
import org.neo4j.impl.core.LockReleaser;
import org.neo4j.impl.core.NeoModule;
import org.neo4j.impl.event.EventModule;
import org.neo4j.impl.nioneo.xa.NioNeoDbPersistenceSource;
import org.neo4j.impl.persistence.IdGeneratorModule;
import org.neo4j.impl.persistence.PersistenceModule;
import org.neo4j.impl.transaction.LockManager;
import org.neo4j.impl.transaction.TxModule;
import org.neo4j.impl.transaction.xaframework.XaDataSource;

class NeoJvmInstance
{

    private static final String NIO_NEO_DB_CLASS = "org.neo4j.impl.nioneo.xa.NeoStoreXaDataSource";
    private static final String DEFAULT_DATA_SOURCE_NAME = "nioneodb";

    private static final String LUCENE_DS_CLASS = "org.neo4j.util.index.LuceneDataSource";

    private boolean started = false;
    private boolean create;
    private String storeDir;

    NeoJvmInstance( String storeDir, boolean create )
    {
        this.storeDir = storeDir;
        this.create = create;
    }

    private Config config = null;
    private NioNeoDbPersistenceSource persistenceSource = null;

    public Config getConfig()
    {
        return config;
    }

    public void start()
    {
        start( new HashMap<String,String>() );
    }

    private Map<Object,Object> getDefaultParams()
    {
        Map<Object,Object> params = new HashMap<Object,Object>();
        params.put( "neostore.nodestore.db.mapped_memory", "20M" );
        params.put( "neostore.propertystore.db.mapped_memory", "90M" );
        params.put( "neostore.propertystore.db.index.mapped_memory", "1M" );
        params.put( "neostore.propertystore.db.index.keys.mapped_memory", "1M" );
        params.put( "neostore.propertystore.db.strings.mapped_memory", "130M" );
        params.put( "neostore.propertystore.db.arrays.mapped_memory", "130M" );
        params.put( "neostore.relationshipstore.db.mapped_memory", "100M" );
        return params;
    }

    /**
     * Starts Neo with default configuration using NioNeo DB as persistence
     * store.
     * 
     * @param storeDir
     *            path to directory where NionNeo DB store is located
     * @param create
     *            if true a new NioNeo DB store will be created if no store
     *            exist at <CODE>storeDir</CODE>
     * @param configuration
     *            parameters
     * @throws StartupFailedException
     *             if unable to start
     */
    public synchronized void start( Map<String,String> stringParams )
    {
        if ( started )
        {
            throw new IllegalStateException( "A Neo instance already started" );
        }
        Map<Object,Object> params = getDefaultParams();
        for ( Map.Entry<String,String> entry : stringParams.entrySet() )
        {
            params.put( entry.getKey(), entry.getValue() );
        }
        config = new Config( storeDir, params );
        // create NioNeo DB persistence source
        storeDir = convertFileSeparators( storeDir );
        String separator = System.getProperty( "file.separator" );
        String store = storeDir + separator + "neostore";
        params.put( "store_dir", storeDir );
        params.put( "neo_store", store );
        params.put( "create", String.valueOf( create ) );
        String logicalLog = storeDir + separator + "nioneo_logical.log";
        params.put( "logical_log", logicalLog );
        byte resourceId[] = "414141".getBytes();
        params.put( LockManager.class, config.getLockManager() );
        params.put( LockReleaser.class, config.getLockReleaser() );
        config.getTxModule().registerDataSource( DEFAULT_DATA_SOURCE_NAME,
            NIO_NEO_DB_CLASS, resourceId, params );
        // hack for lucene index recovery if in path
        XaDataSource lucene = null;
        try
        {
            Class clazz = Class.forName( LUCENE_DS_CLASS );
            cleanWriteLocksInLuceneDirectory( storeDir + "/lucene" );
            lucene = registerLuceneDataSource( clazz.getName(), config
                .getTxModule(), storeDir + "/lucene", config.getLockManager() );
        }
        catch ( ClassNotFoundException e )
        { // ok index util not on class path
        }
        // System.setProperty( "neo.tx_log_directory", storeDir );
        persistenceSource = new NioNeoDbPersistenceSource();
        config.setNeoPersistenceSource( DEFAULT_DATA_SOURCE_NAME, create );
        config.getIdGeneratorModule().setPersistenceSourceInstance(
            persistenceSource );
        config.getEventModule().init();
        config.getTxModule().init();
        config.getPersistenceModule().init();
        persistenceSource.init();
        config.getIdGeneratorModule().init();
        config.getNeoModule().init();

        config.getEventModule().start();
        config.getTxModule().start();
        config.getPersistenceModule().start( persistenceSource );
        persistenceSource.start( config.getTxModule().getXaDataSourceManager() );
        config.getIdGeneratorModule().start();
        config.getNeoModule().start( params );
        if ( lucene != null )
        {
            config.getTxModule().getXaDataSourceManager().unregisterDataSource(
                "lucene" );
            lucene = null;
        }
        started = true;
    }

    private void cleanWriteLocksInLuceneDirectory( String luceneDir )
    {
        File dir = new File( luceneDir );
        if ( !dir.isDirectory() )
        {
            return;
        }
        for ( File file : dir.listFiles() )
        {
            if ( file.isDirectory() )
            {
                cleanWriteLocksInLuceneDirectory( file.getAbsolutePath() );
            }
            else if ( file.getName().equals( "write.lock" ) )
            {
                boolean success = file.delete();
                assert success;
            }
        }
    }

    private XaDataSource registerLuceneDataSource( String className,
        TxModule txModule, String luceneDirectory, LockManager lockManager )
    {
        byte resourceId[] = "162373".getBytes();
        Map<Object,Object> params = new HashMap<Object,Object>();
        params.put( "dir", luceneDirectory );
        params.put( LockManager.class, lockManager );
        return txModule.registerDataSource( "lucene", className, resourceId,
            params, true );
    }

    private String convertFileSeparators( String fileName )
    {
        String fileSeparator = System.getProperty( "file.separator" );
        if ( "\\".equals( fileSeparator ) )
        {
            return fileName.replace( '/', '\\' );
        }
        else if ( "/".equals( fileSeparator ) )
        {
            return fileName.replace( '\\', '/' );
        }
        // dont know what to do
        return fileName;
    }

    /**
     * Returns true if Neo is started.
     * 
     * @return True if Neo started
     */
    public boolean started()
    {
        return started;
    }

    /**
     * Shut down Neo.
     */
    public synchronized void shutdown()
    {
        if ( started )
        {
            config.getNeoModule().stop();
            config.getIdGeneratorModule().stop();
            persistenceSource.stop();
            config.getPersistenceModule().stop();
            config.getTxModule().stop();
            config.getEventModule().stop();
            config.getNeoModule().destroy();
            config.getIdGeneratorModule().destroy();
            persistenceSource.destroy();
            config.getPersistenceModule().destroy();
            config.getTxModule().destroy();
            config.getEventModule().destroy();
        }
        started = false;
    }

    public static class Config
    {
        private EventModule eventModule;
        private AdaptiveCacheManager cacheManager;
        private TxModule txModule;
        private LockManager lockManager;
        private LockReleaser lockReleaser;
        private PersistenceModule persistenceModule;
        private boolean create = false;
        private String persistenceSourceName;
        private IdGeneratorModule idGeneratorModule;
        private NeoModule neoModule;
        private String storeDir;
        private final Map<Object,Object> params;

        Config( String storeDir, Map<Object,Object> params )
        {
            this.storeDir = storeDir;
            this.params = params;
            eventModule = new EventModule();
            cacheManager = new AdaptiveCacheManager();
            txModule = new TxModule( this.storeDir );
            lockManager = new LockManager( txModule.getTxManager() );
            persistenceModule = new PersistenceModule( txModule.getTxManager() );
            idGeneratorModule = new IdGeneratorModule();
            neoModule = new NeoModule( cacheManager, lockManager, txModule
                .getTxManager(), persistenceModule.getPersistenceManager(), 
                idGeneratorModule.getIdGenerator() );
            lockReleaser = neoModule.getLockReleaser();
        }

        /**
         * Sets the persistence source for neo to use. If this method is never
         * called default persistence source is used (NioNeo DB).
         * 
         * @param name
         *            fqn name of persistence source to use
         */
        void setNeoPersistenceSource( String name, boolean create )
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

        public EventModule getEventModule()
        {
            return eventModule;
        }

        public TxModule getTxModule()
        {
            return txModule;
        }

        public NeoModule getNeoModule()
        {
            return neoModule;
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

        public Map<Object,Object> getParams()
        {
            return this.params;
        }
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return config.getNeoModule().getRelationshipTypes();
    }

    public boolean transactionRunning()
    {
        try
        {
            return config.getTxModule().getTxManager().getTransaction() != null;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    public TransactionManager getTransactionManager()
    {
        return config.getTxModule().getTxManager();
    }
}