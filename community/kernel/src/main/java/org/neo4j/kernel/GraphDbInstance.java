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
package org.neo4j.kernel;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.nioneo.xa.NioNeoDbPersistenceSource;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxModule;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.FileUtils;

class GraphDbInstance
{
    private boolean started = false;
    private final boolean create;
    private String storeDir;

    GraphDbInstance( String storeDir, boolean create, Config config )
    {
        this.storeDir = storeDir;
        this.create = create;
        this.config = config;
    }

    private final Config config;

    private NioNeoDbPersistenceSource persistenceSource = null;

    public Config getConfig()
    {
        return config;
    }

    /**
     * Starts Neo4j with default configuration
     * @param graphDb The graph database service.
     *
     * @param storeDir path to directory where Neo4j store is located
     * @param create if true a new Neo4j store will be created if no store exist
     *            at <CODE>storeDir</CODE>
     * @param configuration parameters
     * @throws StartupFailedException if unable to start
     */
    public synchronized Map<Object, Object> start( AbstractGraphDatabase graphDb,
            KernelExtensionLoader kernelExtensionLoader )
    {
        if ( started )
        {
            throw new IllegalStateException( "Neo4j instance already started" );
        }
        Map<Object, Object> params = config.getParams();
        boolean useMemoryMapped = Boolean.parseBoolean( (String) config.getInputParams().get(
                Config.USE_MEMORY_MAPPED_BUFFERS ) );
        boolean dumpToConsole = Boolean.parseBoolean( (String) config.getInputParams().get(
                Config.DUMP_CONFIGURATION ) );
        storeDir = FileUtils.fixSeparatorsInPath( storeDir );
        AutoConfigurator autoConfigurator = new AutoConfigurator( storeDir, useMemoryMapped, dumpToConsole );
        Map<Object, Object> autoConf = new HashMap<Object, Object>();
        autoConfigurator.configure( autoConf );
        for ( Map.Entry<Object, Object> entry : autoConf.entrySet() )
        {
            if ( !config.getInputParams().containsKey( entry.getKey() ) ) params.put( entry.getKey(), entry.getValue() );
        }

        String separator = System.getProperty( "file.separator" );
        String store = storeDir + separator + NeoStore.DEFAULT_NAME;
        params.put( "store_dir", storeDir );
        params.put( "neo_store", store );
        params.put( "create", String.valueOf( create ) );
        String logicalLog = storeDir + separator + NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME;
        params.put( "logical_log", logicalLog );
        params.put( LockManager.class, config.getLockManager() );
        params.put( LockReleaser.class, config.getLockReleaser() );

        kernelExtensionLoader.configureKernelExtensions();

        XaDataSource nioneoDs = config.getTxModule().registerDataSource( Config.DEFAULT_DATA_SOURCE_NAME,
                Config.NIO_NEO_DB_CLASS, NeoStoreXaDataSource.BRANCH_ID, params );
        boolean success = false;
        try
        {
            // hack for lucene index recovery if in path
            if ( !config.isReadOnly() || config.isBackupSlave() )
            {
                try
                {
                    Class clazz = Class.forName( Config.LUCENE_DS_CLASS );
                    cleanWriteLocksInLuceneDirectory( storeDir + File.separator + "lucene" );
                    byte luceneId[] = UTF8.encode( "162373" );
                    registerLuceneDataSource( "lucene", clazz.getName(),
                            config.getTxModule(), storeDir + File.separator + "lucene",
                            config.getLockManager(), luceneId, params );
                }
                catch ( ClassNotFoundException e )
                { // ok index util not on class path
                }
                catch ( NoClassDefFoundError err )
                { // ok index util not on class path
                }
    
                try
                {
                    Class clazz = Class.forName( Config.LUCENE_FULLTEXT_DS_CLASS );
                    cleanWriteLocksInLuceneDirectory( storeDir + File.separator + "lucene-fulltext" );
                    byte[] luceneId = UTF8.encode( "262374" );
                    registerLuceneDataSource( "lucene-fulltext",
                            clazz.getName(), config.getTxModule(),
                            storeDir + File.separator + "lucene-fulltext", config.getLockManager(),
                            luceneId, params );
                }
                catch ( ClassNotFoundException e )
                { // ok index util not on class path
                }
                catch ( NoClassDefFoundError err )
                { // ok index util not on class path
                }
            }
            persistenceSource = new NioNeoDbPersistenceSource();
            config.setPersistenceSource( Config.DEFAULT_DATA_SOURCE_NAME, create );
            config.getIdGeneratorModule().setPersistenceSourceInstance(
                    persistenceSource );
            config.getTxModule().init();
            config.getPersistenceModule().init();
            persistenceSource.init();
            config.getIdGeneratorModule().init();
            config.getGraphDbModule().init();
    
            kernelExtensionLoader.initializeIndexProviders();
    
            config.getTxModule().start();
            config.getPersistenceModule().start( config.getTxModule().getTxManager(), persistenceSource,
                    config.getSyncHookFactory(), config.getLockReleaser() );
            persistenceSource.start( config.getTxModule().getXaDataSourceManager() );
            config.getIdGeneratorModule().start();
            config.getGraphDbModule().start( config.getLockReleaser(),
                    config.getPersistenceModule().getPersistenceManager(),
                    config.getRelationshipTypeCreator(), params );
    
            started = true;
    
            KernelDiagnostics.register( config.getDiagnosticsManager(), graphDb,
                    (NeoStoreXaDataSource) persistenceSource.getXaDataSource() );
            config.getDiagnosticsManager().startup();
            success = true;
            return Collections.unmodifiableMap( params );
        }
        finally
        {
            if ( !success ) nioneoDs.close();
        }
    }

    private static Map<Object, Object> subset( Map<Object, Object> source, String... keys )
    {
        Map<Object, Object> result = new HashMap<Object, Object>();
        for ( String key : keys )
        {
            if ( source.containsKey( key ) )
            {
                result.put( key, source.get( key ) );
            }
        }
        return result;
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

    private XaDataSource registerLuceneDataSource( String name,
            String className, TxModule txModule, String luceneDirectory,
            LockManager lockManager, byte[] resourceId,
            Map<Object,Object> params )
    {
        params.put( "dir", luceneDirectory );
        params.put( LockManager.class, lockManager );
        return txModule.registerDataSource( name, className, resourceId,
                params, true );
    }

    /**
     * Returns true if Neo4j is started.
     *
     * @return True if Neo4j started
     */
    public boolean started()
    {
        return started;
    }

    /**
     * Shut down Neo4j.
     */
    public synchronized void shutdown()
    {
        if ( started )
        {
            config.getGraphDbModule().stop();
            config.getIdGeneratorModule().stop();
            persistenceSource.stop();
            config.getPersistenceModule().stop();
            config.getTxModule().stop();
            config.getGraphDbModule().destroy();
            config.getIdGeneratorModule().destroy();
            persistenceSource.destroy();
            config.getPersistenceModule().destroy();
            config.getTxModule().destroy();
        }
        started = false;
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return config.getGraphDbModule().getRelationshipTypes();
    }

    public boolean transactionRunning()
    {
        try
        {
            return config.getTxModule().getTxManager().getTransaction() != null;
        }
        catch ( Exception e )
        {
            throw new TransactionFailureException(
                    "Unable to get transaction.", e );
        }
    }

    public TransactionManager getTransactionManager()
    {
        return config.getTxModule().getTxManager();
    }
}
