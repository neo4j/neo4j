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
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.ClosableIterable;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.Store;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.store.WindowPoolStats;
import org.neo4j.kernel.impl.persistence.IdGenerationFailedException;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.xaframework.LogBackedXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaContainer;
import org.neo4j.kernel.impl.transaction.xaframework.XaResource;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransaction;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransactionFactory;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * A <CODE>NeoStoreXaDataSource</CODE> is a factory for
 * {@link NeoStoreXaConnection NeoStoreXaConnections}.
 * <p>
 * The {@link NioNeoDbPersistenceSource} will create a <CODE>NeoStoreXaDataSoruce</CODE>
 * and then Neo4j kernel will use it to create {@link XaConnection XaConnections} and
 * {@link XaResource XaResources} when running transactions and performing
 * operations on the node space.
 */
public class NeoStoreXaDataSource extends LogBackedXaDataSource
{
    public static final byte BRANCH_ID[] = UTF8.encode( "414141" );
    private static final String REBUILD_IDGENERATORS_FAST = "rebuild_idgenerators_fast";

    private static Logger logger = Logger.getLogger(
        NeoStoreXaDataSource.class.getName() );

    private final NeoStore neoStore;
    private final XaContainer xaContainer;
    private final ArrayMap<Class<?>,Store> idGenerators;

    private final LockManager lockManager;
    private final LockReleaser lockReleaser;
    private final String storeDir;
    private final boolean readOnly;

    private boolean logApplied = false;
    
    private final StringLogger msgLog;

    /**
     * Creates a <CODE>NeoStoreXaDataSource</CODE> using configuration from
     * <CODE>params</CODE>. First the map is checked for the parameter
     * <CODE>config</CODE>.
     * If that parameter exists a config file with that value is loaded (via
     * {@link Properties#load}). Any parameter that exist in the config file
     * and in the map passed into this constructor will take the value from the
     * map.
     * <p>
     * If <CODE>config</CODE> parameter is set but file doesn't exist an
     * <CODE>IOException</CODE> is thrown. If any problem is found with that
     * configuration file or Neo4j store can't be loaded an <CODE>IOException is
     * thrown</CODE>.
     *
     * @param params
     *            A map containing configuration parameters and/or configuration
     *            file.
     * @throws IOException
     *             If unable to create data source
     */
    public NeoStoreXaDataSource( Map<Object,Object> config ) throws IOException,
        InstantiationException
    {
        super( config );
        readOnly = Boolean.parseBoolean( (String) config.get( Config.READ_ONLY ) );
        this.lockManager = (LockManager) config.get( LockManager.class );
        this.lockReleaser = (LockReleaser) config.get( LockReleaser.class );
        storeDir = (String) config.get( "store_dir" );
        msgLog = StringLogger.getLogger( storeDir );
        String store = (String) config.get( "neo_store" );
        if ( !config.containsKey( REBUILD_IDGENERATORS_FAST ) )
        {
            config.put( REBUILD_IDGENERATORS_FAST, "true" );
        }
        File file = new File( store );
        String create = "" + config.get( "create" );
        if ( !readOnly && !file.exists() && "true".equals( create ) )
        {
            msgLog.logMessage( "Creating new db @ " + store, true );
            autoCreatePath( store );
            NeoStore.createStore( store, config );
        }

        neoStore = new NeoStore( config );
        xaContainer = XaContainer.create( this, (String) config.get( "logical_log" ),
                new CommandFactory( neoStore ), new TransactionFactory(), config );

        if ( !readOnly )
        {
            neoStore.setRecoveredStatus( true );
            try
            {
                xaContainer.openLogicalLog();
            }
            finally
            {
                neoStore.setRecoveredStatus( false );
            }
        }
        if ( !xaContainer.getResourceManager().hasRecoveredTransactions() )
        {
            neoStore.makeStoreOk();
        }
        else
        {
            logger.fine( "Waiting for TM to take care of recovered " +
                "transactions." );
        }
        idGenerators = new ArrayMap<Class<?>,Store>( 5, false, false );
        this.idGenerators.put( Node.class, neoStore.getNodeStore() );
        this.idGenerators.put( Relationship.class,
            neoStore.getRelationshipStore() );
        this.idGenerators.put( RelationshipType.class,
            neoStore.getRelationshipTypeStore() );
        this.idGenerators.put( PropertyStore.class,
            neoStore.getPropertyStore() );
        this.idGenerators.put( PropertyIndex.class,
            neoStore.getPropertyStore().getIndexStore() );
        xaContainer.getLogicalLog().setKeepLogs(
                shouldKeepLog( (String) config.get( Config.KEEP_LOGICAL_LOGS ), "nioneodb" ) );
        setLogicalLogAtCreationTime( xaContainer.getLogicalLog() );
    }

    private void autoCreatePath( String store ) throws IOException
    {
        String fileSeparator = System.getProperty( "file.separator" );
        int index = store.lastIndexOf( fileSeparator );
        String dirs = store.substring( 0, index );
        File directories = new File( dirs );
        if ( !directories.exists() )
        {
            if ( !directories.mkdirs() )
            {
                throw new IOException( "Unable to create directory path["
                    + dirs + "] for Neo4j store." );
            }
        }
    }

    /**
     * Creates a data source with minimum (no memory mapped) configuration.
     *
     * @param neoStoreFileName
     *            The file name of the store
     * @param logicalLogPath
     *            The file name of the logical log
     * @throws IOException
     *             If unable to open store
     */
//    public NeoStoreXaDataSource( String neoStoreFileName,
//        String logicalLogPath, LockManager lockManager,
//        LockReleaser lockReleaser )
//        throws IOException, InstantiationException
//    {
//        super( null );
//        this.readOnly = false;
//        this.lockManager = lockManager;
//        this.lockReleaser = lockReleaser;
//        storeDir = logicalLogPath;
//        neoStore = new NeoStore( neoStoreFileName );
//        xaContainer = XaContainer.create( this, logicalLogPath, new CommandFactory(
//            neoStore ), new TransactionFactory(), null );
//        setLogicalLogAtCreationTime( xaContainer.getLogicalLog() );
//
//        xaContainer.openLogicalLog();
//        if ( !xaContainer.getResourceManager().hasRecoveredTransactions() )
//        {
//            neoStore.makeStoreOk();
//        }
//        else
//        {
//            logger.info( "Waiting for TM to take care of recovered " +
//                "transactions." );
//        }
//        idGenerators = new ArrayMap<Class<?>,Store>( 5, false, false );
//        this.idGenerators.put( Node.class, neoStore.getNodeStore() );
//        this.idGenerators.put( Relationship.class,
//            neoStore.getRelationshipStore() );
//        this.idGenerators.put( RelationshipType.class,
//            neoStore.getRelationshipTypeStore() );
//        // get TestXa unit test to run
//        this.idGenerators.put( PropertyStore.class,
//            neoStore.getPropertyStore() );
//        this.idGenerators.put( PropertyIndex.class,
//            neoStore.getPropertyStore().getIndexStore() );
//    }

    NeoStore getNeoStore()
    {
        return neoStore;
    }

    @Override
    public void close()
    {
        if ( !readOnly )
        {
            neoStore.flushAll();
        }
        xaContainer.close();
        if ( logApplied )
        {
            neoStore.rebuildIdGenerators();
            logApplied = false;
        }
        neoStore.close();
        logger.fine( "NeoStore closed" );
        msgLog.logMessage( "NeoStore closed", true );
    }
    
    public StoreId getStoreId()
    {
        return neoStore.getStoreId();
    }

    @Override
    public XaConnection getXaConnection()
    {
        return new NeoStoreXaConnection( neoStore,
            xaContainer.getResourceManager(), getBranchId() );
    }

    private static class CommandFactory extends XaCommandFactory
    {
        private NeoStore neoStore = null;

        CommandFactory( NeoStore neoStore )
        {
            this.neoStore = neoStore;
        }

        @Override
        public XaCommand readCommand( ReadableByteChannel byteChannel,
            ByteBuffer buffer ) throws IOException
        {
            Command command = Command.readCommand( neoStore, byteChannel,
                buffer );
            if ( command != null )
            {
                command.setRecovered();
            }
            return command;
        }
    }

    private class TransactionFactory extends XaTransactionFactory
    {
        TransactionFactory()
        {
        }

        @Override
        public XaTransaction create( int identifier )
        {
            return new WriteTransaction( identifier, getLogicalLog(), neoStore,
                lockReleaser, lockManager );
        }

        @Override
        public void recoveryComplete()
        {
            logger.fine( "Recovery complete, "
                + "all transactions have been resolved" );
            logger.fine( "Rebuilding id generators as needed. "
                + "This can take a while for large stores..." );
            neoStore.flushAll();
            neoStore.makeStoreOk();
            neoStore.setVersion( xaContainer.getLogicalLog().getCurrentLogVersion() );
            logger.fine( "Rebuild of id generators complete." );
        }

        @Override
        public long getCurrentVersion()
        {
            if ( getLogicalLog().scanIsComplete() )
            {
                return neoStore.getVersion();
            }
//            neoStore.setRecoveredStatus( true );
//            try
//            {
                return neoStore.getVersion();
//            }
//            finally
//            {
//                neoStore.setRecoveredStatus( false );
//            }
        }

        @Override
        public long getAndSetNewVersion()
        {
            return neoStore.incrementVersion();
        }

        @Override
        public void flushAll()
        {
            neoStore.flushAll();
        }

        @Override
        public long getLastCommittedTx()
        {
            return neoStore.getLastCommittedTx();
        }
    }

    public long nextId( Class<?> clazz )
    {
        Store store = idGenerators.get( clazz );

        if ( store == null )
        {
            throw new IdGenerationFailedException( "No IdGenerator for: "
                + clazz );
        }
        return store.nextId();
    }

    public long getHighestPossibleIdInUse( Class<?> clazz )
    {
        Store store = idGenerators.get( clazz );
        if ( store == null )
        {
            throw new IdGenerationFailedException( "No IdGenerator for: "
                + clazz );
        }
        return store.getHighestPossibleIdInUse();
    }

    public long getNumberOfIdsInUse( Class<?> clazz )
    {
        Store store = idGenerators.get( clazz );
        if ( store == null )
        {
            throw new IdGenerationFailedException( "No IdGenerator for: "
                + clazz );
        }
        return store.getNumberOfIdsInUse();
    }

    public String getStoreDir()
    {
        return storeDir;
    }

    @Override
    public long getCreationTime()
    {
        return neoStore.getCreationTime();
    }

    @Override
    public long getRandomIdentifier()
    {
        return neoStore.getRandomNumber();
    }

    @Override
    public long getCurrentLogVersion()
    {
        return neoStore.getVersion();
    }

    public long incrementAndGetLogVersion()
    {
        return neoStore.incrementVersion();
    }

    public void setCurrentLogVersion( long version )
    {
        neoStore.setVersion( version );
    }

    // used for testing, do not use.
    public void setLastCommittedTxId( long txId )
    {
        neoStore.setRecoveredStatus( true );
        try
        {
            neoStore.setLastCommittedTx( txId );
        }
        finally
        {
            neoStore.setRecoveredStatus( false );
        }
    }

    ReadTransaction getReadOnlyTransaction()
    {
        return new ReadTransaction( neoStore );
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    public List<WindowPoolStats> getWindowPoolStats()
    {
        return neoStore.getAllWindowPoolStats();
    }
    
    @Override
    public long getLastCommittedTxId()
    {
        return neoStore.getLastCommittedTx();
    }
    
    @Override
    public XaContainer getXaContainer()
    {
        return xaContainer;
    }
    
    @Override
    public boolean setRecovered( boolean recovered )
    {
        boolean currentValue = neoStore.isInRecoveryMode();
        neoStore.setRecoveredStatus( true );
        return currentValue;
    }
    
    @Override
    public ClosableIterable<File> listStoreFiles()
    {
        final Collection<File> files = new ArrayList<File>();
        File neostoreFile = null;
        for ( File dbFile : new File( storeDir ).listFiles() )
        {
            String name = dbFile.getName();
            // To filter for "neostore" is quite future proof, but the "index.db" file
            // maybe should be 
            if ( dbFile.isFile() )
            {
                if ( name.equals( "neostore" ) )
                {
                    neostoreFile = dbFile;
                }
                else if ( (name.startsWith( "neostore" ) || name.equals( IndexStore.INDEX_DB_FILE_NAME )) && !name.endsWith( ".id" ) )
                {
                    files.add( dbFile );
                }
            }
        }
        files.add( neostoreFile );
        
        return new ClosableIterable<File>()
        {

            public Iterator<File> iterator()
            {
                return files.iterator();
            }

            public void close()
            {
            }
        };
    }
}
