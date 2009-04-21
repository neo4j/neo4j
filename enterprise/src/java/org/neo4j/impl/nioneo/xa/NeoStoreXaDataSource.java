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
package org.neo4j.impl.nioneo.xa;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.core.LockReleaser;
import org.neo4j.impl.core.PropertyIndex;
import org.neo4j.impl.nioneo.store.NeoStore;
import org.neo4j.impl.nioneo.store.PropertyStore;
import org.neo4j.impl.nioneo.store.Store;
import org.neo4j.impl.persistence.IdGenerationFailedException;
import org.neo4j.impl.transaction.LockManager;
import org.neo4j.impl.transaction.xaframework.XaCommand;
import org.neo4j.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.impl.transaction.xaframework.XaConnection;
import org.neo4j.impl.transaction.xaframework.XaContainer;
import org.neo4j.impl.transaction.xaframework.XaDataSource;
import org.neo4j.impl.transaction.xaframework.XaResource;
import org.neo4j.impl.transaction.xaframework.XaTransaction;
import org.neo4j.impl.transaction.xaframework.XaTransactionFactory;
import org.neo4j.impl.util.ArrayMap;

/**
 * A <CODE>NeoStoreXaDataSource</CODE> is a factory for
 * {@link NeoStoreXaConnection NeoStoreXaConnections}.
 * <p>
 * The {@link NioNeoDbPersistenceSource} will create a <CODE>NeoStoreXaDataSoruce</CODE>
 * and then Neo will use it to create {@link XaConnection XaConnections} and
 * {@link XaResource XaResources} when running transactions and performing
 * operations on the node space.
 */
public class NeoStoreXaDataSource extends XaDataSource
{
    private static Logger logger = Logger.getLogger( 
        NeoStoreXaDataSource.class.getName() );

    private final NeoStore neoStore;
    private final XaContainer xaContainer;
    private final ArrayMap<Class<?>,Store> idGenerators;

    private final LockManager lockManager;
    private final LockReleaser lockReleaser;
    private final String storeDir;

    private boolean logApplied = false;

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
     * configuration file or Neo store can't be loaded an <CODE>IOException is
     * thrown</CODE>.
     * 
     * @param params
     *            A map containing configuration parameters and/or configuration
     *            file.
     * @throws IOException
     *             If unable to create data source
     */
    public NeoStoreXaDataSource( Map<?,?> params ) throws IOException,
        InstantiationException
    {
        super( params );
        this.lockManager = (LockManager) params.get( LockManager.class );
        this.lockReleaser = (LockReleaser) params.get( LockReleaser.class );
        String configFileName = (String) params.get( "config" );
        storeDir = (String) params.get( "store_dir" );
        Properties config = new Properties();
        if ( configFileName != null )
        {
            FileInputStream inputStream = new FileInputStream( configFileName );
            try
            {
                config.load( inputStream );
            }
            finally
            {
                inputStream.close();
            }
        }
        Iterator itr = params.entrySet().iterator();
        while ( itr.hasNext() )
        {
            Map.Entry entry = (Map.Entry) itr.next();
            if ( entry.getKey() instanceof String
                && entry.getValue() instanceof String )
            {
                config.setProperty( (String) entry.getKey(), (String) entry
                    .getValue() );
            }
        }
        String store = (String) config.get( "neo_store" );
        File file = new File( store );
        String create = config.getProperty( "create" );
        if ( !file.exists() && "true".equals( create ) )
        {
            autoCreatePath( store );
            NeoStore.createStore( store );
        }

        neoStore = new NeoStore( config );
        xaContainer = XaContainer.create( (String) config.get( "logical_log" ),
            new CommandFactory( neoStore ), new TransactionFactory() );

        xaContainer.openLogicalLog();
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
                    + dirs + "] for Neo store." );
            }
        }
    }

    /**
     * Creates a data source with minimum (no memory mapped) configuration.
     * 
     * @param neoStoreFileName
     *            The file name of the neo store
     * @param logicalLogPath
     *            The file name of the logical log
     * @throws IOException
     *             If unable to open store
     */
    public NeoStoreXaDataSource( String neoStoreFileName,
        String logicalLogPath, LockManager lockManager,
        LockReleaser lockReleaser )
        throws IOException, InstantiationException
    {
        super( null );
        this.lockManager = lockManager;
        this.lockReleaser = lockReleaser;
        storeDir = logicalLogPath;
        neoStore = new NeoStore( neoStoreFileName );
        xaContainer = XaContainer.create( logicalLogPath, new CommandFactory(
            neoStore ), new TransactionFactory() );

        xaContainer.openLogicalLog();
        if ( !xaContainer.getResourceManager().hasRecoveredTransactions() )
        {
            neoStore.makeStoreOk();
        }
        else
        {
            logger.info( "Waiting for TM to take care of recovered " + 
                "transactions." );
        }
        idGenerators = new ArrayMap<Class<?>,Store>( 5, false, false );
        this.idGenerators.put( Node.class, neoStore.getNodeStore() );
        this.idGenerators.put( Relationship.class, 
            neoStore.getRelationshipStore() );
        this.idGenerators.put( RelationshipType.class, 
            neoStore.getRelationshipTypeStore() );
        // get TestXa unit test to run
        this.idGenerators.put( PropertyStore.class, 
            neoStore.getPropertyStore() );
        this.idGenerators.put( PropertyIndex.class, 
            neoStore.getPropertyStore().getIndexStore() );
    }
    
    NeoStore getNeoStore()
    {
        return neoStore;
    }

    public void close()
    {
        xaContainer.close();
        if ( logApplied )
        {
            neoStore.rebuildIdGenerators();
            logApplied = false;
        }
        neoStore.close();
        logger.fine( "NeoStore closed" );
    }

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

        public XaTransaction create( int identifier )
        {
            return new NeoTransaction( identifier, getLogicalLog(), neoStore,
                lockReleaser, lockManager );
        }

        public void recoveryComplete()
        {
            logger.fine( "Recovery complete, "
                + "all transactions have been resolved" );
            logger.fine( "Rebuilding id generators as needed. "
                + "This can take a while for large stores..." );
            neoStore.makeStoreOk();
            logger.fine( "Rebuild of id generators complete." );
        }

        @Override
        public long getCurrentVersion()
        {
            if ( getLogicalLog().scanIsComplete() )
            {
                return neoStore.getVersion();
            }
            neoStore.setRecoveredStatus( true );
            try
            {
                return neoStore.getVersion();
            }
            finally
            {
                neoStore.setRecoveredStatus( false );
            }
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
    }

    public int nextId( Class<?> clazz )
    {
        Store store = idGenerators.get( clazz );

        if ( store == null )
        {
            throw new IdGenerationFailedException( "No IdGenerator for: "
                + clazz );
        }
        return store.nextId();
    }

    public int getHighestPossibleIdInUse( Class<?> clazz )
    {
        Store store = idGenerators.get( clazz );
        if ( store == null )
        {
            throw new IdGenerationFailedException( "No IdGenerator for: "
                + clazz );
        }
        return store.getHighestPossibleIdInUse();
    }

    public int getNumberOfIdsInUse( Class<?> clazz )
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
    public void keepLogicalLogs( boolean keep )
    {
        xaContainer.getLogicalLog().setKeepLogs( keep );
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
    
    @Override
    public void applyLog( ReadableByteChannel byteChannel ) throws IOException
    {
        logApplied = true;
        xaContainer.getLogicalLog().applyLog( byteChannel );
    }
    
    @Override
    public void rotateLogicalLog() throws IOException
    {
        // flush done inside rotate
        xaContainer.getLogicalLog().rotate();
    }
    
    @Override
    public ReadableByteChannel getLogicalLog( long version ) throws IOException
    {
        return xaContainer.getLogicalLog().getLogicalLog( version );
    }

    @Override
    public boolean hasLogicalLog( long version )
    {
        return xaContainer.getLogicalLog().hasLogicalLog( version );
    }
    
    @Override
    public boolean deleteLogicalLog( long version )
    {
        return xaContainer.getLogicalLog().deleteLogicalLog( version );
    }
    
    @Override
    public void setAutoRotate( boolean rotate )
    {
        xaContainer.getLogicalLog().setAutoRotateLogs( rotate );
    }
    
    @Override
    public void setLogicalLogTargetSize( long size )
    {
        xaContainer.getLogicalLog().setLogicalLogTargetSize( size );
    }
    
    @Override
    public void makeBackupSlave()
    {
        xaContainer.getLogicalLog().makeBackupSlave();
    }
}