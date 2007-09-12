package org.neo4j.impl.nioneo.xa;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.transaction.xa.XAException;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.core.PropertyIndex;
import org.neo4j.impl.nioneo.store.NeoStore;
import org.neo4j.impl.nioneo.store.PropertyStore;
import org.neo4j.impl.nioneo.store.Store;
import org.neo4j.impl.persistence.IdGenerationFailedException;
import org.neo4j.impl.transaction.xaframework.XaCommand;
import org.neo4j.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.impl.transaction.xaframework.XaConnection;
import org.neo4j.impl.transaction.xaframework.XaContainer;
import org.neo4j.impl.transaction.xaframework.XaDataSource;
import org.neo4j.impl.transaction.xaframework.XaTransaction;
import org.neo4j.impl.transaction.xaframework.XaTransactionFactory;
import org.neo4j.impl.util.ArrayMap;

/**
 * A <CODE>NeoStoreXaDataSource</CODE> is a facotry for 
 * {@link NeoStoreXaConnection NeoStoreXaConnections}. 
 * <p>
 * The {@link NioNeoDbPersistenceSource} will create a 
 * <CODE>NeoStoreXaDataSoruce</CODE> and then Neo will use it to create 
 * {@link XaConnection XaConnections} and {@link XaResource XaResources} when
 * running transactions and performing operations on the node space.
 */
public class NeoStoreXaDataSource extends XaDataSource
{
	private static Logger logger = 
		Logger.getLogger( NeoStoreXaDataSource.class.getName() );
	
	private final NeoStore neoStore;
	private final XaContainer xaContainer;
	private final ArrayMap<Class,Store> idGenerators;
	

	/**
	 * Creates a <CODE>NeoStoreXaDataSource</CODE> using configuration from
	 * <CODE>params</CODE>. First the map is checked for the parameter
	 * <CODE>config</CODE>. If that parameter exists a config file with that
	 * value is loaded (via {@link Properties#load}). Any parameter that
	 * exist in the config file and in the map passed into this constructor
	 * will take the value from the map.
	 * <p>
	 * If <CODE>config</CODE> parameter is set but file doesn't exist an 
	 * <CODE>IOException</CODE> is thrown. If any problem is found with 
	 * that configuration file or Neo store can't be loaded an 
	 * <CODE>IOException is thrown</CODE>.
	 * 
	 * @param params A map containing configuration parameters and/or
	 * configuration file.
	 * @throws IOException If unable to create data source
	 */
	public NeoStoreXaDataSource( Map params ) throws IOException, 
		InstantiationException
	{
		super( params );
		String configFileName = ( String ) params.get( "config" );
		Properties config = new Properties();
		if ( configFileName != null )
		{
			FileInputStream inputStream = new FileInputStream( 
				configFileName );
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
			Map.Entry entry = ( Map.Entry ) itr.next();
			if ( entry.getKey() instanceof String && 
				entry.getValue() instanceof String )
			{
				config.setProperty( ( String ) entry.getKey(), 
					( String ) entry.getValue() ); 
			}
		}
		String store = ( String ) config.get( "neo_store" );
		File file = new File( store );
		String create = config.getProperty( "create" );
		if ( !file.exists() && "true".equals( create ) )
		{
			autoCreatePath( store );
			NeoStore.createStore( store );
		}
			
		neoStore = new NeoStore( config );
		xaContainer = XaContainer.create( 
			( String ) config.get( "logical_log" ), 
			new CommandFactory( neoStore ), 
			new TransactionFactory( neoStore ) );
		try
		{
			xaContainer.setLazyDoneRecords();
		}
		catch ( XAException e )
		{
			throw new IOException( "Unable to set lazy done records, " + e );
		}
		TxInfoManager.getManager().setRealLog( xaContainer.getLogicalLog() );
		xaContainer.openLogicalLog();
		if ( !xaContainer.getResourceManager().hasRecoveredTransactions() )
		{
			neoStore.makeStoreOk();
		}
		else
		{
			logger.info(  
				"Waiting for TM to take care of recovered transactions." );
		}
		idGenerators = new ArrayMap<Class,Store>( 5, false, false );
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
				throw new IOException( "Unable to create directory path[" + 
					dirs + "] for Neo store." );
			}
		}
	}

	/**
	 * Creates a data source with minimum (no memory mapped) configuration.
	 * 
	 * @param neoStoreFileName The file name of the neo store
	 * @param logicalLogPath The file name of the logical log
	 * @throws IOException If unable to open store
	 */
	public NeoStoreXaDataSource( String neoStoreFileName, 
		String logicalLogPath ) throws IOException, InstantiationException
	{
		super( null );
		neoStore = new NeoStore( neoStoreFileName );
		xaContainer = XaContainer.create( logicalLogPath, 
			new CommandFactory( neoStore ), 
			new TransactionFactory( neoStore ) );
		try
		{
			xaContainer.setLazyDoneRecords();
		}
		catch ( XAException e )
		{
			throw new IOException( "Unable to set lazy done records, " + e );
		}
		TxInfoManager.getManager().setRealLog( xaContainer.getLogicalLog() );
		xaContainer.openLogicalLog();
		if ( !xaContainer.getResourceManager().hasRecoveredTransactions() )
		{
			neoStore.makeStoreOk();
		}
		else
		{
			logger.info( 
				"Waiting for TM to take care of recovered transactions." );
		}
		idGenerators = new ArrayMap<Class,Store>( 5, false, false );
		this.idGenerators.put( Node.class, neoStore.getNodeStore() );
		this.idGenerators.put( Relationship.class, 
			neoStore.getRelationshipStore() );
		this.idGenerators.put( RelationshipType.class, 
			neoStore.getRelationshipTypeStore() ); 
		// hack to get TestXa unit test to run
		this.idGenerators.put( PropertyStore.class, 
				neoStore.getPropertyStore() ); 
		this.idGenerators.put( PropertyIndex.class, 
			neoStore.getPropertyStore().getIndexStore() );
	}

	// used for testing
	public void truncateLogicalLog() throws IOException
	{
		xaContainer.getLogicalLog().truncate();
	}
	
	NeoStore getNeoStore()
	{
		return neoStore;
	}
	
	public void close()
	{
		xaContainer.close();
		try
		{
			neoStore.close();
			logger.fine( "NeoStore closed" );
		}
		catch ( IOException e )
		{
			logger.log( Level.SEVERE, "Unable to close NeoStore ", e );
		}
	}	
	
	public XaConnection getXaConnection()
	{
		return new NeoStoreXaConnection( neoStore, 
			xaContainer.getResourceManager() );
	}
	
	private static class CommandFactory extends XaCommandFactory
	{
		private NeoStore neoStore = null;
		
		CommandFactory( NeoStore neoStore )
		{
			this.neoStore = neoStore;
		}
		
		public XaCommand readCommand( FileChannel fileChannel, 
			ByteBuffer buffer ) throws IOException
		{
			Command command = Command.readCommand( neoStore, fileChannel, 
				buffer );
			if ( command != null )
			{
				command.setRecovered();
			}
			return command;
		}
	}
	
	private static class TransactionFactory extends XaTransactionFactory
	{
		private NeoStore neoStore;
		
		TransactionFactory( NeoStore neoStore )
		{
			this.neoStore = neoStore;
		}
		
		public XaTransaction create( int identifier )
		{
			return new NeoTransaction( identifier, getLogicalLog(), neoStore );
		}
		
		public void recoveryComplete()
		{
			logger.info( "Logical log recovery complete, " + 
				"all transactions have been resolved" );
			try
			{
				neoStore.makeStoreOk();
			}
			catch ( IOException e )
			{
				throw new RuntimeException( "Unable to make stores ok", e );
			}
		}

		@Override
        public void lazyDoneWrite( List<Integer> identifiers ) throws XAException
        {
			try
            {
                neoStore.flushAll();
            }
            catch ( IOException e )
            {
                e.printStackTrace();
                throw new XAException( "Unable to flush neo store." );
            }
        }
	}

	public int nextId( Class clazz )
	{
		Store store = idGenerators.get( clazz );
		
		if ( store == null )
		{
			throw new IdGenerationFailedException( "No IdGenerator for: " + 
				clazz );
		}
		try
		{
			return store.nextId();
		}
		catch ( IOException e )
		{
			throw new IdGenerationFailedException( e );
		}
	}

	public int getHighestPossibleIdInUse( Class clazz )
    {
		Store store = idGenerators.get( clazz );
		if ( store == null )
		{
			throw new IdGenerationFailedException( "No IdGenerator for: " + 
				clazz );
		}
		return store.getHighestPossibleIdInUse();
    }

	public int getNumberOfIdsInUse( Class clazz )
    {
		Store store = idGenerators.get( clazz );
		if ( store == null )
		{
			throw new IdGenerationFailedException( "No IdGenerator for: " + 
				clazz );
		}
		return store.getNumberOfIdsInUse();
    }
}
