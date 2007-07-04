package org.neo4j.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Map;
import java.util.logging.Logger;

import org.neo4j.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.impl.nioneo.xa.TxInfoManager;


/**
 * Contains common implementation for {@link AbstractStore} and 
 * {@link AbstractDynamicStore}. 
 */
public abstract class CommonAbstractStore
{
	protected static final Logger logger = 
		Logger.getLogger( CommonAbstractStore.class.getName() ); 
	/**
	 * Returns the type and version that identifies this store.
	 *
	 * @return This store's implementation type and version identifier
	 */
	public abstract String getTypeAndVersionDescriptor(); 
	 
	/**
	 * Called from the constructor after the end header has been checked. 
	 * The store implementation can setup it's  
	 * {@link PersistenceWindow persistence windows} and other resources that 
	 * are needed by overriding this implementation.<p> This default 
	 * implementation does nothing.
	 *
	 * @throws IOException If unable to initialize 
	 */
	protected void initStorage() throws IOException {}
	 

	/**
	 * This method should close/release all resources that the implementation 
	 * of this store has allocated and is called just before the 
	 * <CODE>close()</CODE> method returns. Override this method to clean up
	 * stuff created in {@link #initStorage()} method.<p> This default 
	 * implementation does nothing.
	 *
	 * @throws IOException If unable to close
	 */
	protected void closeStorage() throws IOException {};

	/**
	 * Should do first validation on store validating stuff like version
	 * and id generator. This method is called by constructors.
	 * 
	 * @throws IOException If unable to load store
	 */
	protected abstract void loadStorage() throws IOException;
	
	/**
	 * Should rebuild the id generator from scratch.
	 * 
	 * @throws IOException If unable to rebuild id generator.
	 */
	protected abstract void rebuildIdGenerator() throws IOException;
	
	// default node store id generator grab size
	protected static final int DEFAULT_ID_GRAB_SIZE = 1024;

	private String storageFileName = null;
	private IdGenerator idGenerator = null;
	private FileChannel fileChannel = null;
	private PersistenceWindowPool windowPool;
	private boolean storeOk = true;
	private FileLock fileLock;
	
	private Map config = null;

	/**
	 * Opens and validates the store contained in <CODE>fileName</CODE> 
	 * loading any configuration defined in <CODE>config</CODE>. 
	 * After validation the <CODE>initStorage</CODE> method is called.
	 * <p>
	 * If the store had a clean shutdown it will be marked as <CODE>ok</CODE>
	 * and the {@link #validate()} method will not throw exception when 
	 * invoked. If a problem was found when opening the store the 
	 * {@link #makeStoreOk()} must be invoked else {@link #validate()} will
	 * throw exception.
	 * 
	 * throws IOException if the unable to open the storage or if the 
	 * <CODE>initStorage</CODE> method fails
	 * 
	 * @param fileName The name of the store
	 * @param config The configuration for store (may be null)
	 * @throws IOException If store doesn't exist
	 */
	public CommonAbstractStore( String fileName, Map config )
		throws IOException
	{
		this.storageFileName = fileName;
		this.config = config;
		checkStorage();
		loadStorage();
		initStorage();
	}
	
	/**
	 * Opens and validates the store contained in <CODE>fileName</CODE>. 
	 * After validation the <CODE>initStorage</CODE> method is called.
	 * <p>
	 * If the store had a clean shutdown it will be marked as <CODE>ok</CODE>
	 * and the {@link #validate()} method will not throw exception when 
	 * invoked. If a problem was found when opening the store the 
	 * {@link #makeStoreOk()} must be invoked else {@link #validate()} will
	 * throw exception.
	 * 
	 * throws IOException if the unable to open the storage or if the 
	 * <CODE>initStorage</CODE> method fails
	 * 
	 * @param fileName The name of the store
	 * @throws IOException If store doesn't exist
	 */
	public CommonAbstractStore( String fileName )
		throws IOException
	{
		this.storageFileName = fileName;
		checkStorage();
		loadStorage();
		initStorage();
	}
	
	private void checkStorage() throws IOException
	{
		if ( !new File( storageFileName ).exists() )
		{
			throw new IOException( "No such store[" + storageFileName + "]" );
		}
		this.fileChannel = 
			new RandomAccessFile( storageFileName, "rw" ).getChannel();
		this.fileLock = this.fileChannel.tryLock();
		if ( fileLock == null )
		{
			fileChannel.close();
			throw new IOException( "Unable to lock store [" + 
				storageFileName + "], other program running?" );
		}
	}
	
	/**
	 * Marks this store as "not ok".
	 *
	 */
	protected void setStoreNotOk()
	{
		storeOk = false;
	}
	
	/**
	 * If store is "not ok" <CODE>false</CODE> is returned.
	 * 
	 * @return True if this store is ok
	 */
	protected boolean getStoreOk()
	{
		return storeOk;
	}
	
	/**
	 * Sets the {@link PersistenceWindowPool} for this store to use. Normaly
	 * this is set in the {@link #loadStorage()} method. This method must
	 * be invoked with a valid "pool" before any of the 
	 * {@link #acquireWindow(int, OperationType)} 
	 * {@link #releaseWindow(PersistenceWindow)}
	 * {@link #flush(int)}
	 * {@link #forget(int)}
	 * {@link #close()} methods are invoked.
	 * 
	 * @param pool The window pool this store should use
	 */
	protected void setWindowPool( PersistenceWindowPool pool )
	{
		this.windowPool = pool;
	}
	
	/**
	 * Returns the next id for this store's {@link IdGenerator}.
	 * 
	 * @return The next free id
	 * @throws IOException If unable to get next free id
	 */
	protected int nextId() throws IOException
	{
		return idGenerator.nextId();
	}

	/**
	 * Frees an id for this store's {@link IdGenerator}.
	 * 
	 * @param id The id to free
	 * @throws IOException If unable to free the id
	 */
	protected void freeId( int id ) throws IOException
	{
		idGenerator.freeId( id );
	}
	
	/**
	 * Return the highest id in use.
	 * 
	 * @return The highest id in use.
	 */
	protected int getHighId()
	{
		return idGenerator.getHighId();
	}
	
	/**
	 * Sets the highest id in use (use this when rebuilding id generator).
	 * 
	 * @param highId The high id to set.
	 */
	protected void setHighId( int highId )
	{
		idGenerator.setHighId( highId );
	}
	
	/**
	 * Returns memory assigned for 
	 * {@link MappedPersistenceWindow memory mapped windows} in bytes. The
	 * configuration map passed in one constructor is checked for an 
	 * entry with this stores name.
	 * 
	 * @return The number of bytes memory mapped windows this store has
	 */
	protected int getMappedMem()
	{
		if ( getConfig() != null )
		{
			String realName = storageFileName.replace( '\\', '/' ).substring( 
				storageFileName.lastIndexOf( '/' ) + 1 );
			String mem = ( String ) getConfig().get( 
				realName + ".mapped_memory" );
			if ( mem != null )
			{
				int multiplier = 1;
				if ( mem.endsWith( "M" ) )
				{
					multiplier = 1024*1024;
					mem = mem.substring( 0, mem.length() - 1 );
				}
				else if ( mem.endsWith( "k" ) )
				{
					multiplier = 1024;
					mem = mem.substring( 0, mem.length() - 1 );
				}
				try
				{
					return Integer.parseInt( mem ) * multiplier;
				}
				catch ( NumberFormatException e )
				{
					logger.info(  "Unable to parse mapped memory[" +
						mem + "] string for " + storageFileName );
				}
			}
		}
		return 0;
	}

	/**
	 * If store is not ok a call to this method will rebuild the {@link 
	 * IdGenerator} used by this store and if successfull mark it as 
	 * <CODE>ok</CODE>.
	 * 
	 * @throws IOException If unable to rebuild id generator
	 */
	public void makeStoreOk() throws IOException
	{
		if ( !storeOk )
		{
			rebuildIdGenerator();
			storeOk = true;
		}
	}
	
	/**
	 * Returns the configuration map if set in constructor. 
	 * 
	 * @return A map containing configuration or <CODE>null<CODE> if no 
	 * configuration map set.
	 */
	public Map getConfig()
	{
		return config;
	}

	/**
	 * Acquires a {@link PersistenceWindow} for <CODE>position</CODE> and 
	 * operation <CODE>type</CODE>. Window must be released after operation
	 * has been performed via {@link #releaseWindow(PersistenceWindow)}.
	 * 
	 * @param position The record position
	 * @param type The operation type
	 * @return presistence A window encapsulating the record 
	 * @throws IOException If unable to acquire window
	 */
	protected PersistenceWindow acquireWindow( int position, 
		OperationType type ) throws IOException
	{
		if ( !isInRecoveryMode() && position > idGenerator.getHighId() )
		{
			throw new IOException( "Illegal position[" + position + 
				"] high id[" + idGenerator.getHighId() + "]" );
		}	
		return windowPool.acquire( position, type );
	}
	
	/**
	 * Releases the window and writes the data (async) if the 
	 * <CODE>window</CODE> was a {@link PersistenceRow}. 
	 * 
	 * @param window The window to be released
	 * @throws IOException If window was a <CODE>DirectPersistenceRow</CODE> 
	 * and unable to write out its data to the store
	 */
	protected void releaseWindow( PersistenceWindow window ) throws IOException
	{
		windowPool.release( window );
	}
	
	/**
	 * Flush of all changes identified by <CODE>identifier</CODE> in this
	 * store.
	 * 
	 * @param identifier The (transaction) identifier
	 * @throws IOException If some I/O error occurs flushing the file channel
	 * of this store
	 */
	public void flush( int identifier ) throws IOException
	{
		windowPool.flush( identifier );
	}
	
	/**
	 * Forgets about all changes made by <CODE>identifier</CODE>. This does
	 * not mean that the changes will be reverted. Instead the mapping between
	 * <CODE>identifier</CODE> and {@link PersistenceWindow persistence 
	 * windows} used is removed.
	 * 
	 * @param identifier The (transaction) identifier
	 */
	public void forget( int identifier )
	{
		windowPool.forget( identifier );
	}

	/**
	 * Utility method to determine if we are in recovery mode. Asks the 
	 * {@link TxInfoManager} if we are in recovery.
	 * 
	 * @return <CODE>true</CODE> if we are in recovery mode.
	 */
	protected boolean isInRecoveryMode()
	{
		return TxInfoManager.getManager().isInRecoveryMode();
	}
	
	/**
	 * Returns the name of this store.
	 * 
	 * @return The name of this store
	 */
	public String getStorageFileName()
	{
		return storageFileName;
	}

	/**
	 * Opens the {@link IdGenerator} used by this store.
	 * 
	 * @throws IOException If unable to open the id generator
	 */
	protected void openIdGenerator() throws IOException
	{
		idGenerator = 
			new IdGenerator( storageFileName + ".id", DEFAULT_ID_GRAB_SIZE );
	}

	/**
	 * Closed the {@link IdGenerator} used by this store
	 * 
	 * @throws IOException If unable to close this store
	 */
	protected void closeIdGenerator() throws IOException
	{
		if ( idGenerator != null )
		{
			idGenerator.close();
		}
	}
	
	/**
	 * Closes this store. This will cause all buffers and channels 
	 * to be closed. Requesting an operation from after this method has  
	 * been invoked is illegal and an exception will be thrown.
	 * <p>
	 * This method will start by invoking the {@link #closeStorage} method 
	 * giving the implementing store way to do anything that it needs to do 
	 * before the fileChannel is closed.
	 * 
	 *  @throws IOException If problem when invoking {@link #closeStorage()}
	 */
	public void close() throws IOException
	{
		if ( fileChannel == null )
		{
			return;
		}
		closeStorage();
		if ( windowPool != null )
		{
			windowPool.close();
			windowPool = null;
		}
		closeIdGenerator();
		fileChannel.position( fileChannel.size() );
		ByteBuffer buffer = ByteBuffer.allocate(
			getTypeAndVersionDescriptor().length() );
		buffer.put( getTypeAndVersionDescriptor().getBytes() ).flip();
		fileChannel.write( buffer );
		fileChannel.force( false );
		fileLock.release();
		fileChannel.close();
		fileChannel = null;
	}
	
	/**
	 * Returns a <CODE>FileChannel</CODE> to this storage's file. If 
	 * <CODE>close()</CODE> method has been invoked <CODE>null</CODE> will 
	 * be returned.
	 *
	 * @return A file channel to this storage
	 */
	protected final FileChannel getFileChannel()
	{
		return fileChannel;
	}
	
	/**
	 * Throws a <CODE>RuntimeException</CODE> if store not ok, else does 
	 * nothing. 
	 */
	public void validate()
	{
		if ( !storeOk )
		{
			throw new RuntimeException( "Store not valid" );
		}
	}
}
