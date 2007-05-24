package org.neo4j.impl.nioneo.store;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Map;

/**
 * An abstract representation of a store. A store is a file that contains 
 * records. Each record has a fixed size (<CODE>getRecordSize()</CODE>) so the 
 * position for a record can be calculated by <CODE>id * getRecordSize()</CODE>.
 * <p>
 * A store has an {@link IdGenerator} managing the records that are free or 
 * in use.
 */
public abstract class AbstractStore extends CommonAbstractStore
{
	/**
	 * Returnes the fixed size of each record in this store.
	 *
	 * @return The record size
	 */
	public abstract int getRecordSize();
	
	/**
	 * Creates a new empty store. The facotry method returning an 
	 * implementation of some store type should make use of this method 
	 * to initialize an empty store.
	 * <p>
	 * This method will create a empty store containing the descriptor 
	 * returned by the <CODE>getTypeAndVersionDescriptor()</CODE>. The id 
	 * generator used by this store will also be created
	 *
	 * @param fileName Ihe file name of the store that will be created
	 * @param typeAndVersionDescriptor Ihe type and version descriptor that 
	 * identifies this store
	 * @throws IOException If fileName is null or if file exists
	 */
	protected static void createEmptyStore( String fileName, 
		String typeAndVersionDescriptor ) 
		throws IOException
	{
		// sanity checks
		if ( fileName == null )
		{
			throw new IOException( "Null filename" );
		}
		File file = new File( fileName );
		if ( file.exists() )
		{
			throw new IOException( "Can't create store[" + fileName + 
				"], file already exists" );
		}

		// write the header
		FileChannel channel = new FileOutputStream( fileName ).getChannel();
		int endHeaderSize = typeAndVersionDescriptor.length();
		ByteBuffer buffer = ByteBuffer.allocate( endHeaderSize );
		buffer.put( typeAndVersionDescriptor.getBytes() ).flip();
		channel.write( buffer );
		channel.force( false );
		channel.close();
		IdGenerator.createGenerator( fileName + ".id" ); 
	}
	
	public AbstractStore( String fileName, Map config )
		throws IOException
	{
		super( fileName, config );
	}
	
	public AbstractStore( String fileName )
		throws IOException
	{
		super( fileName );
	}
	
	protected void loadStorage() throws IOException
	{
		long fileSize = getFileChannel().size();
		String expectedVersion = getTypeAndVersionDescriptor();
		byte version[] = new byte[ expectedVersion.length() ];
		ByteBuffer buffer = ByteBuffer.wrap( version );
		if ( fileSize >= expectedVersion.length() )
		{
			getFileChannel().position( fileSize - expectedVersion.length() );
		}
		else
		{
			setStoreNotOk();
		}
		getFileChannel().read( buffer );
		if ( !expectedVersion.equals( new String( version ) ) )
		{
			setStoreNotOk();
		}
		if ( getRecordSize() != 0 && 
			( fileSize - expectedVersion.length() ) % getRecordSize() != 0 )
		{
			setStoreNotOk();
		}
		if ( getStoreOk() )
		{
			getFileChannel().truncate( fileSize - expectedVersion.length() );
		}
		try
		{
			openIdGenerator();
		}
		catch ( IOException e )
		{
			setStoreNotOk();
		}
		setWindowPool( new PersistenceWindowPool( getStorageFileName(), 
			getRecordSize(), getFileChannel(), getMappedMem() ) );
	}
	
	/**
	 * Returns the next free id from {@link IdGenerator} used by this storage. 
	 *
	 * @return The id generator for this storage
	 */
	public int nextId() throws IOException
	{
		return super.nextId();
	}
	
	/**
	 * Returns the highest id in use by this store.
	 * 
	 * @return The highest id in use
	 */
	public int getHighId()
	{
		return super.getHighId();
	}
	
	/**
	 * Sets the high id of {@link IdGenerator}.
	 * 
	 * @param id The high id
	 */
	protected void setHighId( int id )
	{
		super.setHighId( id );
	}

	/**
	 * Makes a id previously acquired from <CODE>nextId()</CODE> method 
	 * available again.
	 *
	 * @param id The id to free
	 */
	public void freeId( int id ) throws IOException
	{
		super.freeId( id );
	}
	
	/**
	 * Rebuilds the {@link IdGenerator} by looping through all records
	 * and checking if record in use or not.
	 * 
	 * @throws IOException if unable to rebuild the id generator
	 */
	protected void rebuildIdGenerator() throws IOException
	{
		// TODO: fix this hardcoding
		final byte RECORD_NOT_IN_USE = 0;
		
		logger.info( "Rebuilding id generator for[" + getStorageFileName() + 
			"] ..." );
		closeIdGenerator();
		File file = new File( getStorageFileName() + ".id" );
		if ( file.exists() )
		{
			file.delete();
		}
		IdGenerator.createGenerator( getStorageFileName() + ".id" );
		openIdGenerator();
		FileChannel fileChannel = getFileChannel();
		long fileSize = fileChannel.size();
		int recordSize = getRecordSize();
//		long dot = fileSize / recordSize / 20;
		long defragedCount = 0;
		ByteBuffer byteBuffer = ByteBuffer.wrap( new byte[1] );
		LinkedList<Integer> freeIdList = new LinkedList<Integer>();
		int highId = -1;
		for ( int i = 0; i * recordSize < fileSize; i++ )
		{
			fileChannel.position( i * recordSize );
			fileChannel.read( byteBuffer );
			byteBuffer.flip();
			byte inUse = byteBuffer.get();
			byteBuffer.flip();
			nextId();
			if ( inUse == RECORD_NOT_IN_USE )
			{
				freeIdList.add( i );
			}
			else
			{
				highId = i;
				while ( !freeIdList.isEmpty() )
				{
					freeId( freeIdList.removeFirst() );
					defragedCount++;
				}
			}
//			if ( dot != 0 && i % dot == 0 )
//			{
//				System.out.print( "." );
//			}
		}
		setHighId( highId + 1 );
		logger.info( "[" + getStorageFileName() + "] high id=" + getHighId() + 
			" (defraged=" + defragedCount + ")" );  
		closeIdGenerator();
		openIdGenerator();
	}
}
