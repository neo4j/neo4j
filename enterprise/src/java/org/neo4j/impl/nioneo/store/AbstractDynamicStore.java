package org.neo4j.impl.nioneo.store;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * An abstract representation of a dynamic store. The difference between a  
 * normal {@link AbstractStore} and a <CODE>AbstractDynamicStore</CODE> is that 
 * the size of a record/entry can be dynamic.
 * <p>
 * Instead of a fixed record this class uses blocks to store a record. If a 
 * record size is greater than the block size the record will use one or more 
 * blocks to store its data. 
 * <p>
 * A dynamic store don't have a {@link IdGenerator} because the position of a
 * record can't be calculated just by knowing the id. 
 * Instead one should use a {@link AbstractStore} and store the start  
 * block of the record located in the dynamic store.
 * Note: This class makes use of an id generator internaly for managing 
 * free and non free blocks.    
 * <p>
 * Note, the first block of a dynamic store is reservered and contains 
 * information about the store. 
 */
public abstract class AbstractDynamicStore extends CommonAbstractStore
{
	/**
	 * Creates a new empty store. A facotry method returning an 
	 * implementation should make use of this method to initialize an empty 
	 * store. Block size must be greater than zero. Not that the first block 
	 * will be marked as reserved (contains info about the block size). There
	 * will be an overhead for each block of <CODE>13</CODE> bytes.
	 * <p>
	 * This method will create a empty store with descriptor 
	 * returned by the {@link #getTypeAndVersionDescriptor()}. The internal 
	 * id generator used by this store will also be created. 
	 *
	 * @param fileName The file name of the store that will be created
	 * @param blockSize The number of bytes for each block
	 * @param typeAndVersionDescriptor The type and version descriptor that 
	 * identifies this store
	 * 
	 * @throws IOException If fileName is null or if file exists or illegal
	 * block size
	 */
	protected static void createEmptyStore( String fileName, int blockSize,  
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
		if ( blockSize < 1 )
		{
			throw new IOException( "Illegal block size[" + blockSize + "]" );
		}
		blockSize += 13; // in_use(1)+length(4)+prev_block(4)+next_block(4)
		// write the header
		FileChannel channel = new FileOutputStream( fileName ).getChannel();
		int endHeaderSize = blockSize + typeAndVersionDescriptor.length();
		ByteBuffer buffer = ByteBuffer.allocate( endHeaderSize );
		buffer.putInt( blockSize );
		buffer.position( endHeaderSize - typeAndVersionDescriptor.length() );
		buffer.put( typeAndVersionDescriptor.getBytes() ).flip();
		channel.write( buffer );
		channel.force( false );
		channel.close();
		IdGenerator.createGenerator( fileName + ".id" );
		IdGenerator idGenerator = new IdGenerator( fileName + ".id", 1 );
		idGenerator.nextId(); // reserv first for blockSize
		idGenerator.close(); 
	}
	
	private int blockSize;
	
	public AbstractDynamicStore( String fileName, Map config ) 
		throws IOException
	{
		super( fileName, config );
	}

	public AbstractDynamicStore( String fileName ) 
		throws IOException
	{
		super( fileName );
	}

	/**
	 * Loads this store validating version and id generator. Also the block
	 * size is loaded (contained in first block)
	 */
	protected void loadStorage() throws IOException
	{
		long fileSize = getFileChannel().size();
		String expectedVersion = getTypeAndVersionDescriptor();
		byte version[] = new byte[ expectedVersion.length() ];
		ByteBuffer buffer = ByteBuffer.wrap( version );
		getFileChannel().position( fileSize - expectedVersion.length() ); 
		getFileChannel().read( buffer );
		if ( !expectedVersion.equals( new String( version ) ) )
		{
			setStoreNotOk();
		}
		buffer = ByteBuffer.allocate( 4 );
		getFileChannel().position( 0 );
		getFileChannel().read( buffer );
		buffer.flip();
		blockSize = buffer.getInt();
		if ( ( fileSize - expectedVersion.length() ) % blockSize != 0 )
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
			getBlockSize(), getFileChannel(), getMappedMem() ) );
	}

	/**
	 * Returns the byte size of each block for this dynamic store
	 * 
	 * @return The block size of this store
	 */
	public int getBlockSize()
	{
		return blockSize;
	}
	
	/**
	 * Returns next free block.
	 * 
	 * @return The next free block
	 * @throws IOException If capacity exceeded or closed id generator
	 */
	public int nextBlockId() throws IOException
	{
		return nextId();
	}
	
	/**
	 * Makes a previously used block availible again.
	 *
	 * @param blockId The id of the block to free
	 * @throws IOException If id generator closed or illegal block id 
	 */
	public void freeBlockId( int blockId ) throws IOException
	{
		freeId( blockId );
	}

	// in_use(byte)+prev_block(int)+nr_of_bytes(int)+next_block(int) 
	private static final int BLOCK_HEADER_SIZE = 1 + 4 + 4 + 4;

	
	public void updateRecord( DynamicRecord record ) throws IOException
	{
		int blockId = record.getId();
		PersistenceWindow window = acquireWindow( blockId, 
			OperationType.WRITE );
		try
		{
			Buffer buffer = window.getBuffer();
			int offset = ( blockId - buffer.position() ) * getBlockSize();
			buffer.setOffset( offset );
			if ( record.inUse() )
			{
				assert record.getId() != record.getPrevBlock();
				buffer.put( Record.IN_USE.byteValue() ).putInt( 
					record.getPrevBlock() ).putInt( record.getLength() 
						).putInt( record.getNextBlock() );
				buffer.put( record.getData() );
			}
			else
			{
				buffer.put( Record.NOT_IN_USE.byteValue() ).putInt( 
					Record.NO_PREV_BLOCK.intValue() ).putInt( 0 ).putInt( 
						Record.NO_NEXT_BLOCK.intValue() );
				if ( !isInRecoveryMode() )
				{
					freeBlockId( blockId );
				}
			}
		}
		finally
		{
			releaseWindow( window );
		}
	}

	public Collection<DynamicRecord> allocateRecords( int startBlock, 
		byte src[] ) throws IOException
	{
		if ( getFileChannel() == null )
		{
			throw new IOException( "Store closed" );
		}
		List<DynamicRecord> recordList = new LinkedList<DynamicRecord>();
		if ( src == null )
		{
			throw new IOException( "Null source" );
		}
		int nextBlock = startBlock;
		int prevBlock = Record.NO_PREV_BLOCK.intValue();
		int srcOffset = 0;
		int dataSize = getBlockSize() - BLOCK_HEADER_SIZE;
		do
		{
			DynamicRecord record = new DynamicRecord( nextBlock );
			record.setInUse( true );
			assert prevBlock != nextBlock;
			record.setPrevBlock( prevBlock );
			if ( src.length - srcOffset > dataSize )
			{
				byte data[] = new byte[ dataSize ];
				System.arraycopy( src, srcOffset, data, 0, dataSize );
				record.setData( data );
				prevBlock = nextBlock;
				nextBlock = nextBlockId();
				record.setNextBlock( nextBlock );
				srcOffset += dataSize;
			}
			else
			{
				byte data[] = new byte[ src.length - srcOffset ];
				System.arraycopy( src, srcOffset, data, 0, data.length );
				record.setData( data );
				nextBlock = Record.NO_NEXT_BLOCK.intValue();
				record.setNextBlock( nextBlock );
			}
			recordList.add( record );
		} 
		while ( nextBlock != Record.NO_NEXT_BLOCK.intValue() );
		return recordList;
	}
	
	public Collection<DynamicRecord> getRecords( int startBlockId ) 
		throws IOException
	{
		List<DynamicRecord> recordList = new LinkedList<DynamicRecord>();
		int blockId = startBlockId;
		PersistenceWindow window = acquireWindow( blockId, OperationType.READ );
		try
		{
			DynamicRecord record = new DynamicRecord( blockId );
			Buffer buffer = window.getBuffer();
			int offset = ( blockId - buffer.position() ) * getBlockSize();
			buffer.setOffset( offset );
			byte inUse = buffer.get();
			if ( inUse != Record.IN_USE.byteValue() )
			{
				throw new IOException( "Not in use [" + inUse + 
					"] blockId[" + blockId + "]" );
			}
			record.setInUse( true );
			int prevBlock = buffer.getInt();
			if ( prevBlock != Record.NO_PREV_BLOCK.intValue() )
			{
				throw new IOException( "Start block has previous block set" );
			}
			record.setPrevBlock( prevBlock );
			int nextBlock = blockId;
			int dataSize = getBlockSize() - BLOCK_HEADER_SIZE;
			do
			{
				int nrOfBytes = buffer.getInt();
				prevBlock = nextBlock;
				nextBlock = buffer.getInt();
				if ( nextBlock != Record.NO_NEXT_BLOCK.intValue() && 
						nrOfBytes < dataSize || nrOfBytes > dataSize )
				{
					throw new IOException( "Next block set[" + nextBlock + 
						"] current block illegal size[" +
						nrOfBytes + "/" + dataSize + "]" );
				}
				record.setNextBlock( nextBlock );
				byte byteArrayElement[] = new byte[ nrOfBytes ]; 
				buffer.get( byteArrayElement );
				record.setData( byteArrayElement );
				recordList.add( record );
				if ( nextBlock != Record.NO_NEXT_BLOCK.intValue() ) 
				{
					releaseWindow( window );
					window = acquireWindow( nextBlock, OperationType.READ );
					record = new DynamicRecord( nextBlock );
					buffer = window.getBuffer();
					offset = ( nextBlock - buffer.position() ) * getBlockSize();
					buffer.setOffset( offset );
					inUse = buffer.get();
					if ( inUse != Record.IN_USE.byteValue() )
					{
						throw new IOException( "Next block[" + nextBlock + 
							"] not in use [" + inUse + "]" );
					}
					record.setInUse( true );
					record.setPrevBlock( buffer.getInt() );
					if ( record.getPrevBlock() != prevBlock )
					{
						throw new IOException( "Previous block don't match" );
					}
				}
			} while ( nextBlock != Record.NO_NEXT_BLOCK.intValue() );
		}
		finally
		{
			releaseWindow( window );
		}
		return recordList;
	}
	
	/**
	 * Reads a <CODE>byte array</CODE> stored in this dynamic store using
	 * <CODE>blockId</CODE> as start block. 
	 * 
	 * @param blockId The starting block id
	 * @return The <CODE>byte array</CODE> stored 
	 * @throws IOException If unable to read the data
	 */
	protected byte[] get( int blockId ) throws IOException
	{
		LinkedList<byte[]> byteArrayList = new LinkedList<byte[]>();
		PersistenceWindow window = acquireWindow( blockId, 
			OperationType.READ );
		try
		{
			Buffer buffer = window.getBuffer();
			int offset = ( blockId - buffer.position() ) * getBlockSize();
			buffer.setOffset( offset );
			byte inUse = buffer.get();
			if ( inUse != Record.IN_USE.byteValue() )
			{
				throw new IOException( "Not in use [" + inUse + 
					"] blockId[" + blockId + "]" );
			}
			int prevBlock = buffer.getInt();
			if ( prevBlock != Record.NO_PREV_BLOCK.intValue() )
			{
				throw new IOException( "Start block has previous block set" );
			}
			int nextBlock = blockId;
			int dataSize = getBlockSize() - BLOCK_HEADER_SIZE;
			do
			{
				int nrOfBytes = buffer.getInt();
				prevBlock = nextBlock;
				nextBlock = buffer.getInt();
				if ( nextBlock != Record.NO_NEXT_BLOCK.intValue() && 
						nrOfBytes < dataSize || nrOfBytes > dataSize )
				{
					throw new IOException( "Next block set[" + nextBlock + 
						"] current block illegal size[" +
						nrOfBytes + "/" + dataSize + "]" );
				}
				byte byteArrayElement[] = new byte[ nrOfBytes ]; 
				buffer.get( byteArrayElement );
				byteArrayList.add( byteArrayElement );
				if ( nextBlock != Record.NO_NEXT_BLOCK.intValue() ) 
				{
					releaseWindow( window );
					window = acquireWindow( nextBlock, OperationType.READ );
					buffer = window.getBuffer();
					offset = ( nextBlock - buffer.position() ) * 
						getBlockSize();
					buffer.setOffset( offset );
					inUse = buffer.get();
					if ( inUse != Record.IN_USE.byteValue() )
					{
						throw new IOException( "Next block[" + nextBlock + 
							"] not in use [" + inUse + "]" );
					}
					if ( buffer.getInt() != prevBlock )
					{
						throw new IOException( "Previous block don't match" );
					}
				}
			} while ( nextBlock != Record.NO_NEXT_BLOCK.intValue() );
		}
		finally
		{
			releaseWindow( window );
		}
		int totalSize = 0;
		Iterator<byte[]> itr = byteArrayList.iterator();
		while ( itr.hasNext() )
		{
			totalSize += itr.next().length;
		}
		byte allBytes[] = new byte[ totalSize ];
		itr = byteArrayList.iterator();
		int index = 0;
		while ( itr.hasNext() )
		{
			byte currentArray[] = itr.next();
			System.arraycopy( currentArray, 0, allBytes, index, 
				currentArray.length );
			index += currentArray.length;
		}
		return allBytes;
	}
	
	/**
	 * Rebuilds the internal id generator keeping track of what blocks are 
	 * free or taken.
	 * 
	 * @throws IOException If unable to rebuild the id generator
	 */
	protected void rebuildIdGenerator() throws IOException
	{
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
		nextBlockId(); // reserved first block containing blockSize
		FileChannel fileChannel = getFileChannel();
		long fileSize = fileChannel.size();
		// long dot = fileSize / getBlockSize() / 20;
		long defragedCount = 0;
		ByteBuffer byteBuffer = ByteBuffer.wrap( new byte[1] );
		LinkedList<Integer> freeIdList = new LinkedList<Integer>();
		int highId = 0;
		for ( int i = 1; i * getBlockSize() < fileSize; i++ )
		{
			fileChannel.position( i * getBlockSize() );
			fileChannel.read( byteBuffer );
			byteBuffer.flip();
			byte inUse = byteBuffer.get();
			byteBuffer.flip();
			nextBlockId();
			if ( inUse == Record.NOT_IN_USE.byteValue() )
			{
				freeIdList.add( i );
			}
			else
			{
				highId = i;
				while ( !freeIdList.isEmpty() )
				{
					freeBlockId( freeIdList.removeFirst() );
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
