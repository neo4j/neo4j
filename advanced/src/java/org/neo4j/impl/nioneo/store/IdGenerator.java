package org.neo4j.impl.nioneo.store;

import java.util.LinkedList;
import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * This class generates unique ids for a resource type. For exmaple, nodes 
 * in a nodes space are connected to eachother via relationships. On nodes 
 * and relationship one can add properties. We have three different resource 
 * types here (nodes, relationships and properties) where each resource needs 
 * a unique id to be able to differ resources of the same type from eachother. 
 * Creating three id generators (one for each resource type ) will do the trick.
 * <p>  
 * <CODE>IdGenerator</CODE> makes use of so called "defraged" ids. A 
 * defraged id is an id that has been in use one or many times but the 
 * resource that was using it doesn't exist anymore. This makes it possible to 
 * reuse the id and that in turn makes it possible to write a resource store 
 * with fixed records and size (you can calculate the position of a record by 
 * knowing the id without using indexes or a translation table).
 * <p> 
 * The int value returned by the {@link #nextId} may not be the lowest  
 * avialable id but will be one of the defraged ids if such exist or the next 
 * new free id that has never been used. 
 * <p>
 * The {@link #freeId} will not check if the id passed in to it really is 
 * free. Passing a non free id will corrupt the id generator and 
 * {@link #nextId} method will eventually return that id.
 * <p>
 * The {@link #close()} method must always be invoked when done using an 
 * generator (for this time). Failure to do will render the generator as 
 * "sticky" and unsuable next time you try to initilize a generator using the 
 * same file. Also you can only have one <CODE>IdGenerator</CODE> instance per 
 * id generator file at the same time. 
 * <p>
 * In case of disk/file I/O failure an <CODE>IOException</CODE> is thrown.
 */
public class IdGenerator
{
	// sticky(byte), nextFreeId(int)
	private static final int HEADER_SIZE 		= 5;

	// if sticky the id generator wasn't closed properly so it has to be 
	// rebuilt (go throu the node, relationship, property, rel type etc files)
	private static final byte CLEAN_GENERATOR 	= (byte) 0;
	private static final byte STICKY_GENERATOR 	= (byte) 1;

	//  number of defraged ids to grab form file in batch (also used for write)
	private int grabSize 				= -1;
	private int nextFreeId 				= -1;
	// total bytes read from file, used in writeIdBatch() and close() 
	private long totalBytesRead 		= 0;
	// true if more defragged ids can be read from file
	private boolean haveMore 			= true;
	// marks where this sessions released ids will be written 
	private long readBlocksTo 			= HEADER_SIZE;	
	// used to calculate number of ids actually in use
	private int defragedIdCount = -1;

	private String fileName 			= null;
	private FileChannel fileChannel 	= null; 
	// in memory defraged ids read from file (and from freeId)
	private LinkedList<Integer> defragedIdList = new LinkedList<Integer>();
	// in memory newly free defraged ids that havn't been flushed to disk yet
	private LinkedList<Integer> releasedIdList = new LinkedList<Integer>();
	// buffer used in readIdBatch()
	private ByteBuffer readBuffer = null;
	// buffer used in writeIdBatch() and close()
	private ByteBuffer writeBuffer = null;
	
	
	/**
	 * Opens the id generator represented by <CODE>fileName</CODE>. The  
	 * <CODE>grabSize</CODE> means how many defraged ids we should keep in 
	 * memory and is also the size (x4) of the two buffers used for reading 
	 * and writing to the id generator file. The highest returned id will be 
	 * read from file and if <CODE>grabSize</CODE> number of ids exist they  
	 * will be read into memory (if less exist all defraged ids will be in 
	 * memory).
	 * <p>
	 * If this id generator hasn't been closed properly since the previous 
	 * session (sticky) an <CODE>IOException</CODE> will be thrown. When this 
	 * happens one has to rebuild the id generator from the (node/rel/prop) 
	 * store file.
	 * 
	 * @param fileName The file name (and path if needed) for the id generator 
	 * to be opened
	 * @param grabSize The number of defraged ids to keep in memory
	 * @throws IOException If no such file exist or if the id generator is 
	 * sticky
	 */
	public IdGenerator( String fileName, int grabSize )
		throws IOException
	{
		if ( grabSize < 1 )
		{
			throw new IOException( "Illegal grabSize: " + grabSize );
		}
		this.fileName = fileName;
		this.grabSize = grabSize;
		readBuffer = ByteBuffer.allocate( grabSize * 4 );
		writeBuffer = ByteBuffer.allocate( grabSize * 4 );
		initGenerator();
	}
	
	/**
	 * Returns the next "free" id. If a defraged id exist it will be returned 
	 * else the next free id that hasn't been used yet is returned. If no  
	 * id exist the capacity is exeeded (all int values >= 0 are taken) and a 
	 * <CODE>IOException</CODE> will be thrown.
	 *
	 * @return The next free id
	 * @throws IOException If the capcity is exceeded or closed generator
	 */
	public synchronized int nextId() throws IOException
	{
		if ( fileChannel == null )
		{
			throw new IOException( "Closed id generator" );
		}
		if ( defragedIdList.size() > 0 )
		{
			int id = defragedIdList.removeFirst();
			if ( haveMore && defragedIdList.size() == 0 )
			{
				readIdBatch();
			}
			defragedIdCount--;
			return id;
		}
		if ( nextFreeId < 0 )
		{
			throw new IOException( "Capacity exceeded" );
		}
		return nextFreeId++;
	}
	
	/**
	 * Sets the next free "high" id. This method should be called when 
	 * an id generator has been rebuilt.
	 * 
	 * @param id The next free id
	 */
	synchronized void setHighId( int id )
	{
		nextFreeId = id;
	}
	
	/**
	 * Returns the next "high" id that will be returned if no defraged ids
	 * exist.
	 * 
	 * @return The next free "high" id
	 */
	public synchronized int getHighId()
	{
		return nextFreeId;
	}
	
	/**
	 * Frees the <CODE>id</CODE> making it a defraged id that will be returned 
	 * by next id before any new id (that hasn't been used yet) is returned.
	 * <p>
	 * This method will throw an <CODE>IOException</CODE> if id is negative 
	 * or if id is greater than the highest returned id. 
	 * However as stated in the class documentation above the id isn't 
	 * validated to see if it really is free.
	 *
	 * @param id The id to be made avilable again
	 * @throws IOException If id is negative or greater than the highest 
	 * returned id
	 */
	public synchronized void freeId( int id ) throws IOException
	{
		if ( id < 0 || id >= nextFreeId )
		{
			throw new IOException( "Illegal id[" + id +	"]" );
		}
		releasedIdList.add( id  );
		defragedIdCount++;
		if ( releasedIdList.size() >= grabSize )
		{
			writeIdBatch();
		}
	}
	
	/**
	 * Closes the id generator flushing defraged ids in memory to file. The 
	 * file willl be truncated to the minimal size required to hold all 
	 * defraged ids and it will be marked as clean (not sticky).
	 * <p>
	 * An invoke to the <CODE>nextId</CODE> or <CODE>freeId</CODE> after this 
	 * method has been invoked will result in an <CODE>IOException</CODE> since
	 * the highest returned id has been set to a negative value.
	 *
	 * @throws IOException If unable to close this id generator
	 */
	public synchronized void close() throws IOException
	{
		if ( nextFreeId == -1 )
		{
		 	return;
		}
			
		// write out lists
		if ( releasedIdList.size() > 0 )
		{
			writeIdBatch();
		}
		if ( defragedIdList.size() > 0 )
		{
			while ( defragedIdList.size() > 0 )
			{
				releasedIdList.add( defragedIdList.removeFirst() );
			}
			writeIdBatch();
		}
		
		// write header
		fileChannel.position( 0 );
		ByteBuffer buffer = ByteBuffer.allocate( HEADER_SIZE );
		buffer.put( STICKY_GENERATOR ).putInt( nextFreeId );
		buffer.flip();
		fileChannel.write( buffer );
		// move data to remove fragmentation in file
		if ( totalBytesRead > HEADER_SIZE )
		{
			long writePosition = HEADER_SIZE; 
			long readPosition = readBlocksTo;
			if ( totalBytesRead < readBlocksTo )
			{
				readPosition = totalBytesRead;
			}
			int bytesRead = -1;
			do
			{
				writeBuffer.clear();
				fileChannel.position( readPosition );
				bytesRead = fileChannel.read( writeBuffer );
				readPosition += bytesRead;
				writeBuffer.flip();
				fileChannel.position( writePosition );
				writePosition += fileChannel.write( writeBuffer );
			} while ( bytesRead > 0 );
			// truncate
			fileChannel.truncate( writePosition );
		}
		// flush
		fileChannel.force( false );
		// remove sticky
		buffer.clear();
		buffer.put( CLEAN_GENERATOR );
		buffer.limit( 1 );
		buffer.flip();
		fileChannel.position( 0 );
		fileChannel.write( buffer );
		// flush and close
		fileChannel.force( false );
		fileChannel.close();
		fileChannel = null;
		// make this generator unsuable
		nextFreeId = -1;
	}

	/**
	 * Returns the file associated with this id generator. 
	 *
	 * @return The id generator's file name
	 */
	public String getFileName()
	{
		return this.fileName;
	}
	
	/**
	 * Creates a new id generator.
	 * 
	 * @param fileName The name of the id generator
	 * @throws IOException If unable to create the id generator
	 */
	public static void createGenerator( String fileName ) 
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
			throw new IOException( "Can't create IdGeneratorFile[" + fileName + 
				"], file already exists" );
		}
		FileChannel channel = new FileOutputStream( fileName ).getChannel();
		// write the header
		ByteBuffer buffer = ByteBuffer.allocate( HEADER_SIZE );
		buffer.put( CLEAN_GENERATOR ).putInt( 0 ).flip();
		channel.write( buffer );
		channel.force( false );
	}
	
	// initilize the id generator and performs a simple validation
	private void initGenerator() 
		throws IOException
	{
		fileChannel = new RandomAccessFile( fileName, "rw" ).getChannel();
		ByteBuffer buffer = ByteBuffer.allocate( HEADER_SIZE );
		totalBytesRead = fileChannel.read( buffer );
		if ( totalBytesRead != HEADER_SIZE )
		{
			fileChannel.close();
			throw new IOException( "Unable to read header, bytes read: " +
				totalBytesRead );
		}
		buffer.flip();
		byte storageStatus = buffer.get(); 
		if ( storageStatus != CLEAN_GENERATOR )
		{
			fileChannel.close();
			throw new IOException( "Sticky generator[ " + fileName + "]" + 
				"delete this id generator and build a new one" ); 
		}
		this.nextFreeId = buffer.getInt();
		buffer.flip();
		buffer.put( STICKY_GENERATOR ).limit( 1 ).flip();
		fileChannel.position( 0 );
		fileChannel.write( buffer );
		fileChannel.position( HEADER_SIZE );
		readBlocksTo = fileChannel.size();
		defragedIdCount = (int) ( readBlocksTo - HEADER_SIZE ) / 4;
		readIdBatch();
	}
	
	private void readIdBatch() throws IOException
	{
		if ( !haveMore )
		{
			return;
		}
		if ( totalBytesRead >= readBlocksTo )
		{
			haveMore = false;
			return;
		}
		if ( totalBytesRead + readBuffer.capacity() > readBlocksTo )
		{
			readBuffer.clear();
			readBuffer.limit( 
				(int) (readBlocksTo - fileChannel.position()) );
		}
		else
		{
			readBuffer.clear();
		}
		fileChannel.position( totalBytesRead );
		int bytesRead = fileChannel.read( readBuffer );
		assert fileChannel.position() <= readBlocksTo;
		totalBytesRead += bytesRead;
		readBuffer.flip();
		int defragedIdCount = bytesRead / 4;
		if ( bytesRead % 4 != 0 )
		{
			throw new RuntimeException( "azzert" );
		}
		for ( int i = 0; i < defragedIdCount; i++ )
		{
			int id = readBuffer.getInt();
			defragedIdList.add( id );
		}
	}
	
	// writes a batch of defraged ids to file
	private void writeIdBatch() throws IOException
	{
		// position at end
		fileChannel.position( fileChannel.size() );
		writeBuffer.clear();
		while ( releasedIdList.size() > 0 )
		{
			writeBuffer.putInt( releasedIdList.removeFirst() );
			if ( writeBuffer.position() == writeBuffer.capacity() )
			{
				writeBuffer.flip();
				fileChannel.write( writeBuffer );
				writeBuffer.clear();
			}
		}
		writeBuffer.flip();
		fileChannel.write( writeBuffer );
		// position for next readIdBatch
		fileChannel.position( totalBytesRead );
	}

	/**
	 * Utility method that will dump all defraged id's and the "high id" to 
	 * console. Do not  call while running store using this id generator since
	 * it could corrupt the id generator (not thread safe). This method will 
	 * close the id generator after beeing invoked.
	 * 
	 * @throws IOException If problem dumping free ids
	 */
	public void dumpFreeIds() throws IOException
	{
		while ( haveMore )
		{
			readIdBatch();
		}
		java.util.Iterator itr = defragedIdList.iterator();
		while ( itr.hasNext() )
		{
			System.out.print( " " + itr.next() );
		}
		System.out.println( "\nNext free id: " + nextFreeId );
		close();
	}
	
	public int getNumberOfIdsInUse()
	{
		return nextFreeId - defragedIdCount;
	}
}
