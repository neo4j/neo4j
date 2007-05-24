package org.neo4j.impl.nioneo.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * PersistenRow only encapsulates one record in a store. It is used as a light-
 * weight window when no other (larger window) is found that encapsulates the 
 * required record/block and it would be unefficient to create a large new 
 * window to perform the required operation.
 */
class PersistenceRow extends LockableWindow
{
	private int recordSize = -1;
	private int position = -1;
	private Buffer buffer = null;
	
	PersistenceRow( int recordSize, FileChannel channel ) 
		throws IOException
	{
		super( channel );
		if ( recordSize <= 0 )
		{
			throw new IOException( "Illegal recordSize[" + recordSize + "]" );
		}
		if ( channel == null )
		{
			throw new IOException( "Null file channel" );
		}
		this.recordSize = recordSize;
		this.buffer = new Buffer( this );
		this.buffer.setByteBuffer( ByteBuffer.allocate( recordSize ) );
	}
	
	public Buffer getBuffer()
	{
		return buffer;
	}
	
	public int position()
	{
		return position;
	}
	
	void position( int id ) throws IOException
	{
		long fileSize = getFileChannel().size();
		int recordCount = ( int ) fileSize / recordSize;
		if ( id < 0 )
		{
			throw new IOException( "Illegal position[" + id + "]" );
		}
		if ( position == id )
		{
			return;
		}
		ByteBuffer byteBuffer = buffer.getBuffer();
		position = id;
		if ( id >= recordCount )
		{
			// get a new buffer since it will contain only zeros
			this.buffer.setByteBuffer( ByteBuffer.allocate( recordSize ) );
			return;
		}
		byteBuffer.clear();
		getFileChannel().read( byteBuffer, position * recordSize );
		byteBuffer.rewind();
	}
	
	void writeOut() throws IOException
	{
		if ( getOperationType() == OperationType.WRITE )
		{
			ByteBuffer byteBuffer = buffer.getBuffer();
			byteBuffer.clear();
			getFileChannel().write( byteBuffer, position * recordSize );
		}
	}
	
	public int size()
	{
		return 1;
	}
	
	public boolean equals( Object o )
	{
		if ( !( o instanceof PersistenceRow ) )
		{
			return false;
		}
		return position() == ( ( PersistenceRow ) o ).position();
	}
	
	private volatile int hashCode = 0;

	public int hashCode()
	{
		if ( hashCode == 0 )
		{
			hashCode = 3217 * position();
		}
		return hashCode;
	}
	
	public String toString()
	{
		return "PersistenceRow[" + position + "]";
	}
}