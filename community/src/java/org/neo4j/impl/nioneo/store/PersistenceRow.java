package org.neo4j.impl.nioneo.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * PersistenRow only encapsulates one record in a store. It is used as a light-
 * weight window when no other (larger window) is found that encapsulates the 
 * required record/block and it would be non efficient to create a large new 
 * window to perform the required operation.
 */
class PersistenceRow extends LockableWindow
{
	private int recordSize = -1;
	private final long position;
	private Buffer buffer = null;
	
	PersistenceRow( long position, int recordSize, FileChannel channel ) 
		throws IOException
	{
		super( channel );
		if ( position < 0 )
		{
			throw new IOException( "Illegal position[" + position + "]" );
		}
		if ( recordSize <= 0 )
		{
			throw new IOException( "Illegal recordSize[" + recordSize + "]" );
		}
		if ( channel == null )
		{
			throw new IOException( "Null file channel" );
		}
		this.position = position;
		this.recordSize = recordSize;
		this.buffer = new Buffer( this );
		goToPosition();
	}
	
	public Buffer getBuffer()
	{
		return buffer;
	}
	
	public long position()
	{
		return position;
	}
	
	private void goToPosition() throws IOException
	{
		long fileSize = getFileChannel().size();
		int recordCount = ( int ) (fileSize / recordSize);
		this.buffer.setByteBuffer( ByteBuffer.allocate( recordSize ) );
		if ( position > recordCount )
		{
			// use new buffer since it will contain only zeros
			return;
		}
		ByteBuffer byteBuffer = buffer.getBuffer();
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
	
	public int hashCode()
	{
		return (int) this.position;
	}
	
	public String toString()
	{
		return "PersistenceRow[" + position + "]";
	}
}