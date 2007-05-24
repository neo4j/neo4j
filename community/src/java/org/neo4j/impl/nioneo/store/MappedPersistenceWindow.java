package org.neo4j.impl.nioneo.store;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Persistence window using a {@link MappedByteBuffer} as underlying buffer.
 */
class MappedPersistenceWindow extends LockableWindow
{
	private int position = -1;
	private Buffer buffer = null;
	private int windowSize = -1;
	private int recordSize = -1;
	private int totalSize = -1;
	
	MappedPersistenceWindow( int position, int recordSize, int totalSize,  
		FileChannel channel ) throws IOException
	{
		super( channel );
		if ( recordSize <= 0 || totalSize < recordSize )
		{
			throw new IOException( "Illegal record/window size: " + 
				recordSize + "/" + totalSize );
		}
		if ( totalSize % recordSize != 0 )
		{
			throw new IOException( "Illegal mod size/window (" + totalSize + 
				"/" + recordSize + ")" );
		}
		this.totalSize = totalSize;
		windowSize = totalSize / recordSize;
		this.recordSize = recordSize;
		this.position = position;
		buffer = new Buffer( this );
		try
		{
			buffer.setByteBuffer( channel.map( FileChannel.MapMode.READ_WRITE, 
				position * recordSize, totalSize ) );
		}
		catch ( IOException e )
		{
			this.position = -1;
			throw new MappedMemException( e );
		}
	}
	
	public Buffer getBuffer()
	{
		return buffer;
	}
	
	public int position()
	{
		return position;
	}
	
	public int size()
	{
		return windowSize;
	}

	void force()
	{
		( ( java.nio.MappedByteBuffer ) buffer.getBuffer() ).force();
	}
	
	public boolean equals( Object o )
	{
		if ( !( o instanceof MappedPersistenceWindow ) )
		{
			return false;
		}
		return position() == ( ( MappedPersistenceWindow ) o ).position();
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
		return "MappedPersistenceWindow[p=" + position + ",rs=" + recordSize + 
			",ws=" + windowSize + ",ts=" + totalSize + "]";
	}
}
