package org.neo4j.impl.nioneo.store;

import java.nio.channels.FileChannel;

public abstract class AbstractRecord
{
	private boolean inUse = false;
	private final int id;
	private boolean created = false;
	private long startPosition = -1;
	private long count = -1;
	private FileChannel fromChannel = null;
	
	AbstractRecord( int id )
	{
		this.id = id;
	}
	
	AbstractRecord( int id, boolean inUse )
	{
		this.id = id;
		this.inUse = inUse;
	}
	
	public int getId()
	{
		return id;
	}
	
	public boolean inUse()
	{
		return inUse;
	}
	
	public void setInUse( boolean inUse )
	{
		this.inUse = inUse;
	}
	
	public void setCreated()
	{
		this.created = true;
	}
	
	public boolean isCreated()
	{
		return created;
	}

	public void setTransferStartPosition( FileChannel fileChannel, long pos ) 
	{
		this.startPosition = pos;
		this.fromChannel = fileChannel;
	}
	
	boolean isTransferable()
	{
		// return false;
		return fromChannel != null;
	}
	
	FileChannel getFromChannel()
	{
		return fromChannel;
	}
	
	long getTransferStartPosition()
	{
		return startPosition;
	}
	
	long getTransferCount()
	{
		return count;
	}
	
	public void setTransferCount( long count )
	{
		this.count = count;
	}
}
