/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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

//	public void setTransferStartPosition( FileChannel fileChannel, long pos ) 
//	{
//		this.startPosition = pos;
//		this.fromChannel = fileChannel;
//	}
	
//	boolean isTransferable()
//	{
//		return fromChannel != null;
//	}
	
	FileChannel getFromChannel()
	{
		return fromChannel;
	}
	
//	long getTransferStartPosition()
//	{
//		return startPosition;
//	}
	
//	long getTransferCount()
//	{
//		return count;
//	}
//	
//	public void setTransferCount( long count )
//	{
//		this.count = count;
//	}
}
