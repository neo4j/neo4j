package org.neo4j.impl.nioneo.store;

public class DynamicRecord
{
	private int id;
	private boolean inUse = false;
	private byte[] data;
	private int prevBlock = Record.NO_PREV_BLOCK.intValue();
	private int nextBlock = Record.NO_NEXT_BLOCK.intValue();
	
	public DynamicRecord( int id )
	{
		this.id = id;
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
	
	public void setData( byte[] data )
	{
		this.data = data;
	}
	
	public int getLength()
	{
		return data.length;
	}
	
	public byte[] getData()
	{
		return data;
	}
	
	public int getPrevBlock()
	{
		return prevBlock;
	}
	
	public void setPrevBlock( int prevBlock )
	{
		this.prevBlock = prevBlock;
	}
	
	public int getNextBlock()
	{
		return nextBlock;
	}
	
	public void setNextBlock( int nextBlock )
	{
		this.nextBlock = nextBlock;
	}
	
	@Override
	public String toString()
	{
		StringBuffer buf = new StringBuffer();
		buf.append( "DynamicRecord[" ).append( id ).append( "," ).append( 
			inUse ).append( "," ).append( prevBlock ).append( "," ).append( 
			data.length ).append( "," ).append( nextBlock ).append( "]" );
		return buf.toString();
	}
}
