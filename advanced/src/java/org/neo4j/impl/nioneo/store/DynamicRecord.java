package org.neo4j.impl.nioneo.store;


public class DynamicRecord extends AbstractRecord
{
	private byte[] data;
	private int length;
	private int prevBlock = Record.NO_PREV_BLOCK.intValue();
	private int nextBlock = Record.NO_NEXT_BLOCK.intValue();
	private boolean isLight = false;
	
	public DynamicRecord( int id )
	{
		super( id );
	}
	
	void setIsLight( boolean status )
	{
		this.isLight = status;
	}
	
	public boolean isLight()
	{
		return isLight;
	}
	
	public void setLength( int length )
	{
		this.length = length;
	}
	
	public void setInUse( boolean inUse )
	{
		super.setInUse( inUse );
		if ( !inUse )
		{
			data = null;
		}
	}
	
	public void setData( byte[] data )
	{
		isLight = false;
		this.length = data.length;
		this.data = data;
	}
	
	public int getLength()
	{
		return length;
	}
	
	public byte[] getData()
	{
		assert !isLight;
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
		buf.append( "DynamicRecord[" ).append( getId() ).append( "," ).append( 
			inUse() );
		if ( inUse() )
		{
			buf.append( "," ).append( prevBlock ).append( "," ).append( 
				isLight ? null : data.length ).append( "," ).append( 
				nextBlock ).append( "]" );
		}
		return buf.toString();
	}
}
