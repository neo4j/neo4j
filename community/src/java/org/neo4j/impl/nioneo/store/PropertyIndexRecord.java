package org.neo4j.impl.nioneo.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PropertyIndexRecord extends AbstractRecord
{
	private int propCount = 0;
	private int keyBlockId = Record.NO_NEXT_BLOCK.intValue();
	private List<DynamicRecord> keyRecords = new ArrayList<DynamicRecord>();
	private boolean isLight = false;
	
	public PropertyIndexRecord( int id )
	{
		super( id );
	}
	
	void setIsLight( boolean status )
	{
		isLight = status;
	}
	
	public boolean isLight()
	{
		return isLight;
	}
	
	public int getPropertyCount()
	{
		return propCount;
	}
	
	public void setPropertyCount( int count )
	{
		this.propCount = count;
	}
	
	public int getKeyBlockId()
	{
		return keyBlockId;
	}
	
	public void setKeyBlockId( int blockId )
	{
		this.keyBlockId = blockId;
	}
	
	public Collection<DynamicRecord> getKeyRecords()
	{
		return keyRecords;
	}
	
	public void addKeyRecord( DynamicRecord record )
	{
		keyRecords.add( record );
	}
	
	public String toString()
	{
		StringBuffer buf = new StringBuffer();
		buf.append( "PropertyIndex[" ).append( getId() ).append( "," ).append( 
			inUse() ).append( "," ).append( propCount ).append( "," ).append(
			keyBlockId ).append( "]" );
		return buf.toString();
	}
}
