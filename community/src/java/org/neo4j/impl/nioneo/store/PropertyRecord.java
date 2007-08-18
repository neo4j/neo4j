package org.neo4j.impl.nioneo.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class PropertyRecord extends AbstractRecord
{
	private PropertyType type;
	private int keyIndexId = Record.NO_NEXT_BLOCK.intValue();
	private long propBlock = Record.NO_NEXT_BLOCK.intValue();
	private int prevProp = Record.NO_PREVIOUS_PROPERTY.intValue();
	private int nextProp = Record.NO_NEXT_PROPERTY.intValue();
	private List<DynamicRecord> valueRecords = new ArrayList<DynamicRecord>();
	private boolean isLight = false;
	
	public PropertyRecord( int id )
	{
		super( id );
	}
	
	public void setType( PropertyType type )
	{
		this.type = type;
	}
	
	void setIsLight( boolean status )
	{
		isLight = status;
	}
	
	public boolean isLight()
	{
		return isLight;
	}
	
	public Collection<DynamicRecord> getValueRecords()
	{
		if ( isLight )
		{
			throw new RuntimeException( "light property" );
		}
		return valueRecords;
	}
	
	public void addValueRecord( DynamicRecord record )
	{
		if ( isLight )
		{
			throw new RuntimeException( "light property" );
		}
		valueRecords.add( record );
	}
	
	public PropertyType getType()
	{
		return type;
	}
	
	public int getKeyIndexId()
	{
		return keyIndexId;
	}
	
	public void setKeyIndexId( int keyId )
	{
		this.keyIndexId = keyId;
	}
	
	public long getPropBlock()
	{
		return propBlock;
	}
	
	public void setPropBlock( long propBlock )
	{
		this.propBlock = propBlock;
	}
	
	public int getPrevProp()
	{
		return prevProp;
	}
	
	public void setPrevProp( int prevProp )
	{
		this.prevProp = prevProp;
	}
	
	public int getNextProp()
	{
		return nextProp;
	}
	
	public void setNextProp( int nextProp )
	{
		this.nextProp = nextProp;
	}

	@Override
	public String toString()
	{ 
		StringBuffer buf = new StringBuffer();
		buf.append( "PropertyRecord[" ).append( getId() ).append( 
			"," ).append( inUse() ).append( "," ).append( type ).append( 
			"," ).append( keyIndexId ).append( "," ).append( propBlock ).append( 
			"," ).append( prevProp ).append( "," ).append( nextProp );
//		buf.append( ", Key[" );
//		for ( DynamicRecord record : keyRecords )
//		{
//			buf.append( record );
//		}
		buf.append( ", Value[" );
		for ( DynamicRecord record : valueRecords )
		{
			buf.append( record );
		}
		buf.append( "]]" );
		return buf.toString();
	}
}
