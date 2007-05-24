package org.neo4j.impl.nioneo.store;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class PropertyRecord
{
	private int id;
	private PropertyType type;
	private boolean inUse = false;
	private int keyBlock = Record.NO_NEXT_BLOCK.intValue();
	private long propBlock = Record.NO_NEXT_BLOCK.intValue();
	private int prevProp = Record.NO_PREVIOUS_PROPERTY.intValue();
	private int nextProp = Record.NO_NEXT_PROPERTY.intValue();
	private Map<Integer,DynamicRecord> keyRecords = 
		new HashMap<Integer,DynamicRecord>();
	private Map<Integer,DynamicRecord> valueRecords = 
		new HashMap<Integer,DynamicRecord>();
	
	public PropertyRecord( int id, PropertyType type )
	{
		this.id = id;
		this.type = type;
	}
	
	public DynamicRecord getKeyRecord( int blockId )
	{
		return keyRecords.get( blockId ); 
	}
	
	public Collection<DynamicRecord> getKeyRecords()
	{
		return keyRecords.values();
	}
	
	public DynamicRecord getValueRecord( int blockId )
	{
		return valueRecords.get( blockId );
	}
	
	public Collection<DynamicRecord> getValueRecords()
	{
		return valueRecords.values();
	}
	
	public void addKeyRecord( DynamicRecord record )
	{
		keyRecords.put( record.getId(), record );
	}
	
	public void addValueRecord( DynamicRecord record )
	{
		valueRecords.put( record.getId(), record );
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

	public PropertyType getType()
	{
		return type;
	}
	
	public int getKeyBlock()
	{
		return keyBlock;
	}
	
	public void setKeyBlock( int keyBlock )
	{
		this.keyBlock = keyBlock;
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

	public void clearValueRecords()
	{
		int nextValueId = ( int ) propBlock;
		while ( nextValueId != Record.NO_NEXT_BLOCK.intValue() ) 
		{
			DynamicRecord record = valueRecords.get( nextValueId );
			record.setInUse( false );
			nextValueId = record.getNextBlock();
		}
	}

	public void clearKeyRecords()
	{
		int nextBlockId = ( int ) keyBlock;
		while ( nextBlockId != Record.NO_NEXT_BLOCK.intValue() ) 
		{
			DynamicRecord record = keyRecords.get( nextBlockId );
			record.setInUse( false );
			nextBlockId = record.getNextBlock();
		}
	}
	
	@Override
	public String toString()
	{ 
		StringBuffer buf = new StringBuffer();
		buf.append( "PropertyRecord[" ).append( id ).append( 
			"," ).append( inUse ).append( "," ).append( type ).append( 
			"," ).append( keyBlock ).append( "," ).append( propBlock ).append( 
			"," ).append( prevProp ).append( "," ).append( nextProp );
		buf.append( ", Key[" );
		for ( DynamicRecord record : keyRecords.values() )
		{
			buf.append( record );
		}
		buf.append( "] Value[" );
		for ( DynamicRecord record : valueRecords.values() )
		{
			buf.append( record );
		}
		buf.append( "]]" );
		return buf.toString();
	}
}
