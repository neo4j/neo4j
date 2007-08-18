package org.neo4j.impl.nioneo.store;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RelationshipTypeRecord extends AbstractRecord
{
	private int typeBlock = Record.NO_NEXT_BLOCK.intValue();
	private Map<Integer,DynamicRecord> typeRecords = 
		new HashMap<Integer,DynamicRecord>();
	
	public RelationshipTypeRecord( int id )
	{
		super( id );
	}
	
	public DynamicRecord getTypeRecord( int blockId )
	{
		return typeRecords.get( blockId ); 
	}
	
	public void addTypeRecord( DynamicRecord record )
	{
		typeRecords.put( record.getId(), record );
	}
	
	public int getTypeBlock()
	{
		return typeBlock;
	}
	
	public void setTypeBlock( int typeBlock )
	{
		this.typeBlock = typeBlock;
	}
	
	public Collection<DynamicRecord> getTypeRecords()
	{
		return typeRecords.values();
	}
	
	@Override
	public String toString()
	{
		StringBuffer buf = new StringBuffer();
		buf.append( "RelationshipTypeRecord[" ).append( getId() ).append( 
			"," ).append( inUse() ).append( "," ).append( typeBlock );
		buf.append( ", blocks[" );
		for ( DynamicRecord record : typeRecords.values() )
		{
			buf.append( record );
		}
		buf.append( "]]" );
		return buf.toString();
	}
}
