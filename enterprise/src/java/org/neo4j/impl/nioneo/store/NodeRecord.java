package org.neo4j.impl.nioneo.store;


public class NodeRecord extends AbstractRecord
{
	private int nextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
	private int nextProp = Record.NO_NEXT_PROPERTY.intValue();
	
	public NodeRecord( int id )
	{
		super( id );
	}
	
	public int getNextRel()
	{
		return nextRel;
	}
	
	public void setNextRel( int nextRel )
	{
		this.nextRel = nextRel;
	}
	
	public int getNextProp()
	{
		return nextProp;
	}
	
	public void setNextProp( int nextProp )
	{
		this.nextProp = nextProp;
	}
	
	public String toString()
	{
		StringBuffer buf = new StringBuffer();
		buf.append( "NodeRecord[" ).append( getId() ).append( "," ).append( 
			inUse() ).append( "," ).append( nextRel ).append( "," ).append(
			nextProp ).append( "]" );
		return buf.toString();
	}
}
