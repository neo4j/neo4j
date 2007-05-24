package org.neo4j.impl.nioneo.store;

public class RelationshipRecord
{
	private int id;
	private int firstNode;
	private int secondNode;
	private int type;
	private boolean inUse = false;
	private int firstPrevRel = Record.NO_PREV_RELATIONSHIP.intValue();
	private int firstNextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
	private int secondPrevRel = Record.NO_PREV_RELATIONSHIP.intValue();
	private int secondNextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
	private int nextProp = Record.NO_NEXT_PROPERTY.intValue();
	
	public RelationshipRecord( int id, int firstNode, 
		int secondNode, int type )
	{
		this.id = id;
		this.firstNode = firstNode;
		this.secondNode = secondNode;
		this.type = type;
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
	
	public int getFirstNode()
	{
		return firstNode;
	}
	
	public int getSecondNode()
	{
		return secondNode;
	}
	
	public int getType()
	{
		return type;
	}
	
	public int getFirstPrevRel()
	{
		return firstPrevRel;
	}
	
	public void setFirstPrevRel( int firstPrevRel )
	{
		this.firstPrevRel = firstPrevRel;
	}

	public int getFirstNextRel()
	{
		return firstNextRel;
	}
	
	public void setFirstNextRel( int firstNextRel )
	{
		this.firstNextRel = firstNextRel;
	}
	
	public int getSecondPrevRel()
	{
		return secondPrevRel;
	}
	
	public void setSecondPrevRel( int secondPrevRel )
	{
		this.secondPrevRel = secondPrevRel;
	}
	
	public int getSecondNextRel()
	{
		return secondNextRel;
	}
	
	public void setSecondNextRel( int secondNextRel )
	{
		this.secondNextRel = secondNextRel;
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
		buf.append( "RelationshipRecord[" ).append( id ).append( "," ).append( 
			inUse ).append( "," ).append( "," ).append( 
			firstNode ).append( "," ).append( secondNode ).append( 
			"," ).append( type ).append( "," ).append( firstPrevRel ).append( 
			",").append( firstNextRel ).append( "," ).append( 
			secondPrevRel ).append( "," ).append( secondNextRel ).append( 
			"," ).append(  nextProp ).append( "]" );
		return buf.toString();
	}
}
