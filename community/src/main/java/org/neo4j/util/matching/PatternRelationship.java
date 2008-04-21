package org.neo4j.util.matching;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.RelationshipType;

public class PatternRelationship
{
	private RelationshipType type;
	private PatternNode firstNode;
	private PatternNode secondNode;
	private boolean optional;
	private boolean isMarked = false;
	
	PatternRelationship( RelationshipType type, PatternNode firstNode,
		PatternNode secondNode )
	{
		this( type, firstNode, secondNode, false );
	}
	
	PatternRelationship( RelationshipType type, PatternNode firstNode, 
		PatternNode secondNode, boolean optional )
	{
		this.type = type;
		this.firstNode = firstNode;
		this.secondNode = secondNode;
		this.optional = optional;
	}
	
	public PatternNode getOtherNode( PatternNode node )
	{
		if ( node == firstNode )
		{
			return secondNode;
		}
		if ( node == secondNode )
		{
			return firstNode;
		}
		throw new RuntimeException( "Node[" + node + 
			"] not in this relationship" );
	}
	
	public PatternNode getFirstNode()
	{
		return firstNode;
	}
	
	public PatternNode getSecondNode()
	{
		return secondNode;
	}
	
	public boolean isOptional()
	{
		return optional;
	}
	
	void mark()
	{
		isMarked = true;
	}
	
	void unMark()
	{
		isMarked = false;
	}
	
	boolean isMarked()
	{
		return isMarked;
	}
	
	public RelationshipType getType()
	{
		return type;
	}
	
	public void disconnect()
	{
		getFirstNode().removeRelationship( this, optional );
		getSecondNode().removeRelationship( this, optional );
		firstNode = null;
		secondNode = null;
	}
	
	public Direction getDirectionFrom( PatternNode fromNode )
    {
	    if ( fromNode.equals( firstNode ) )
	    {
	    	return Direction.OUTGOING;
	    }
	    if ( fromNode.equals( secondNode ) )
	    {
	    	return Direction.INCOMING;
	    }
	    throw new RuntimeException( fromNode + " not in " + this );
    }	
	
	@Override
	public String toString()
	{
		return type + ":" + optional;
	}
}
