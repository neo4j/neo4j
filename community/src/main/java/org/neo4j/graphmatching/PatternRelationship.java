package org.neo4j.graphmatching;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class PatternRelationship extends AbstractPatternObject<Relationship>
{
	private final RelationshipType type;
    private final boolean directed;
    private final boolean optional;
    private final boolean anyType;
	private final PatternNode firstNode;
	private final PatternNode secondNode;
	
	private boolean isMarked = false;
    
    PatternRelationship( PatternNode firstNode, 
        PatternNode secondNode, boolean optional, boolean directed )
    {
        this.directed = directed;
        this.anyType = true;
        this.firstNode = firstNode;
        this.secondNode = secondNode;
        this.optional = optional;
        this.type = null;
    }
    
	PatternRelationship( RelationshipType type, PatternNode firstNode, 
		PatternNode secondNode, boolean optional, boolean directed )
	{
	    this.directed = directed;
	    this.anyType = false;
		this.type = type;
		this.firstNode = firstNode;
		this.secondNode = secondNode;
		this.optional = optional;
	}
	
    boolean anyRelType()
    {
        return anyType;
    }
    
    boolean isDirected()
    {
        return directed;
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
