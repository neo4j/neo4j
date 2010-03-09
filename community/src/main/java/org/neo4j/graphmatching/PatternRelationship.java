package org.neo4j.graphmatching;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class PatternRelationship
{
	private final RelationshipType type;
    private final boolean directed;
    private final boolean optional;
    private final boolean anyType;
	private final PatternNode firstNode;
	private final PatternNode secondNode;
	
	private boolean isMarked = false;
    private Set<String> propertiesExist = new HashSet<String>();
    private Map<String,Object[]> propertiesEqual = 
        new HashMap<String,Object[]>();
        
    private Relationship associatedRel = null;
    
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

    public void addPropertyExistConstraint( String propertyName )
    {
        this.propertiesExist.add( propertyName );
    }
    
    public void addPropertyEqualConstraint( String propertyName,
        Object... atLeastOneOfTheseValues )
    {
        assert atLeastOneOfTheseValues != null &&
            atLeastOneOfTheseValues.length > 0;
        this.propertiesEqual.put( propertyName, atLeastOneOfTheseValues );
    }
    
    Set<String> getPropertiesExist()
    {
        return this.propertiesExist;
    }
    
    Set<String> getPropertiesEqual()
    {
        return this.propertiesEqual.keySet();
    }
    
    Object[] getPropertyValue( String propertyName )
    {
        return this.propertiesEqual.get( propertyName );
    }

    public void setAssociation( Relationship rel )
    {
        associatedRel = rel;
    }

    public Relationship getAssociation()
    {
        return associatedRel;
    }
}
