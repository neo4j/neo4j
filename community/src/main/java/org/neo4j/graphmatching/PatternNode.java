package org.neo4j.graphmatching;

import java.util.LinkedList;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

public class PatternNode extends AbstractPatternObject<Node>
{
    public static final PatternGroup DEFAULT_PATTERN_GROUP = new PatternGroup();
    
	private LinkedList<PatternRelationship> relationships = 
		new LinkedList<PatternRelationship>();
	private LinkedList<PatternRelationship> optionalRelationships =
		new LinkedList<PatternRelationship>();
	 
	private final String label;
	private final PatternGroup group;
	
	public PatternNode()
	{
	    this( DEFAULT_PATTERN_GROUP, "" );
	}
	
	public PatternNode( String label )
	{
	    this( DEFAULT_PATTERN_GROUP, label );
	}
	
	public PatternNode( PatternGroup group )
	{
	    this( group, "" );
	}
	
	public PatternNode( PatternGroup group, String label )
	{
	    this.group = group;
	    this.label = label;
	}
	
	public PatternGroup getGroup()
	{
	    return this.group;
	}

	public Iterable<PatternRelationship> getAllRelationships()
	{
		LinkedList<PatternRelationship> allRelationships =
			new LinkedList<PatternRelationship>();
		allRelationships.addAll( relationships );
		allRelationships.addAll( optionalRelationships );
		
		return allRelationships;
	}
	
	public Iterable<PatternRelationship> getRelationships( boolean optional )
	{
		return optional ? optionalRelationships : relationships;
	}
	
	void addRelationship( PatternRelationship relationship, boolean optional )
	{
		if ( optional )
		{
			optionalRelationships.add( relationship );
		}
		else
		{
			relationships.add( relationship );
		}
	}
	
	void removeRelationship(
		PatternRelationship relationship, boolean optional )
	{
		if ( optional )
		{
			optionalRelationships.remove( relationship );
		}
		else
		{
			relationships.remove( relationship );
		}
	}
	
    public PatternRelationship createRelationshipTo(
        PatternNode otherNode )
    {
        return this.createRelationshipTo( otherNode, false, true );
    }
    
    public PatternRelationship createRelationshipTo(
            PatternNode otherNode, Direction dir )
    {
        return this.createRelationshipTo( otherNode, false, 
                dir == Direction.BOTH ? false : true );
    }
    
	public PatternRelationship createRelationshipTo(
		PatternNode otherNode, RelationshipType type )
	{
		return this.createRelationshipTo( otherNode, type, false, true );
	}
	
    public PatternRelationship createRelationshipTo(
        PatternNode otherNode, RelationshipType type, Direction dir )
    {
        return this.createRelationshipTo( otherNode, type, false, 
                dir == Direction.BOTH ? false : true );
    }
    
    public PatternRelationship createOptionalRelationshipTo(
        PatternNode otherNode )
    {
        return this.createRelationshipTo( otherNode, true, true );
    }
    
    public PatternRelationship createOptionalRelationshipTo(
            PatternNode otherNode, Direction dir )
    {
        return this.createRelationshipTo( otherNode, true, 
                dir == Direction.BOTH ? false : true );
    }
    
    public PatternRelationship createOptionalRelationshipTo(
        PatternNode otherNode, RelationshipType type )
    {
        return this.createRelationshipTo( otherNode, type, true, true );
    }
    
    public PatternRelationship createOptionalRelationshipTo(
        PatternNode otherNode, RelationshipType type, Direction dir )
    {
        return this.createRelationshipTo( otherNode, type, true, 
                dir == Direction.BOTH ? false : true );
    }
    PatternRelationship createRelationshipTo( PatternNode otherNode, 
            boolean optional, boolean directed )
    {
        PatternRelationship relationship = 
            new PatternRelationship( this, otherNode, optional, directed );
        addRelationship( relationship, optional );
        otherNode.addRelationship( relationship, optional );
        return relationship;
    }
    
    PatternRelationship createRelationshipTo( 
            PatternNode otherNode, RelationshipType type, boolean optional, 
            boolean directed )
    {
        PatternRelationship relationship = 
            new PatternRelationship( type, this, otherNode, optional, directed );
        addRelationship( relationship, optional );
        otherNode.addRelationship( relationship, optional );
        return relationship;
    }
    
	public String getLabel()
	{
		return this.label;
	}
	
	@Override
	public String toString()
	{
		return this.label;
	}
}