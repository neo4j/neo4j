package org.neo4j.graphmatching;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

public class PatternNode
{
    public static final PatternGroup DEFAULT_PATTERN_GROUP = new PatternGroup();
    
	private LinkedList<PatternRelationship> relationships = 
		new LinkedList<PatternRelationship>();
	private LinkedList<PatternRelationship> optionalRelationships =
		new LinkedList<PatternRelationship>();
	 
	private Set<String> propertiesExist = new HashSet<String>();
	private Map<String, Object[]> propertiesEqual =
		new HashMap<String, Object[]>();
	private final String label;
	private final PatternGroup group;
	
	private Node associatedNode = null; 
	
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
	
	public String getLabel()
	{
		return this.label;
	}
	
	@Override
	public String toString()
	{
		return this.label;
	}
	
	public Set<String> getPropertiesExist()
	{
		return this.propertiesExist;
	}
	
	public Set<String> getPropertiesEqual()
	{
		return this.propertiesEqual.keySet();
	}
	
	public Object[] getPropertyValue( String propertyName )
	{
		return this.propertiesEqual.get( propertyName );
	}
	
	public void setAssociation( Node node )
	{
	    associatedNode = node;
	}

	public Node getAssociation()
	{
	    return associatedNode;
	}
}