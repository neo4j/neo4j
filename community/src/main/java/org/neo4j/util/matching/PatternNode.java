package org.neo4j.util.matching;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.neo4j.api.core.RelationshipType;

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
        return this.createRelationshipTo( otherNode, false );
    }
    
	public PatternRelationship createRelationshipTo(
		PatternNode otherNode, RelationshipType type )
	{
		return this.createRelationshipTo( otherNode, type, false );
	}
	
    public PatternRelationship createRelationshipTo( 
        PatternNode otherNode, boolean optional )
    {
        PatternRelationship relationship = 
            new PatternRelationship( this, otherNode, optional );
        addRelationship( relationship, optional );
        otherNode.addRelationship( relationship, optional );
        return relationship;
    }
    
	public PatternRelationship createRelationshipTo( 
		PatternNode otherNode, RelationshipType type, boolean optional )
	{
		PatternRelationship relationship = 
			new PatternRelationship( type, this, otherNode, optional );
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
}
