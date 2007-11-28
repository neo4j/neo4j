package org.neo4j.util.matching;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.neo4j.api.core.RelationshipType;

public class PatternNode
{
	 private LinkedList<PatternRelationship> relationships = 
		new LinkedList<PatternRelationship>();
	 
	 private Set<String> propertiesExist = new HashSet<String>();
	 private Map<String, Object> propertiesEqual =
		 new HashMap<String, Object>();
	 private String label;
	
	public PatternNode()
	{
		this.label = "";
	}
	
	public PatternNode( String label )
	{
		this.label = label;
	}
	
	public Iterable<PatternRelationship> getRelationships()
	{
		return relationships;
	}
	
	void addRelationship( PatternRelationship relationship )
	{
		relationships.add( relationship );
	}
	
	void removeRelationship( PatternRelationship relationship )
	{
		relationships.remove( relationship );
	}
	
	public PatternRelationship createRelationshipTo( 
		PatternNode otherNode, RelationshipType type )
	{
		PatternRelationship relationship = 
			new PatternRelationship( type, this, otherNode );
		addRelationship( relationship );
		otherNode.addRelationship( relationship );
		return relationship;
	}
	
	public void addPropertyExistConstraint( String propertyName )
	{
		this.propertiesExist.add( propertyName );
	}
	
	public void addPropertyEqualConstraint( String propertyName, Object value )
	{
		this.propertiesEqual.put( propertyName, value );
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
	
	Object getPropertyValue( String propertyName )
	{
		return this.propertiesEqual.get( propertyName );
	}
}
