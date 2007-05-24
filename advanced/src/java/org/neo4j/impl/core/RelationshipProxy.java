package org.neo4j.impl.core;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;

class RelationshipProxy implements Relationship
{
	private static NodeManager nm = NodeManager.getManager();

	private int relId = -1;
	
	RelationshipProxy( int relId )
	{
		this.relId = relId;
	}
	
	public long getId()
	{
		return relId;
	}
	
	public void delete()
	{
		nm.getRelForProxy( relId ).delete();
	}
	
	public Node[] getNodes()
	{
		return nm.getRelForProxy( relId ).getNodes();
	}
	
	public Node getOtherNode( Node node )
	{
		return nm.getRelForProxy( relId ).getOtherNode( node );
	}

	public Node getStartNode()
	{
		return nm.getRelForProxy( relId ).getStartNode();
	}
	
	public Node getEndNode()
	{
		return nm.getRelForProxy( relId ).getEndNode();
	}

	public RelationshipType getType()
	{
		return nm.getRelForProxy( relId ).getType();
	}

	public Iterable<String> getPropertyKeys()
	{
		return nm.getRelForProxy( relId ).getPropertyKeys();
	}

	public Iterable<Object> getPropertyValues()
	{
		return nm.getRelForProxy( relId ).getPropertyValues();
	}
	
	public Object getProperty( String key )
	{
		return nm.getRelForProxy( relId ).getProperty( key );
	}
	
	public Object getProperty( String key, Object defaultValue )
	{
		return nm.getRelForProxy( relId ).getProperty( key, defaultValue );
	}
	
	public boolean hasProperty( String key )
	{
		return nm.getRelForProxy( relId ).hasProperty( key );
	}
	
	public void setProperty( String key, Object property )
	{
		 nm.getRelForProxy( relId ).setProperty( key, property );
	}

	public Object removeProperty( String key )
	{
		return nm.getRelForProxy( relId ).removeProperty( key );
	}
	
	public int compareTo( Object rel )
	{
		Relationship r = (Relationship) rel;
		int ourId = (int) this.getId(), theirId = (int) r.getId();
		
		if ( ourId < theirId )
		{
			return -1;
		}
		else if ( ourId > theirId )
		{
			return 1;
		}
		else
		{
			return 0;
		}
	}

	public boolean equals( Object o )
	{
		if ( !(o instanceof Relationship) )
		{
			return false;
		}
		return this.getId() == ((Relationship) o).getId();
		
	}
	
	private volatile int hashCode = 0;

	public int hashCode()
	{
		if ( hashCode == 0 )
		{
			hashCode = 3217 * (int) this.getId();
		}
		return hashCode;
	}
	
	public String toString()
	{
		return "Proxy#" + "Relationship[" + this.getId() + "]";
	}
}
