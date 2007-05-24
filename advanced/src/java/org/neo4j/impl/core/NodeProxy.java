package org.neo4j.impl.core;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.Traverser;
import org.neo4j.api.core.Traverser.Order;

class NodeProxy implements Node
{
	private static NodeManager nm = NodeManager.getManager();
	
	private int nodeId = -1;
	
	NodeProxy( int nodeId )
	{
		this.nodeId = nodeId;
	}
		
	public long getId()
	{
		return nodeId;
	}
	
	public void delete()
	{
		nm.getNodeForProxy( nodeId ).delete();
	}

	public Iterable<Relationship> getRelationships()
	{
		return nm.getNodeForProxy( nodeId ).getRelationships();
	}
	
	public Iterable<Relationship> getRelationships( Direction dir )
	{
		return nm.getNodeForProxy( nodeId ).getRelationships( dir );
	}
	
	public Iterable<Relationship> getRelationships( RelationshipType type )
	{
		return nm.getNodeForProxy( nodeId ).getRelationships( type );
	}

	public Iterable<Relationship> getRelationships( RelationshipType... types )
	{
		return nm.getNodeForProxy( nodeId ).getRelationships( types );
	}
	
	public Iterable<Relationship> getRelationships( RelationshipType type, Direction dir )
	{
		return nm.getNodeForProxy( nodeId ).getRelationships( type, dir );
	}

	public Relationship getSingleRelationship( RelationshipType type, 
		Direction dir )
	{
		return nm.getNodeForProxy( nodeId ).getSingleRelationship( type, dir );
	}
	
	public void setProperty( String key, Object value ) 
		throws IllegalValueException
	{
		nm.getNodeForProxy( nodeId ).setProperty( key, value );
	}
	
	public Object removeProperty( String key ) throws NotFoundException
	{
		return nm.getNodeForProxy( nodeId ).removeProperty( key );
	}
	
	public Object getProperty( String key, Object defaultValue )
	{
		return nm.getNodeForProxy( nodeId ).getProperty( key, defaultValue );
	}

	public Iterable<Object> getPropertyValues()
	{
		return nm.getNodeForProxy( nodeId ).getPropertyValues();
	}

	public Iterable<String> getPropertyKeys()
	{
		return nm.getNodeForProxy( nodeId ).getPropertyKeys();
	}
	
	public Object getProperty( String key )
		throws NotFoundException
	{
		return nm.getNodeForProxy( nodeId ).getProperty( key );
	}
	
	public boolean hasProperty( String key )
	{
		return nm.getNodeForProxy( nodeId ).hasProperty( key );
	}
	
	public int compareTo( Object node )
	{
		Node n = (Node) node;
		int ourId = (int) this.getId(), theirId = (int) n.getId();
		
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
		if ( !(o instanceof Node) )
		{
			return false;
		}
		return this.getId() == ((Node) o).getId();
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
		return "Proxy#" + "Node[" + this.getId() + "]";
	}

	public Relationship createRelationshipTo( Node otherNode, 
		RelationshipType type )
	{
		return nm.getNodeForProxy( nodeId ).createRelationshipTo( 
			otherNode, type );
	}

	public Traverser traverse( Order traversalOrder, 
		StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, 
		RelationshipType relationshipType, Direction direction )
	{
		return nm.getNodeForProxy( nodeId ).traverse( traversalOrder, 
			stopEvaluator, returnableEvaluator, relationshipType, direction );
	}

	public Traverser traverse( Order traversalOrder, 
		StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, 
		RelationshipType firstRelationshipType, Direction firstDirection, 
		RelationshipType secondRelationshipType, Direction secondDirection )
	{
		return nm.getNodeForProxy( nodeId ).traverse( traversalOrder, 
			stopEvaluator, returnableEvaluator, firstRelationshipType, 
			firstDirection, secondRelationshipType, secondDirection );
	}

	public Traverser traverse( Order traversalOrder, 
		StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, 
		Object... relationshipTypesAndDirections )
	{
		return nm.getNodeForProxy( nodeId ).traverse( traversalOrder, 
			stopEvaluator, returnableEvaluator, 
			relationshipTypesAndDirections );
	}
}
