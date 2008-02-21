package org.neo4j.impl.shell.apps;

import java.util.ArrayList;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;

/**
 * This is a wrapper class for a Node or a Relationship, since some apps
 * deals with a nodes or a relationships properties and shouldn't need to
 * duplicate that code. Please make this interface follow the Node/Relationship
 * interfaces.
 */
abstract class NodeOrRelationship
{
	static NodeOrRelationship wrap( Node node )
	{
		return new WrapNode( node );
	}
	
	static NodeOrRelationship wrap( Relationship rel )
	{
		return new WrapRelationship( rel );
	}
	
	private Object nodeOrRelationship;
	
	private NodeOrRelationship( Object nodeOrRelationship )
	{
		this.nodeOrRelationship = nodeOrRelationship;
	}
	
	public boolean isNode()
	{
		return nodeOrRelationship instanceof Node;
	}
	
	public Node asNode()
	{
		return ( Node ) nodeOrRelationship;
	}
	
	public boolean isRelationship()
	{
		return nodeOrRelationship instanceof Relationship;
	}
	
	public Relationship asRelationship()
	{
		return ( Relationship ) nodeOrRelationship;
	}
	
	public abstract long getId();

	public abstract boolean hasProperty( String key );

	public abstract Object getProperty( String key );

	public abstract Object getProperty( String key, Object defaultValue );

	public abstract void setProperty( String key, Object value );

	public abstract Object removeProperty( String key );

	public abstract Iterable<String> getPropertyKeys();

	public abstract Iterable<Object> getPropertyValues();
	
	public abstract Iterable<Relationship> getRelationships(
		Direction direction );
	
	static class WrapNode extends NodeOrRelationship
	{
		private WrapNode( Node node )
		{
			super( node );
		}
		
		private Node object()
		{
			return asNode();
		}
		
		@Override
		public long getId()
		{
			return object().getId();
		}

		@Override
		public boolean hasProperty( String key )
		{
			return object().hasProperty( key );
		}

		@Override
		public Object getProperty( String key )
		{
			return object().getProperty( key );
		}

		@Override
		public Object getProperty( String key, Object defaultValue )
		{
			return object().getProperty( key, defaultValue );
		}

		@Override
		public void setProperty( String key, Object value )
		{
			object().setProperty( key, value );
		}

		@Override
		public Object removeProperty( String key )
		{
			return object().removeProperty( key );
		}

		@Override
		public Iterable<String> getPropertyKeys()
		{
			return object().getPropertyKeys();
		}

		@Override
		public Iterable<Object> getPropertyValues()
		{
			return object().getPropertyValues();
		}
		
		@Override
		public Iterable<Relationship> getRelationships( Direction direction )
		{
			return object().getRelationships( direction );
		}
	}

	static class WrapRelationship extends NodeOrRelationship
	{
		private WrapRelationship( Relationship rel )
		{
			super( rel );
		}
		
		private Relationship object()
		{
			return asRelationship();
		}
		
		@Override
		public long getId()
		{
			return object().getId();
		}

		@Override
		public boolean hasProperty( String key )
		{
			return object().hasProperty( key );
		}

		@Override
		public Object getProperty( String key )
		{
			return object().getProperty( key );
		}

		@Override
		public Object getProperty( String key, Object defaultValue )
		{
			return object().getProperty( key, defaultValue );
		}

		@Override
		public void setProperty( String key, Object value )
		{
			object().setProperty( key, value );
		}

		@Override
		public Object removeProperty( String key )
		{
			return object().removeProperty( key );
		}

		@Override
		public Iterable<String> getPropertyKeys()
		{
			return object().getPropertyKeys();
		}

		@Override
		public Iterable<Object> getPropertyValues()
		{
			return object().getPropertyValues();
		}
		
		@Override
		public Iterable<Relationship> getRelationships( Direction direction )
		{
			return new ArrayList<Relationship>();
		}
	}
}
