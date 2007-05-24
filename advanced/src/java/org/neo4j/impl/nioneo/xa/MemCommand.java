package org.neo4j.impl.nioneo.xa;

import org.neo4j.impl.nioneo.store.RelationshipData;


/**
 * Memory holders for all the commands that can be performed on a 
 * Neo store.
 */
abstract class MemCommand
{
	abstract int getId();
	
	static class NodeCreate extends MemCommand
	{
		private int nodeId;
		
		NodeCreate( int nodeId )
		{
			this.nodeId = nodeId;
		}
		
		int getId()
		{
			return nodeId;
		}
		
		public boolean equals( Object o )
		{
			if ( !( o instanceof NodeCreate ) )
			{
				return false;
			}
			return nodeId == ( ( NodeCreate ) o ).nodeId;
		}
	
		private volatile int hashCode = 0;

		public int hashCode()
		{
			if ( hashCode == 0 )
			{
				hashCode = 3217 * nodeId;
			}
			return hashCode;
		}
	}
	
	static class NodeDelete extends MemCommand
	{
		private int nodeId;
		
		NodeDelete( int nodeId )
		{
			this.nodeId = nodeId;
		}
		
		int getId()
		{
			return nodeId;
		}

		public boolean equals( Object o )
		{
			if ( !( o instanceof NodeDelete ) )
			{
				return false;
			}
			return nodeId == ( ( NodeDelete ) o ).nodeId;
		}
	
		private volatile int hashCode = 0;

		public int hashCode()
		{
			if ( hashCode == 0 )
			{
				hashCode = 3217 * nodeId;
			}
			return hashCode;
		}
	}

	static class NodeAddProperty extends MemCommand
	{
		private int nodeId;
		private int propertyId;
		private String key;
		private Object value;
		
		NodeAddProperty( int nodeId,  int propertyId, String key, 
			Object value )
		{
			this.nodeId = nodeId;
			this.propertyId = propertyId;
			this.key = key;
			this.value = value;
		}
		
		void setNewValue( Object value )
		{
			this.value = value;
		}
		
		int getNodeId()
		{
			return nodeId;
		}
		
		int getPropertyId()
		{
			return propertyId;
		}

		int getId()
		{
			return propertyId;
		}
		
		String getKey()
		{
			return key;
		}
		
		Object getValue()
		{
			return value;
		}
		
		public boolean equals( Object o )
		{
			if ( !( o instanceof NodeAddProperty ) )
			{
				return false;
			}
			return propertyId == ( ( NodeAddProperty ) o ).propertyId;
		}
	
		private volatile int hashCode = 0;

		public int hashCode()
		{
			if ( hashCode == 0 )
			{
				hashCode = 3217 * propertyId;
			}
			return hashCode;
		}
	}
	
	static class NodeChangeProperty extends MemCommand
	{
		private int nodeId;
		private int propertyId;
		private Object newValue;
		
		NodeChangeProperty( int nodeId, int propertyId, Object newValue )
		{
			this.nodeId = nodeId;
			this.propertyId = propertyId;
			this.newValue = newValue;
		}
		
		void setNewValue( Object value )
		{
			this.newValue = value;
		}

		int getNodeId()
		{
			return nodeId;
		}
		
		int getPropertyId()
		{
			return propertyId;
		}
		
		int getId()
		{
			return propertyId;
		}
		
		Object getValue()
		{
			return newValue;
		}
		
		public boolean equals( Object o )
		{
			if ( !( o instanceof NodeChangeProperty ) )
			{
				return false;
			}
			return propertyId == ( ( NodeChangeProperty ) o ).propertyId;
		}
	
		private volatile int hashCode = 0;

		public int hashCode()
		{
			if ( hashCode == 0 )
			{
				hashCode = 3217 * propertyId;
			}
			return hashCode;
		}
	}
	
	static class NodeRemoveProperty extends MemCommand
	{
		private int nodeId;
		private int propertyId;
		
		NodeRemoveProperty( int nodeId, int propertyId )
		{
			this.nodeId = nodeId;
			this.propertyId = propertyId;
		}
		
		int getNodeId()
		{
			return nodeId;
		}
		
		int getPropertyId()
		{
			return propertyId;
		}

		int getId()
		{
			return propertyId;
		}
		
		public boolean equals( Object o )
		{
			if ( !( o instanceof NodeRemoveProperty ) )
			{
				return false;
			}	
			return propertyId == ( ( NodeRemoveProperty ) o ).propertyId;
		}
	
		private volatile int hashCode = 0;

		public int hashCode()
		{
			if ( hashCode == 0 )
			{
				hashCode = 3217 * propertyId;
			}
			return hashCode;
		}
	}

	static class RelationshipCreate extends MemCommand
	{
		private int relId;
		private int firstNode;
		private int secondNode;
		private int type;
		
		RelationshipCreate( int relId, int firstNode, int secondNode, 
			int type )
		{
			this.relId = relId;
			this.firstNode = firstNode;
			this.secondNode = secondNode;
			this.type = type;
		}
		
		int getFirstNode()
		{
			return firstNode;
		}
		
		int getSecondNode()
		{
			return secondNode;
		}

		int getId()
		{
			return relId;
		}
		
		int getType()
		{
			return type;
		}
		
		public boolean equals( Object o )
		{
			if ( !( o instanceof RelationshipCreate ) )
			{
				return false;
			}
			return relId == ( ( RelationshipCreate ) o ).relId;
		}
	
		private volatile int hashCode = 0;

		public int hashCode()
		{
			if ( hashCode == 0 )
			{
				hashCode = 3217 * relId;
			}
			return hashCode;
		}

		public RelationshipData getRelationshipData()
		{
			return new RelationshipData( relId, firstNode, secondNode, 
				type, -1, -1, -1, -1, -1 );
		}
	}
	
	static class RelationshipDelete extends MemCommand
	{
		private int relId;
		
		RelationshipDelete( int relId )
		{
			this.relId = relId;
		}
		
		int getId()
		{
			return relId;
		}

		public boolean equals( Object o )
		{
			if ( !( o instanceof RelationshipDelete ) )
			{
				return false;
			}
			return relId == ( ( RelationshipDelete ) o ).relId;
		}
	
		private volatile int hashCode = 0;

		public int hashCode()
		{
			if ( hashCode == 0 )
			{
				hashCode = 3217 * relId;
			}
			return hashCode;
		}
	}

	static class RelationshipAddProperty extends MemCommand
	{
		private int relId;
		private int propertyId;
		private String key;
		private Object value;
		
		RelationshipAddProperty( int relId, int propertyId, String key, 
			Object value )
		{
			this.relId = relId;
			this.propertyId = propertyId;
			this.key = key;
			this.value = value;
		}
		
		void setNewValue( Object value )
		{
			this.value = value;
		}
		
		int getRelId()
		{
			return relId;
		}
		
		int getPropertyId()
		{
			return propertyId;
		}
		
		int getId()
		{
			return propertyId;
		}
		
		String getKey()
		{
			return key;
		}
		
		Object getValue()
		{
			return value;
		}

		public boolean equals( Object o )
		{
			if ( !( o instanceof RelationshipAddProperty ) )
			{
				return false;
			}
			return relId == ( ( RelationshipAddProperty ) o ).relId;
		}
	
		private volatile int hashCode = 0;

		public int hashCode()
		{
			if ( hashCode == 0 )
			{
				hashCode = 3217 * relId;
			}
			return hashCode;
		}
	}

	static class RelationshipChangeProperty extends MemCommand
	{
		private int relId;
		private int propertyId;
		private Object value;
		
		RelationshipChangeProperty( int relId, int propertyId, Object value )
		{
			this.relId = relId;
			this.propertyId = propertyId;
			this.value = value;
		}
		
		void setNewValue( Object value )
		{
			this.value = value;
		}

		Object getValue()
		{
			return value;
		}
		
		int getRelId()
		{
			return relId;
		}
		
		int getPropertyId()
		{
			return propertyId;
		}

		int getId()
		{
			return propertyId;
		}
		
		public boolean equals( Object o )
		{
			if ( !( o instanceof RelationshipChangeProperty ) )
			{
				return false;
			}
			return propertyId == 
				( ( RelationshipChangeProperty ) o ).propertyId;
		}
	
		private volatile int hashCode = 0;

		public int hashCode()
		{
			if ( hashCode == 0 )
			{
				hashCode = 3217 * propertyId;
			}
			return hashCode;
		}
	}
	
	static class RelationshipRemoveProperty extends MemCommand
	{
		private int relId;
		private int propertyId;
		
		RelationshipRemoveProperty( int relId, int propertyId )
		{
			this.relId = relId;
			this.propertyId = propertyId;
		}
		
		int getRelId()
		{
			return relId;
		}
		
		int getPropertyId()
		{
			return propertyId;
		}
		
		int getId()
		{
			return propertyId;
		}
		
		public boolean equals( Object o )
		{
			if ( !( o instanceof RelationshipRemoveProperty ) )
			{
				return false;
			}
			return relId == ( ( RelationshipRemoveProperty ) o ).relId;
		}
	
		private volatile int hashCode = 0;

		public int hashCode()
		{
			if ( hashCode == 0 )
			{
				hashCode = 3217 * relId;
			}
			return hashCode;
		}
	}

	static class RelationshipTypeAdd extends MemCommand
	{
		private int id;
		private String name; 
		
		RelationshipTypeAdd( int id, String name )
		{
			this.id = id;
			this.name = name;
		}
		
		int getId()
		{
			return id;
		}
		
		String getName()
		{
			return name;
		}
		
		public boolean equals( Object o )
		{
			if ( !( o instanceof RelationshipTypeAdd ) )
			{
				return false;
			}
			return id == ( ( RelationshipTypeAdd ) o ).id;
		}

		private volatile int hashCode = 0;

		public int hashCode()
		{
			if ( hashCode == 0 )
			{
				hashCode = 3217 * id;
			}
			return hashCode;
		}
	}
}
