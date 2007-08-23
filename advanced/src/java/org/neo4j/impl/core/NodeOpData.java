package org.neo4j.impl.core;

class NodeOpData implements NodeOperationEventData
{
	private final int nodeId;
	private int propertyId;
	private PropertyIndex index;
	private Object value;
	private final NodeImpl node;

	NodeOpData( NodeImpl node, int nodeId )
	{
		this.nodeId = nodeId;
		this.node = node;
	}
	
	public Object getEntity()
	{
		return node;
	}
	
	NodeOpData( NodeImpl node, int nodeId, int propertyId )
    {
		this.node = node;
	    this.nodeId = nodeId;
	    this.propertyId = propertyId;
    }
	
	NodeOpData( NodeImpl node, int nodeId, int propertyId, 
		PropertyIndex index, Object value )
    {
		this.node = node;
	    this.nodeId = nodeId;
	    this.propertyId = propertyId;
	    this.index = index;
	    this.value = value;
    }


	public int getNodeId()
	{
		return nodeId;
	}

	public Object getProperty()
	{
		return value;
	}

	public int getPropertyId()
	{
		return propertyId;
	}

	public PropertyIndex getPropertyIndex()
	{
		return index;
	}

	public void setNewPropertyId( int id )
	{
		this.propertyId = id;
	}
}
