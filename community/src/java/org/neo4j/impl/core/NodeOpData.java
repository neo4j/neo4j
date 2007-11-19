/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
