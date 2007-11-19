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

class RelationshipOpData implements RelationshipOperationEventData
{
	private final RelationshipImpl rel;
	private int startNode;
	private int endNode;
	private int type;
	private final int id;
	private int propertyId;
	private PropertyIndex index;
	private Object value;
	
	RelationshipOpData( RelationshipImpl rel, int id, int type, int startNode, 
		int endNode )
	{
		this.rel = rel;
		this.id = id;
		this.type = type;
		this.startNode = startNode;
		this.endNode = endNode;
	}
	
	RelationshipOpData( RelationshipImpl rel, int relId, int propertyId )
    {
		this.rel = rel;
	    this.id = relId;
	    this.propertyId = propertyId;
    }
	
	RelationshipOpData( RelationshipImpl rel, int relId, int propertyId, 
		PropertyIndex index, Object value )
    {
		this.rel = rel;
	    this.id = relId;
	    this.propertyId = propertyId;
	    this.index = index;
	    this.value = value;
    }
	
	public Object getEntity()
	{
		return rel;
	}
	
	public int getEndNodeId()
	{
		return endNode;
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

	public int getRelationshipId()
	{
		return id;
	}

	public int getStartNodeId()
	{
		return startNode;
	}

	public int getTypeId()
	{
		return type;
	}
}
