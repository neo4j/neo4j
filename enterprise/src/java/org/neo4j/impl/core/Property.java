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

class Property
{
	private static NodeManager nodeManager = NodeManager.getManager();
	
	private final int id;
	private Object value = null;
	
	Property( int id, Object value )
	{
		this.id = id;
		this.value = value;
	}
	
	int getId()
	{
		return id;
	}
	
	synchronized Object getValue()
	{
		if ( value == null )
		{
			value = nodeManager.loadPropertyValue( id );
		}
		return value;
	}
	
	void setNewValue( Object newValue )
	{
		this.value = newValue;
	}
}
