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
package org.neo4j.impl.nioneo.store;


/**
 * Wrapper class for the data contained in a property record.
 */
public class PropertyData
{
	private final int id;
	private final int keyIndexId;
	private final Object value;
	
	/**
	 * @param id The id of the property
	 * @param key The key of the property
	 * @param value The value of the property
	 * @param nextPropertyId The next property id in the property chain, 
	 * -1 if last property
	 */
	public PropertyData( int id, int keyIndexId, Object value )
	{
		this.id = id;
		this.keyIndexId = keyIndexId;
		this.value = value;
	}
	
	public int getId()
	{
		return id;
	}
	
	public int getIndex()
	{
		return keyIndexId;
	}
	
	public Object getValue()
	{
		return value;
	}
}

