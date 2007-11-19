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

import org.neo4j.impl.persistence.PersistenceMetadata;

/**
 * This interface defines methods needed for persistence layer when 
 * incremental changes to a node are made persistent. Nodes generates
 * a pro-active event when changes are made to them and passes event data
 * of this type.
 */
public interface NodeOperationEventData extends PersistenceMetadata
{
	/**
	 * Returns the node that generated the event. This method is used in 
	 * all persistence operation and should not return null.
	 * 
	 * @return the node
	 */
	public int getNodeId();
	
	/**
	 * Returns the unique id for the property (if it is a property change / 
	 * remove operation). Some persistence sources needs this and some 
	 * don't. If the persistence source needs this id it should load and 
	 * set it when the property is loaded. If not, <CODE>-1</CODE> is returned 
	 * (also the case if not a change / remove property operation).
	 *
	 * @return the property id of the property to be changed/removed
	 */
	public int getPropertyId();
	
	/**
	 * Sets the unique id for the property. This is used as a callback when a 
	 * new property is created from persistence sources that needs to have a 
	 * property id. 
	 *
	 * @param id new property id
	 */
	public void setNewPropertyId( int id );

	/**
	 * Returns the property key name if the operation is a property 
	 * related. Property related operations are add, remove and change 
	 * property. Returns null if not property operation (create delete node).
	 *
	 * @return the property index
	 */
	public PropertyIndex getPropertyIndex();
	
	/**
	 * Returns the value of a added property or new value in case of change
	 * property operation, else null.
	 *
	 * @return the property value
	 */
	public Object getProperty();
}