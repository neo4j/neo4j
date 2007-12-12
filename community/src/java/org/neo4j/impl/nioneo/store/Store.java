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

import java.io.IOException;

/**
 * Common interface for the node,relationship,property and relationship
 * type stores.
 */
public interface Store
{
	/**
	 * Returns the id of next free record.
	 * 
	 * @return The id of the next free record
	 * @throws IOException If unable to 
	 */
	public int nextId(); // throws IOException;
	
	public int getHighestPossibleIdInUse();
	
	public int getNumberOfIdsInUse();
}
