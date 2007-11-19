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
package org.neo4j.api.core;

import java.util.Collection;
import java.util.Iterator;

/**
 * See {@link Node#traverse Node.traverse(...)}.
 */
public interface Traverser extends Iterable<Node>
{
	/**
	 * Enum defining the two types of traversals.
	 */
	public static enum Order { DEPTH_FIRST, BREADTH_FIRST }
	
	/**
	 * Returns the current traversal postion.
	 * 
	 * @return The current traversal position
	 */
	public TraversalPosition currentPosition();
	
	/**
	 * Returns a collection of all nodes returned by this traverser.
	 * 
	 * @return A collection of all node returned by this traverser
	 */
	public Collection<Node> getAllNodes();
	
	// Doc: especially remove() thing
	/**
	 * Returns an iterator for this traverser.
	 * 
	 * @return An iterator for this traverser
	 */
	// Doc: does it create a new iterator or reuse the existing one? This is
	// very important! It must be re-use, how else would currentPosition()
    // make sense?
	public Iterator<Node> iterator();
}
