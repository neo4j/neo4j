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

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;

class RelationshipIterator implements Iterable<Relationship>, 
	Iterator<Relationship>
{
	private int[] relIds;
	private Node fromNode;
	private Direction direction = null;
	private Relationship nextElement = null;
	private int nextPosition = 0;
	private final NodeManager nodeManager;
	
	RelationshipIterator( int[] relIds, Node fromNode, 
		Direction direction, NodeManager nodeManager )
	{
		this.relIds = relIds;
		this.fromNode = fromNode;
		this.direction = direction;
		this.nodeManager = nodeManager;
	}

	public Iterator<Relationship> iterator()
	{
		return new RelationshipIterator( relIds, fromNode, direction, 
			nodeManager );
	}

	public boolean hasNext()
	{
		if ( nextElement != null )
		{
			return true;
		}
		do
		{
			if ( nextPosition < relIds.length )
			{
				try
				{
					Relationship possibleElement = 
						nodeManager.getRelationshipById( 
							relIds[nextPosition++] );
					if ( direction == Direction.INCOMING && 
						possibleElement.getEndNode().equals( fromNode ) )
					{
						nextElement = possibleElement;
					}
					else if ( direction == Direction.OUTGOING && 
						possibleElement.getStartNode().equals( fromNode ) )
					{
						nextElement = possibleElement;
					}
					else if ( direction == Direction.BOTH )
					{
						nextElement = possibleElement;
					}
				}
				catch ( Throwable t )
				{ // ok unable to get that relationship
					// TODO: add logging here so user knows he is doing 
					// something wrong
				}
			}
		} while ( nextElement == null && nextPosition < relIds.length );
		return nextElement != null;
	}

	public Relationship next()
	{
		hasNext();
		if ( nextElement != null )
		{
			Relationship elementToReturn = nextElement;
			nextElement = null;
			return elementToReturn;
		}
		throw new NoSuchElementException();
	}

	public void remove()
	{
		throw new UnsupportedOperationException();
	}
}
