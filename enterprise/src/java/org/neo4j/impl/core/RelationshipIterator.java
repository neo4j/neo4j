package org.neo4j.impl.core;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;

class RelationshipIterator implements Iterable<Relationship>, 
	Iterator<Relationship>
{
	private static final NodeManager nm = NodeManager.getManager();
	
	private int[] relIds;
	private Node fromNode;
	private Direction direction = null;
	private Relationship nextElement = null;
	private int nextPosition = 0;
	
	RelationshipIterator( int[] relIds, Node fromNode, 
		Direction direction )
	{
		this.relIds = relIds;
		this.fromNode = fromNode;
		this.direction = direction;
	}

	public Iterator<Relationship> iterator()
	{
		return new RelationshipIterator( relIds, fromNode, direction );
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
					Relationship possibleElement = nm.getRelationshipById( 
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
