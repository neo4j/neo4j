package org.neo4j.util.matching;

import java.util.Iterator;
//import java.util.LinkedList;
import java.util.NoSuchElementException;
import org.neo4j.api.core.Node;

class PatternPosition
{
	private Node currentNode;
	private PatternNode pNode;
	private Iterator<PatternRelationship> itr;
	private PatternRelationship nextPRel = null;
	private PatternRelationship previous = null;
	private PatternRelationship returnPrevious = null;

	PatternPosition( Node currentNode, PatternNode pNode )
	{
		this.currentNode = currentNode;
		this.pNode = pNode;
		itr = pNode.getRelationships().iterator();
	}

	Node getCurrentNode()
	{
		return currentNode;
	}

	private void setNextQRel()
	{
		while ( itr.hasNext() )
		{
			nextPRel = itr.next();
			if ( !nextPRel.isMarked() )
			{
				return;
			}
			nextPRel = null;
		}
	}

	PatternNode getPatternNode()
	{
		return pNode;
	}

	boolean hasNext()
	{
		if ( returnPrevious != null )
		{
			return true;
		}
		if ( nextPRel == null )
		{
			setNextQRel();
		}
		return nextPRel != null;
	}

	PatternRelationship next()
	{
		if ( returnPrevious != null )
		{
			PatternRelationship relToReturn = returnPrevious;
			returnPrevious = null;
			return relToReturn;
		}
		if ( nextPRel == null )
		{
			setNextQRel();
		}
		else
		{
			return resetNextPRel();
		}
		if ( nextPRel == null )
		{
			throw new NoSuchElementException();
		}
		return resetNextPRel();
	}

	private PatternRelationship resetNextPRel()
	{
		PatternRelationship relToReturn = nextPRel;
		previous = nextPRel;
		nextPRel = null;
		return relToReturn;
	}

	void reset()
    {
		returnPrevious = null;
		previous = null;
		nextPRel = null;
		itr = pNode.getRelationships().iterator();
    }

	public void returnPreviousAgain()
    {
		returnPrevious = previous;
    }
}
