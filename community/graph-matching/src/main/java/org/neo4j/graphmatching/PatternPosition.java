/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphmatching;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * Represents a position were we are in the pattern itself. So the
 * {@link PatternMatcher} starts matching from a starting point, on a
 * {@link PatternNode} and when trying to find a complete match (a complete
 * match is a graph) it uses the PatternPosition to know where in the matching
 * pattern we are at the moment.
 */
@Deprecated
class PatternPosition
{
	private Node currentNode;
	private PatternNode pNode;
	private Iterator<PatternRelationship> itr;
	private PatternRelationship nextPRel = null;
	private PatternRelationship previous = null;
	private PatternRelationship returnPrevious = null;
	private boolean optional = false;
    private PatternRelationship fromPRel = null;
    private Relationship fromRel = null;

	PatternPosition( Node currentNode, PatternNode pNode, boolean optional )
	{
		this.currentNode = currentNode;
		this.pNode = pNode;
		itr = pNode.getRelationships( optional ).iterator();
		this.optional = optional;
	}

    PatternPosition( Node currentNode, PatternNode pNode,
        PatternRelationship fromPRel, Relationship fromRel, boolean optional )
    {
        this.currentNode = currentNode;
        this.pNode = pNode;
        itr = pNode.getRelationships( optional ).iterator();
        this.optional = optional;
        this.fromPRel = fromPRel;
        this.fromRel = fromRel;
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
		itr = pNode.getRelationships( optional ).iterator();
    }

	public void returnPreviousAgain()
    {
		returnPrevious = previous;
    }

	@Override
	public String toString()
	{
		return pNode.toString();
	}

    public PatternRelationship fromPatternRel()
    {
        return fromPRel;
    }

    public Relationship fromRelationship()
    {
        return fromRel;
    }
}
