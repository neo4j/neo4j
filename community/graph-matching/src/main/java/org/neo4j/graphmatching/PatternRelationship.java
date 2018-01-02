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

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * Represents a pattern for matching a {@link Relationship}.
 */
@Deprecated
public class PatternRelationship extends AbstractPatternObject<Relationship>
{
	private final RelationshipType type;
    private final boolean directed;
    private final boolean optional;
    private final boolean anyType;
	private final PatternNode firstNode;
	private final PatternNode secondNode;

	private boolean isMarked = false;

    PatternRelationship( PatternNode firstNode,
        PatternNode secondNode, boolean optional, boolean directed )
    {
        this.directed = directed;
        this.anyType = true;
        this.firstNode = firstNode;
        this.secondNode = secondNode;
        this.optional = optional;
        this.type = null;
    }

	PatternRelationship( RelationshipType type, PatternNode firstNode,
		PatternNode secondNode, boolean optional, boolean directed )
	{
	    this.directed = directed;
	    this.anyType = false;
		this.type = type;
		this.firstNode = firstNode;
		this.secondNode = secondNode;
		this.optional = optional;
	}

    boolean anyRelType()
    {
        return anyType;
    }

    /**
     * Get the {@link PatternNode} that this pattern relationship relates, that
     * is not the specified node.
     *
     * @param node one of the {@link PatternNode}s this pattern relationship
     *            relates.
     * @return the other pattern node.
     */
	public PatternNode getOtherNode( PatternNode node )
	{
		if ( node == firstNode )
		{
			return secondNode;
		}
		if ( node == secondNode )
		{
			return firstNode;
		}
		throw new RuntimeException( "Node[" + node +
			"] not in this relationship" );
	}

    /**
     * Get the first pattern node that this pattern relationship relates.
     *
     * @return the first pattern node.
     */
	public PatternNode getFirstNode()
	{
		return firstNode;
	}

    /**
     * Get the second pattern node that this pattern relationship relates.
     *
     * @return the second pattern node.
     */
	public PatternNode getSecondNode()
	{
		return secondNode;
	}

    /**
     * Does this pattern relationship represent a relationship that has to exist
     * in the subgraph to consider the subgraph a match of the pattern, or is it
     * an optional relationship.
     *
     * @return <code>true</code> if this pattern relationship represents an
     *         optional relationship, <code>false</code> if it represents a
     *         required relationship.
     */
	public boolean isOptional()
	{
		return optional;
	}

	void mark()
	{
		isMarked = true;
	}

	void unMark()
	{
		isMarked = false;
	}

	boolean isMarked()
	{
		return isMarked;
	}

    /**
     * Get the {@link RelationshipType} a relationship must have in order to
     * match this pattern relationship. Will return <code>null</code> if a
     * relationship with any {@link RelationshipType} will match.
     *
     * @return the {@link RelationshipType} of this relationship pattern.
     */
	public RelationshipType getType()
	{
		return type;
	}

    /**
     * Get the direction in which relationships are discovered using this
     * relationship pattern from the specified node. May be
     * {@link Direction#OUTGOING outgoing}, {@link Direction#INCOMING incoming},
     * or {@link Direction#BOTH both}.
     *
     * @param fromNode the pattern node to find the direction of this pattern
     *            relationship from.
     * @return the direction to discover relationships matching this pattern in.
     */
	public Direction getDirectionFrom( PatternNode fromNode )
    {
        if ( !directed )
        {
            return Direction.BOTH;
        }
	    if ( fromNode.equals( firstNode ) )
	    {
	    	return Direction.OUTGOING;
	    }
	    if ( fromNode.equals( secondNode ) )
	    {
	    	return Direction.INCOMING;
	    }
	    throw new RuntimeException( fromNode + " not in " + this );
    }

	@Override
	public String toString()
	{
		return type + ":" + optional;
	}
}
