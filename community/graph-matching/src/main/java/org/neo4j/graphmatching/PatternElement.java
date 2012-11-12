/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * Represents a part of a match. It holds the matching {@link Node}, its
 * corresponsing {@link PatternNode} in the client-supplied pattern as well
 * as the last {@link Relationship} and {@link PatternRelationship} to get
 * there.
 */
@Deprecated
public class PatternElement
{
	private PatternNode pNode;
	private Node node;
    private PatternRelationship prevPatternRel = null;
    private Relationship prevRel = null;

	PatternElement( PatternNode pNode, PatternRelationship pRel,
        Node node, Relationship rel )
	{
		this.pNode = pNode;
		this.node = node;
        this.prevPatternRel = pRel;
        this.prevRel = rel;
	}

	/**
	 * Returns the {@link PatternNode} corresponding to the matching
	 * {@link Node}.
	 * @return the {@link PatternNode} corresponsing to matching {@link Node}.
	 */
	public PatternNode getPatternNode()
	{
		return pNode;
	}

    /**
     * Returns the matching {@link Node} which is just one part of the whole
     * match.
     * @return the matching {@link Node} which is just one part of the whole
     * match.
     */
	public Node getNode()
	{
		return node;
	}

	@Override
	public String toString()
	{
		return pNode.toString();
	}

    /**
     * Returns the {@link PatternRelationship} corresponding to the matching
     * {@link Relationship}.
     * @return the {@link PatternRelationship} corresponsing to matching
     * {@link Relationship}.
     */
    public PatternRelationship getFromPatternRelationship()
    {
        return prevPatternRel;
    }

    /**
     * Returns the {@link Relationship} traversed to get to the {@link Node}
     * returned from {@link #getNode()}.
     * @return the {@link Relationship} traversed to get to this node.
     */
    public Relationship getFromRelationship()
    {
        return prevRel;
    }
    
    public int hashCode()
    {
        return pNode.hashCode();
    }
    
    public boolean equals( Object o )
    {
        if ( o instanceof PatternElement )
        {
            return pNode.equals( ( (PatternElement) o ).pNode );
        }
        return false;
    }
}
