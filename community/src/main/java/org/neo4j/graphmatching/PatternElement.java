/*
 * Copyright (c) 2008-2010 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphmatching;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

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

	public PatternNode getPatternNode()
	{
		return pNode;
	}

	public Node getNode()
	{
		return node;
	}

	@Override
	public String toString()
	{
		return pNode.toString();
	}

    public PatternRelationship getFromPatternRelationship()
    {
        return prevPatternRel;
    }

    public Relationship getFromRelationship()
    {
        return prevRel;
    }
}
