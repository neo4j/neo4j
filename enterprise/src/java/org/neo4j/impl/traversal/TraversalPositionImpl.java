/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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
package org.neo4j.impl.traversal;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.TraversalPosition;

class TraversalPositionImpl implements TraversalPosition
{
    private Node currentNode = null;
    private Node previousNode = null;
    private Relationship lastRelTraversed = null;
    private int currentDepth = -1;
    private int returnedNodesCount = -1;

    TraversalPositionImpl( Node currentNode, Node previousNode,
        Relationship lastRelTraversed, int currentDepth )
    {
        this.currentNode = currentNode;
        this.previousNode = previousNode;
        this.lastRelTraversed = lastRelTraversed;
        this.currentDepth = currentDepth;
    }

    void setReturnedNodesCount( int returnedNodesCount )
    {
        this.returnedNodesCount = returnedNodesCount;
    }

    public Node currentNode()
    {
        return this.currentNode;
    }

    public Node previousNode()
    {
        return this.previousNode;
    }

    public Relationship lastRelationshipTraversed()
    {
        return this.lastRelTraversed;
    }

    public int depth()
    {
        return this.currentDepth;
    }

    public int returnedNodesCount()
    {
        return this.returnedNodesCount;
    }

    public boolean notStartNode()
    {
        return this.depth() > 0;
    }

    public boolean isStartNode()
    {
        return this.depth() == 0;
    }
}