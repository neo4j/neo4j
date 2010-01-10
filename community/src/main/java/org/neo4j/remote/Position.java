/*
 * Copyright (c) 2008-2009 "Neo Technology,"
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
package org.neo4j.remote;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.TraversalPosition;

class Position implements TraversalPosition
{
    private final int depth;
    private final int returned;
    private final Relationship last;
    private final Node current;

    Position( int depth, int returned, Relationship last, Node current )
    {
        this.depth = depth;
        this.returned = returned;
        this.last = last;
        this.current = current;
    }

    public Node currentNode()
    {
        return current;
    }

    public int depth()
    {
        return depth;
    }

    public boolean isStartNode()
    {
        return last == null;
    }

    public boolean notStartNode()
    {
        return last != null;
    }

    public Relationship lastRelationshipTraversed()
    {
        return last;
    }

    public Node previousNode()
    {
        return last.getOtherNode( current );
    }

    public int returnedNodesCount()
    {
        return returned;
    }
}
