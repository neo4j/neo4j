/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

/**
 * Wrapper class for the data contained in a relationship record.
 */
public class RelationshipData
{
    private final long id;
    private final long firstNode;
    private final long secondNode;
    private final int relType;

    /**
     * @param id
     *            The id of the relationship
     * @param directed
     *            Set to true if directed
     * @param firstNode
     *            The id of the first node
     * @param secondNode
     *            The id of the second node
     * @param relType
     *            The id of the relationship type
     */
    public RelationshipData( long id, long firstNode, long secondNode, int relType )
    {
        this.id = id;
        this.firstNode = firstNode;
        this.secondNode = secondNode;
        this.relType = relType;
    }

    public long getId()
    {
        return id;
    }

    public long firstNode()
    {
        return firstNode;
    }

    public long secondNode()
    {
        return secondNode;
    }

    public int relationshipType()
    {
        return relType;
    }

    public String toString()
    {
        return "R[" + firstNode + "," + secondNode + "," + relType + "] fN:";
    }
}