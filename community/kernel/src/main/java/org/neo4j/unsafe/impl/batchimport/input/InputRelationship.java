/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input;

import org.neo4j.graphdb.Direction;

/**
 * Represents a relationship from an input source, for example a .csv file.
 */
public class InputRelationship extends InputEntity
{
    private final long startNode;
    private final long endNode;
    private final String type;
    private final Integer typeId;

    public InputRelationship( long id, Object[] properties, Long firstPropertyId, long startNode, long endNode,
            String type, Integer typeId )
    {
        super( id, properties, firstPropertyId );
        this.startNode = startNode;
        this.endNode = endNode;
        this.type = type;
        this.typeId = typeId;
    }

    public long startNode()
    {
        return startNode;
    }

    public long endNode()
    {
        return endNode;
    }

    public boolean isLoop()
    {
        return startNode == endNode;
    }

    public Direction startDirection()
    {
        return isLoop() ? Direction.BOTH : Direction.OUTGOING;
    }

    public String type()
    {
        return type;
    }

    public boolean hasTypeId()
    {
        return typeId != null;
    }

    public int typeId()
    {
        return typeId.intValue();
    }
}
