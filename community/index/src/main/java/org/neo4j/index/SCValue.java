/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index;

public class SCValue
{
    private long relId;
    private long nodeId;

    public SCValue( long relId, long nodeId )
    {
        this.relId = relId;
        this.nodeId = nodeId;
    }

    public long getRelId()
    {
        return relId;
    }

    public long getNodeId()
    {
        return nodeId;
    }

    @Override
    public int hashCode() {
        return (int) (relId * 23 + nodeId);
    }

    @Override
    public boolean equals( Object obj ) {
        if ( !( obj instanceof SCValue) )
            return false;
        if ( obj == this )
            return true;

        SCValue rhs = (SCValue) obj;
        return relId == rhs.relId && nodeId == rhs.nodeId;
    }

    @Override
    public String toString()
    {
        return String.format( "(%d,%d)", relId, nodeId );
    }
}
