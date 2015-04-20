/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import org.neo4j.kernel.api.properties.Property;

public class RelationshipImpl extends ArrayBasedPrimitive
{
    /*
     * The id long is used to store the relationship id (which is at most 35 bits).
     * But also the high order bits for the start node (s) and end node (e) as well
     * as the relationship type. This allows for a more compressed memory
     * representation.
     *
     *    2 bytes type      start/end high                   5 bytes of id
     * [tttt,tttt][tttt,tttt][ssss,eeee][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii]
     */
    private final long idAndMore;
    private final int startNodeId;
    private final int endNodeId;

    public RelationshipImpl( long id, long startNodeId, long endNodeId, int typeId )
    {
        this.startNodeId = (int) startNodeId;
        this.endNodeId = (int) endNodeId;
        this.idAndMore = (((long)typeId) << 48) | ((startNodeId&0xF00000000L)<<12) | ((endNodeId&0xF00000000L)<<8) | id;
    }

    @Override
    public boolean equals( Object obj )
    {
        return this == obj
               || ( obj instanceof RelationshipImpl && ( (RelationshipImpl) obj ).getId() == getId() );
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (int) (idAndMore ^ (idAndMore >>> 32));
        return result;
    }

    @Override
    public int sizeOfObjectInBytesIncludingOverhead()
    {
        return super.sizeOfObjectInBytesIncludingOverhead() + 8/*idAndMore*/ + 8/*startNodeId and endNodeId*/;
    }

    @Override
    public long getId()
    {
        return idAndMore&0xFFFFFFFFFFL;
    }

    public long getStartNodeId()
    {
        return (startNodeId&0xFFFFFFFFL) | ((idAndMore&0xF00000000000L)>>12);
    }

    public long getEndNodeId()
    {
        return (endNodeId&0xFFFFFFFFL) | ((idAndMore&0xF0000000000L)>>8);
    }

    public int getTypeId()
    {
        return (int)((idAndMore&0xFFFF000000000000L)>>>48);
    }

    @Override
    public String toString()
    {
        return "RelationshipImpl #" + this.getId() + " of type " + getTypeId()
            + " between Node[" + getStartNodeId() + "] and Node[" + getEndNodeId() + "]";
    }

    @Override
    protected Property noProperty( int key )
    {
        return Property.noRelationshipProperty( getId(), key );
    }

}
