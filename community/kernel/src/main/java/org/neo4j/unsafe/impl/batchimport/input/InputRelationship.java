/**
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
package org.neo4j.unsafe.impl.batchimport.input;

import java.util.Collection;

import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Pair;

import static org.neo4j.unsafe.impl.batchimport.input.Group.GLOBAL;

/**
 * Represents a relationship from an input source, for example a .csv file.
 */
public class InputRelationship extends InputEntity
{
    private final long id;
    private final Object startNode;
    private final Object endNode;
    private String type;
    private final Integer typeId;
    private final Group startNodeGroup;
    private final Group endNodeGroup;

    public InputRelationship( long id,
            Object[] properties, Long firstPropertyId, Object startNode, Object endNode,
            String type, Integer typeId )
    {
        this( id, properties, firstPropertyId, GLOBAL, startNode, GLOBAL, endNode, type, typeId );
    }

    public InputRelationship( long id,
            Object[] properties, Long firstPropertyId,
            Group startNodeGroups, Object startNode,
            Group endNodeGroups, Object endNode,
            String type, Integer typeId )
    {
        super( properties, firstPropertyId );
        this.id = id;
        this.startNodeGroup = startNodeGroups;
        this.startNode = startNode;
        this.endNodeGroup = endNodeGroups;
        this.endNode = endNode;
        this.type = type;
        this.typeId = typeId;
    }

    public long id()
    {
        return id;
    }

    public Group startNodeGroup()
    {
        return startNodeGroup;
    }

    public Object startNode()
    {
        return startNode;
    }

    public Group endNodeGroup()
    {
        return endNodeGroup;
    }

    public Object endNode()
    {
        return endNode;
    }

    public boolean isLoop()
    {
        return startNode.equals( endNode );
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

    public void setType( String type )
    {
        this.type = type;
    }

    @Override
    protected void toStringFields( Collection<Pair<String, ?>> fields )
    {
        super.toStringFields( fields );
        fields.add( Pair.of( "startNode", startNode ) );
        fields.add( Pair.of( "endNode", endNode ) );
        if ( hasTypeId() )
        {
            fields.add( Pair.of( "typeId", typeId ) );
        }
        else
        {
            fields.add( Pair.of( "type", type ) );
        }
    }
}
