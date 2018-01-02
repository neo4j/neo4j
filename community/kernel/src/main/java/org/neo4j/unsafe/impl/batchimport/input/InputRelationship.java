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
package org.neo4j.unsafe.impl.batchimport.input;

import java.util.Collection;

import org.neo4j.helpers.Pair;

import static org.neo4j.unsafe.impl.batchimport.input.Group.GLOBAL;

/**
 * Represents a relationship from an input source, for example a .csv file.
 */
public class InputRelationship extends InputEntity
{
    public static final long NO_SPECIFIC_ID = -1L;

    private long id = NO_SPECIFIC_ID;
    private final Object startNode;
    private final Object endNode;
    private String type;
    private final Integer typeId;
    private final Group startNodeGroup;
    private final Group endNodeGroup;

    public InputRelationship( String sourceDescription, long lineNumber, long position,
            Object[] properties, Long firstPropertyId, Object startNode, Object endNode,
            String type, Integer typeId )
    {
        this( sourceDescription, lineNumber, position,
                properties, firstPropertyId, GLOBAL, startNode, GLOBAL, endNode, type, typeId );
    }

    public InputRelationship(
            String sourceDescription, long lineNumber, long position,
            Object[] properties, Long firstPropertyId,
            Group startNodeGroups, Object startNode,
            Group endNodeGroups, Object endNode,
            String type, Integer typeId )
    {
        super( sourceDescription, lineNumber, position, properties, firstPropertyId );
        this.startNodeGroup = startNodeGroups;
        this.startNode = startNode;
        this.endNodeGroup = endNodeGroups;
        this.endNode = endNode;
        this.type = type;
        this.typeId = typeId;
    }

    public InputRelationship setSpecificId( long id )
    {
        this.id = id;
        return this;
    }

    public boolean hasSpecificId()
    {
        return id != NO_SPECIFIC_ID;
    }

    public long specificId()
    {
        assert hasSpecificId() : "Didn't have specific id set";
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
