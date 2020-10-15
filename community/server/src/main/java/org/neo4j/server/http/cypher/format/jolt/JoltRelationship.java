/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.server.http.cypher.format.jolt;

import java.util.Map;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

final class JoltRelationship
{
    private final long id;
    private final long startNodeId;
    private final long endNodeId;
    private final RelationshipType relationshipType;
    private final Map<String,Object> properties;

    private JoltRelationship( long id, long startNodeId, RelationshipType relationshipType, long endNodeId, Map<String,Object> properties )
    {
        this.id = id;
        this.startNodeId = startNodeId;
        this.endNodeId = endNodeId;
        this.relationshipType = relationshipType;
        this.properties = properties;
    }

    public static JoltRelationship fromRelationshipReversed( Relationship relationship )
    {
        return new JoltRelationship( relationship.getId(), relationship.getEndNodeId(),
                                     relationship.getType(), relationship.getStartNodeId(), relationship.getAllProperties() );
    }

    public RelationshipType getType()
    {
        return relationshipType;
    }

    public long getId()
    {
        return id;
    }

    public long getStartNodeId()
    {
        return startNodeId;
    }

    public long getEndNodeId()
    {
        return endNodeId;
    }

    public Map<String,Object> getAllProperties()
    {
        return properties;
    }
}
