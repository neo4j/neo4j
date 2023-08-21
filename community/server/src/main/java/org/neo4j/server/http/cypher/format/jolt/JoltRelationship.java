/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.http.cypher.format.jolt;

import java.util.Map;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public final class JoltRelationship {
    private final long id;
    private final String elementId;
    private final long startNodeId;
    private final String startNodeElementId;
    private final long endNodeId;
    private final String endNodeElementId;
    private final RelationshipType relationshipType;
    private final Map<String, Object> properties;

    private JoltRelationship(
            String elementId,
            long id,
            String startNodeElementId,
            long startNodeId,
            RelationshipType relationshipType,
            String endNodeElementId,
            long endNodeId,
            Map<String, Object> properties) {
        this.elementId = elementId;
        this.id = id;
        this.startNodeElementId = startNodeElementId;
        this.startNodeId = startNodeId;
        this.endNodeElementId = endNodeElementId;
        this.endNodeId = endNodeId;
        this.relationshipType = relationshipType;
        this.properties = properties;
    }

    public static JoltRelationship fromRelationshipReversed(Relationship relationship) {
        return new JoltRelationship(
                relationship.getElementId(),
                relationship.getId(),
                relationship.getEndNode().getElementId(),
                relationship.getEndNodeId(),
                relationship.getType(),
                relationship.getStartNode().getElementId(),
                relationship.getStartNodeId(),
                relationship.getAllProperties());
    }

    public RelationshipType getType() {
        return relationshipType;
    }

    public long getId() {
        return id;
    }

    public String getElementId() {
        return elementId;
    }

    public long getStartNodeId() {
        return startNodeId;
    }

    public String getStartNodeElementId() {
        return startNodeElementId;
    }

    public long getEndNodeId() {
        return endNodeId;
    }

    public String getEndNodeElementId() {
        return endNodeElementId;
    }

    public Map<String, Object> getAllProperties() {
        return properties;
    }
}
