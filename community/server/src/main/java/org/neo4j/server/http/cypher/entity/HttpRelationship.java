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
package org.neo4j.server.http.cypher.entity;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class HttpRelationship implements Relationship {
    private final long relId;
    private final String relElementId;
    private final long startNodeId;
    private final String startNodeElementId;
    private final long endNodeId;
    private final String endNodeElementId;
    private final String type;
    private final Map<String, Object> properties;
    private final boolean isDeleted;
    private final BiFunction<Long, Boolean, Optional<Node>> getNodeById;

    public HttpRelationship(
            String relElementId,
            long relId,
            String startNodeElementId,
            long startNodeId,
            String endNodeElementId,
            long endNodeId,
            String type,
            Map<String, Object> properties,
            boolean isDeleted,
            BiFunction<Long, Boolean, Optional<Node>> getNodeById) {
        this.relId = relId;
        this.relElementId = relElementId;
        this.startNodeId = startNodeId;
        this.startNodeElementId = startNodeElementId;
        this.endNodeId = endNodeId;
        this.endNodeElementId = endNodeElementId;
        this.type = type;
        this.properties = properties;
        this.isDeleted = isDeleted;
        this.getNodeById = getNodeById;
    }

    @Override
    public long getId() {
        return relId;
    }

    @Override
    public String getElementId() {
        return relElementId;
    }

    @Override
    public boolean hasProperty(String key) {
        return false;
    }

    @Override
    public Object getProperty(String key) {
        return null;
    }

    @Override
    public Object getProperty(String key, Object defaultValue) {
        return null;
    }

    @Override
    public void setProperty(String key, Object value) {}

    @Override
    public Object removeProperty(String key) {
        return null;
    }

    @Override
    public Iterable<String> getPropertyKeys() {
        return null;
    }

    @Override
    public Map<String, Object> getProperties(String... keys) {
        return null;
    }

    @Override
    public Map<String, Object> getAllProperties() {
        return properties;
    }

    @Override
    public void delete() {}

    @Override
    public long getStartNodeId() {
        return startNodeId;
    }

    @Override
    public long getEndNodeId() {
        return endNodeId;
    }

    @Override
    public Node getStartNode() {
        return getNodeById.apply(startNodeId, isDeleted).orElseGet(() -> new HttpNode(startNodeElementId, startNodeId));
    }

    @Override
    public Node getEndNode() {
        return getNodeById.apply(endNodeId, isDeleted).orElseGet(() -> new HttpNode(endNodeElementId, endNodeId));
    }

    @Override
    public Node getOtherNode(Node node) {
        return null;
    }

    @Override
    public Node[] getNodes() {
        return new Node[0];
    }

    @Override
    public RelationshipType getType() {
        return RelationshipType.withName(type);
    }

    @Override
    public boolean isType(RelationshipType type) {
        return false;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(relId);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Relationship && this.getId() == ((Relationship) other).getId();
    }
}
