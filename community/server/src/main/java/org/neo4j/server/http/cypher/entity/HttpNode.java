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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;

public class HttpNode implements Node {
    private final long nodeId;
    private final String elementId;
    private final Map<String, Object> propertyMap;
    private final List<Label> labels;
    private final boolean isDeleted;
    private final boolean isFullNode;

    public HttpNode(String elementId, long id, List<Label> labels, Map<String, Object> propertyMap, boolean isDeleted) {
        this.elementId = elementId;
        this.nodeId = id;
        this.labels = labels;
        this.propertyMap = propertyMap;
        this.isDeleted = isDeleted;
        this.isFullNode = true;
    }

    public HttpNode(String elementId, long id) {
        this.elementId = elementId;
        this.nodeId = id;
        this.labels = new ArrayList<>();
        this.propertyMap = new HashMap<>();
        this.isDeleted = false;
        this.isFullNode = false;
    }

    @Override
    public long getId() {
        return nodeId;
    }

    @Override
    public String getElementId() {
        return elementId;
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
        return propertyMap;
    }

    @Override
    public void delete() {}

    @Override
    public ResourceIterable<Relationship> getRelationships() {
        return null;
    }

    @Override
    public boolean hasRelationship() {
        return false;
    }

    @Override
    public ResourceIterable<Relationship> getRelationships(RelationshipType... types) {
        return null;
    }

    @Override
    public ResourceIterable<Relationship> getRelationships(Direction direction, RelationshipType... types) {
        return null;
    }

    @Override
    public boolean hasRelationship(RelationshipType... types) {
        return false;
    }

    @Override
    public boolean hasRelationship(Direction direction, RelationshipType... types) {
        return false;
    }

    @Override
    public ResourceIterable<Relationship> getRelationships(Direction dir) {
        return null;
    }

    @Override
    public boolean hasRelationship(Direction dir) {
        return false;
    }

    @Override
    public Relationship getSingleRelationship(RelationshipType type, Direction dir) {
        return null;
    }

    @Override
    public Relationship createRelationshipTo(Node otherNode, RelationshipType type) {
        return null;
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes() {
        return null;
    }

    @Override
    public int getDegree() {
        return 0;
    }

    @Override
    public int getDegree(RelationshipType type) {
        return 0;
    }

    @Override
    public int getDegree(Direction direction) {
        return 0;
    }

    @Override
    public int getDegree(RelationshipType type, Direction direction) {
        return 0;
    }

    @Override
    public void addLabel(Label label) {}

    @Override
    public void removeLabel(Label label) {}

    @Override
    public boolean hasLabel(Label label) {
        return false;
    }

    @Override
    public Iterable<Label> getLabels() {
        return labels;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public boolean isFullNode() {
        return isFullNode;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Node && this.getId() == ((Node) o).getId();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId());
    }
}
