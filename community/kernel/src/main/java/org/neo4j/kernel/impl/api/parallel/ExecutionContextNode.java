/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.api.parallel;

import java.util.Map;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;

public class ExecutionContextNode implements Node {

    private final long nodeId;

    public ExecutionContextNode(long nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public long getId() {
        return nodeId;
    }

    @Override
    public String getElementId() {
        return Long.toString(nodeId);
    }

    @Override
    public boolean hasProperty(String key) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public Object getProperty(String key) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public Object getProperty(String key, Object defaultValue) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public void setProperty(String key, Object value) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public Object removeProperty(String key) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public Iterable<String> getPropertyKeys() {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public Map<String, Object> getProperties(String... keys) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public Map<String, Object> getAllProperties() {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public void delete() {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public ResourceIterable<Relationship> getRelationships() {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public boolean hasRelationship() {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public ResourceIterable<Relationship> getRelationships(RelationshipType... types) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public ResourceIterable<Relationship> getRelationships(Direction direction, RelationshipType... types) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public boolean hasRelationship(RelationshipType... types) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public boolean hasRelationship(Direction direction, RelationshipType... types) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public ResourceIterable<Relationship> getRelationships(Direction dir) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public boolean hasRelationship(Direction dir) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public Relationship getSingleRelationship(RelationshipType type, Direction dir) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public Relationship createRelationshipTo(Node otherNode, RelationshipType type) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes() {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public int getDegree() {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public int getDegree(RelationshipType type) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public int getDegree(Direction direction) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public int getDegree(RelationshipType type, Direction direction) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public void addLabel(Label label) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public void removeLabel(Label label) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public boolean hasLabel(Label label) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public Iterable<Label> getLabels() {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }
}
