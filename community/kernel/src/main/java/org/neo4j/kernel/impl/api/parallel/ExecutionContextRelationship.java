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
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class ExecutionContextRelationship implements Relationship {

    private final long relationshipId;

    public ExecutionContextRelationship(long relationshipId) {
        this.relationshipId = relationshipId;
    }

    @Override
    public long getId() {
        return relationshipId;
    }

    @Override
    public String getElementId() {
        return Long.toString(relationshipId);
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
    public Node getStartNode() {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public Node getEndNode() {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public Node getOtherNode(Node node) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public Node[] getNodes() {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public RelationshipType getType() {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public boolean isType(RelationshipType type) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }
}
