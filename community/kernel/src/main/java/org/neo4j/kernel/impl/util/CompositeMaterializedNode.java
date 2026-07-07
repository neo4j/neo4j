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
package org.neo4j.kernel.impl.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.CompositeDatabaseValue.CompositeGraphDirectNodeValue;

/**
 * A read-only {@link Node} backed by an already-materialized
 * {@link CompositeGraphDirectNodeValue}, where the underlying value already carries the labels and
 * properties.
 *
 * All graph-mutating and traversal operations throw {@link UnsupportedOperationException} using the
 * message supplied at construction time to describe why the operation is not available.
 */
class CompositeMaterializedNode implements Node {
    private final CompositeGraphDirectNodeValue value;
    private final ValueMapper<Object> valueMapper;
    private final String unsupportedOperationMessage;

    CompositeMaterializedNode(
            CompositeGraphDirectNodeValue value, ValueMapper<Object> valueMapper, String unsupportedOperationMessage) {
        this.value = value;
        this.valueMapper = valueMapper;
        this.unsupportedOperationMessage = unsupportedOperationMessage;
    }

    @Override
    public long getId() {
        return value.id();
    }

    @Override
    public String getElementId() {
        return value.elementId();
    }

    @Override
    public boolean hasProperty(String key) {
        return value.properties().containsKey(key);
    }

    @Override
    public Object getProperty(String key) {
        var v = value.properties().get(key);
        if (v == null || v == org.neo4j.values.storable.Values.NO_VALUE) {
            throw new NotFoundException("No such property, '" + key + "'.");
        }
        return v.map(valueMapper);
    }

    @Override
    public Object getProperty(String key, Object defaultValue) {
        var v = value.properties().get(key);
        if (v == null || v == org.neo4j.values.storable.Values.NO_VALUE) {
            return defaultValue;
        }
        return v.map(valueMapper);
    }

    @Override
    public Iterable<String> getPropertyKeys() {
        return value.properties().keySet();
    }

    @Override
    public Map<String, Object> getProperties(String... keys) {
        Map<String, Object> result = new HashMap<>(keys.length);
        var props = value.properties();
        for (String key : keys) {
            var v = props.get(key);
            if (v != null && v != org.neo4j.values.storable.Values.NO_VALUE) {
                result.put(key, v.map(valueMapper));
            }
        }
        return result;
    }

    @Override
    public Map<String, Object> getAllProperties() {
        Map<String, Object> result = new HashMap<>();
        value.properties().foreach((k, v) -> result.put(k, v.map(valueMapper)));
        return result;
    }

    @Override
    public Iterable<Label> getLabels() {
        var labels = value.labels();
        int size = labels.intSize();
        List<Label> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            result.add(Label.label(labels.stringValue(i).stringValue()));
        }
        return result;
    }

    @Override
    public boolean hasLabel(Label label) {
        var labels = value.labels();
        String name = label.name();
        int size = labels.intSize();
        for (int i = 0; i < size; i++) {
            if (name.equals(labels.stringValue(i).stringValue())) {
                return true;
            }
        }
        return false;
    }

    private UnsupportedOperationException unsupportedOperation() {
        return new UnsupportedOperationException(unsupportedOperationMessage);
    }

    @Override
    public void setProperty(String key, Object value) {
        throw unsupportedOperation();
    }

    @Override
    public Object removeProperty(String key) {
        throw unsupportedOperation();
    }

    @Override
    public void delete() {
        throw unsupportedOperation();
    }

    @Override
    public ResourceIterable<Relationship> getRelationships() {
        throw unsupportedOperation();
    }

    @Override
    public boolean hasRelationship() {
        throw unsupportedOperation();
    }

    @Override
    public ResourceIterable<Relationship> getRelationships(RelationshipType... types) {
        throw unsupportedOperation();
    }

    @Override
    public ResourceIterable<Relationship> getRelationships(Direction direction, RelationshipType... types) {
        throw unsupportedOperation();
    }

    @Override
    public boolean hasRelationship(RelationshipType... types) {
        throw unsupportedOperation();
    }

    @Override
    public boolean hasRelationship(Direction direction, RelationshipType... types) {
        throw unsupportedOperation();
    }

    @Override
    public ResourceIterable<Relationship> getRelationships(Direction dir) {
        throw unsupportedOperation();
    }

    @Override
    public boolean hasRelationship(Direction dir) {
        throw unsupportedOperation();
    }

    @Override
    public Relationship getSingleRelationship(RelationshipType type, Direction dir) {
        throw unsupportedOperation();
    }

    @Override
    public Relationship createRelationshipTo(Node otherNode, RelationshipType type) {
        throw unsupportedOperation();
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes() {
        throw unsupportedOperation();
    }

    @Override
    public int getDegree() {
        throw unsupportedOperation();
    }

    @Override
    public int getDegree(RelationshipType type) {
        throw unsupportedOperation();
    }

    @Override
    public int getDegree(Direction direction) {
        throw unsupportedOperation();
    }

    @Override
    public int getDegree(RelationshipType type, Direction direction) {
        throw unsupportedOperation();
    }

    @Override
    public void addLabel(Label label) {
        throw unsupportedOperation();
    }

    @Override
    public void removeLabel(Label label) {
        throw unsupportedOperation();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CompositeMaterializedNode other)) {
            return false;
        }
        return this.value.equals(other.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return "Node[" + getElementId() + "]";
    }
}
