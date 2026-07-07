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

import java.util.HashMap;
import java.util.Map;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.CompositeDatabaseValue.CompositeDirectRelationshipValue;

/**
 * A read-only {@link Relationship} backed by an already-materialized
 * {@link CompositeDirectRelationshipValue}, where the underlying value already carries
 * the type and properties.
 *
 * Start/end node access and all mutating operations throw {@link UnsupportedOperationException}
 * using the message supplied at construction time.
 */
class CompositeMaterializedRelationship implements Relationship {
    private final CompositeDirectRelationshipValue value;
    private final ValueMapper<Object> valueMapper;
    private final String unsupportedOperationMessage;

    CompositeMaterializedRelationship(
            CompositeDirectRelationshipValue value,
            ValueMapper<Object> valueMapper,
            String unsupportedOperationMessage) {
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
    public RelationshipType getType() {
        return RelationshipType.withName(value.type().stringValue());
    }

    @Override
    public boolean isType(RelationshipType type) {
        return getType().name().equals(type.name());
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
    public Node getStartNode() {
        throw unsupportedOperation();
    }

    @Override
    public Node getEndNode() {
        throw unsupportedOperation();
    }

    @Override
    public Node getOtherNode(Node node) {
        throw unsupportedOperation();
    }

    @Override
    public Node[] getNodes() {
        throw unsupportedOperation();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CompositeMaterializedRelationship other)) {
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
        return "Relationship[" + getElementId() + "]";
    }
}
