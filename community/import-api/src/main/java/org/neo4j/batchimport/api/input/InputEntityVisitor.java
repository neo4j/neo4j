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
package org.neo4j.batchimport.api.input;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.neo4j.internal.id.IdSequence;

/**
 * Receives calls for extracted data from {@link InputChunk}. This callback design allows for specific methods
 * using primitives and other optimizations, to avoid garbage.
 */
public interface InputEntityVisitor extends Closeable {
    default boolean propertyId(long nextProp) {
        return true;
    }

    default boolean properties(ByteBuffer properties, boolean offloaded) {
        return true;
    }

    /**
     * Visits a property for this entity.
     * @param key property key name.
     * @param value property value.
     * @param identifier if {@code true} then this property acts as some sort of identifier for this entity.
     * When creating an entity (i.e. for {@link #applicationMode(ApplicationMode) CREATE} even identifier properties
     * should be stored on the entity, but for UPDATE/DELETE instead acts as hint to help finding the correct
     * entity.
     * @return {@code true} if this property was accepted and processing should continue, otherwise {@code false}.
     */
    default boolean property(String key, Object value, boolean identifier) {
        return true;
    }

    /**
     * Visits a property for this entity.
     * @param propertyKeyId property key id.
     * @param value property value.
     * @param identifier if {@code true} then this property acts as some sort of identifier for this entity.
     * When creating an entity (i.e. for {@link #applicationMode(ApplicationMode) CREATE} even identifier properties
     * should be stored on the entity, but for UPDATE/DELETE instead acts as hint to help finding the correct
     * entity.
     * @return {@code true} if this property was accepted and processing should continue, otherwise {@code false}.
     */
    default boolean property(int propertyKeyId, Object value, boolean identifier) {
        return true;
    }

    default boolean removedProperties(String[] keys) {
        return true;
    }

    default boolean removedProperties(int[] keys) {
        return true;
    }

    // For nodes
    default boolean id(long id) {
        return true;
    }

    default boolean id(Object id, Group group) {
        return true;
    }

    default boolean id(Object id, Group group, IdSequence idSequence) {
        return true;
    }

    default boolean labels(String[] labels) {
        return true;
    }

    default boolean labels(int[] labels) {
        return true;
    }

    default boolean removedLabels(String[] labels) {
        return true;
    }

    default boolean removedLabels(int[] labels) {
        return true;
    }

    default boolean labelField(long labelField) {
        return true;
    }

    // For relationships
    default boolean startId(long id) {
        return true;
    }

    default boolean startId(Object id, Group group) {
        return true;
    }

    default boolean endId(long id) {
        return true;
    }

    default boolean endId(Object id, Group group) {
        return true;
    }

    default boolean type(int type) {
        return true;
    }

    default boolean type(String type) {
        return true;
    }

    default boolean applicationMode(ApplicationMode mode) {
        return true;
    }

    default void endOfEntity() throws IOException {}

    default void reset() {}

    @Override
    default void close() throws IOException {}

    default void flush() throws IOException {}

    class Adapter implements InputEntityVisitor {
        @Override
        public boolean property(String key, Object value, boolean identifier) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean properties(ByteBuffer properties, boolean offloaded) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean property(int propertyKeyId, Object value, boolean identifier) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean propertyId(long nextProp) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removedProperties(String[] keys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removedProperties(int[] keys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean id(long id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean id(Object id, Group group) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean id(Object id, Group group, IdSequence idSequence) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean labels(String[] labels) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean labels(int[] labels) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removedLabels(String[] labels) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removedLabels(int[] labels) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean startId(long id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean startId(Object id, Group group) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean endId(long id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean endId(Object id, Group group) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean type(int type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean type(String type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean labelField(long labelField) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean applicationMode(ApplicationMode mode) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void endOfEntity() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {}
    }

    class Delegate implements InputEntityVisitor {
        private final InputEntityVisitor actual;

        public Delegate(InputEntityVisitor actual) {
            this.actual = actual;
        }

        @Override
        public boolean propertyId(long nextProp) {
            return actual.propertyId(nextProp);
        }

        @Override
        public boolean properties(ByteBuffer properties, boolean offloaded) {
            return actual.properties(properties, offloaded);
        }

        @Override
        public boolean property(String key, Object value, boolean identifier) {
            return actual.property(key, value, identifier);
        }

        @Override
        public boolean property(int propertyKeyId, Object value, boolean identifier) {
            return actual.property(propertyKeyId, value, identifier);
        }

        @Override
        public boolean removedProperties(String[] keys) {
            return actual.removedProperties(keys);
        }

        @Override
        public boolean id(long id) {
            return actual.id(id);
        }

        @Override
        public boolean id(Object id, Group group) {
            return actual.id(id, group);
        }

        @Override
        public boolean id(Object id, Group group, IdSequence idSequence) {
            return actual.id(id, group, idSequence);
        }

        @Override
        public boolean labels(String[] labels) {
            return actual.labels(labels);
        }

        @Override
        public boolean labels(int[] labels) {
            return actual.labels(labels);
        }

        @Override
        public boolean removedLabels(String[] labels) {
            return actual.removedLabels(labels);
        }

        @Override
        public boolean removedLabels(int[] labels) {
            return actual.removedLabels(labels);
        }

        @Override
        public boolean labelField(long labelField) {
            return actual.labelField(labelField);
        }

        @Override
        public boolean startId(long id) {
            return actual.startId(id);
        }

        @Override
        public boolean startId(Object id, Group group) {
            return actual.startId(id, group);
        }

        @Override
        public boolean endId(long id) {
            return actual.endId(id);
        }

        @Override
        public boolean endId(Object id, Group group) {
            return actual.endId(id, group);
        }

        @Override
        public boolean type(int type) {
            return actual.type(type);
        }

        @Override
        public boolean type(String type) {
            return actual.type(type);
        }

        @Override
        public boolean applicationMode(ApplicationMode mode) {
            return actual.applicationMode(mode);
        }

        @Override
        public void endOfEntity() throws IOException {
            actual.endOfEntity();
        }

        @Override
        public void reset() {
            actual.reset();
        }

        @Override
        public void close() throws IOException {
            actual.close();
        }

        @Override
        public boolean removedProperties(int[] keys) {
            return actual.removedProperties(keys);
        }

        @Override
        public void flush() throws IOException {
            actual.flush();
        }
    }

    InputEntityVisitor NULL = new InputEntityVisitor() {};
}
