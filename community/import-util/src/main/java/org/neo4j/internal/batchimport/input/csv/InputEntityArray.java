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
package org.neo4j.internal.batchimport.input.csv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.input.InputEntity;
import org.neo4j.internal.id.IdSequence;

/**
 * An array of {@link InputEntity} looking like an {@link InputEntityVisitor} to be able to fit into thinks like {@link Decorator}.
 */
public class InputEntityArray implements InputEntityVisitor {
    private final InputEntity[] entities;
    private int cursor;

    public InputEntityArray(int length) {
        this.entities = new InputEntity[length];
    }

    @Override
    public void close() {}

    @Override
    public boolean propertyId(long nextProp) {
        return currentEntity().propertyId(nextProp);
    }

    @Override
    public boolean properties(ByteBuffer properties, boolean offloaded) {
        return currentEntity().properties(properties, offloaded);
    }

    @Override
    public boolean property(String key, Object value) {
        return currentEntity().property(key, value);
    }

    @Override
    public boolean property(int propertyKeyId, Object value) {
        return currentEntity().property(propertyKeyId, value);
    }

    @Override
    public boolean property(String property) {
        return currentEntity().property(property);
    }

    @Override
    public boolean id(long id) {
        return currentEntity().id(id);
    }

    @Override
    public boolean id(Object id, Group group) {
        return currentEntity().id(id, group);
    }

    @Override
    public boolean id(Object id, Group group, IdSequence idSequence) {
        return currentEntity().id(id, group, idSequence);
    }

    @Override
    public boolean labels(String[] labels) {
        return currentEntity().labels(labels);
    }

    @Override
    public boolean labelField(long labelField) {
        return currentEntity().labelField(labelField);
    }

    @Override
    public boolean startId(long id) {
        return currentEntity().startId(id);
    }

    @Override
    public boolean startId(Object id, Group group) {
        return currentEntity().startId(id, group);
    }

    @Override
    public boolean endId(long id) {
        return currentEntity().endId(id);
    }

    @Override
    public boolean endId(Object id, Group group) {
        return currentEntity().endId(id, group);
    }

    @Override
    public boolean type(int type) {
        return currentEntity().type(type);
    }

    @Override
    public boolean type(String type) {
        return currentEntity().type(type);
    }

    @Override
    public void endOfEntity() throws IOException {
        currentEntity().endOfEntity();
        cursor++;
    }

    @Override
    public void reset() {
        currentEntity().reset();
    }

    private InputEntity currentEntity() {
        if (entities[cursor] == null) {
            entities[cursor] = new InputEntity();
        }
        return entities[cursor];
    }

    public InputEntity[] toArray() {
        return cursor == entities.length ? entities : Arrays.copyOf(entities, cursor);
    }
}
