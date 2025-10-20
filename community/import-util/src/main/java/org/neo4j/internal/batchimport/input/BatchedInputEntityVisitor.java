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
package org.neo4j.internal.batchimport.input;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.batchimport.api.input.ApplicationMode;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.internal.id.IdSequence;

/**
 * Batches parsed entities as {@link InputEntity} instances and passes batches of those to
 * {@link #handle(InputEntity[])}.
 */
public abstract class BatchedInputEntityVisitor implements InputEntityVisitor {
    private final int batchSize;
    private final List<InputEntity> batch = new ArrayList<>();
    private InputEntity current = new InputEntity();

    protected BatchedInputEntityVisitor(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public boolean propertyId(long nextProp) {
        return current.propertyId(nextProp);
    }

    @Override
    public boolean properties(ByteBuffer properties, boolean offloaded) {
        return current.properties(properties, offloaded);
    }

    @Override
    public boolean property(String key, Object value, boolean identifier) {
        return current.property(key, value, identifier);
    }

    @Override
    public boolean property(int propertyKeyId, Object value, boolean identifier) {
        return current.property(propertyKeyId, value, identifier);
    }

    @Override
    public boolean removedProperties(String[] keys) {
        return current.removedProperties(keys);
    }

    @Override
    public boolean removedProperties(int[] keys) {
        return current.removedProperties(keys);
    }

    @Override
    public boolean id(long id) {
        return current.id(id);
    }

    @Override
    public boolean id(Object id, Group group) {
        return current.id(id, group);
    }

    @Override
    public boolean id(Object id, Group group, IdSequence idSequence) {
        return current.id(id, group, idSequence);
    }

    @Override
    public boolean labels(String[] labels) {
        return current.labels(labels);
    }

    @Override
    public boolean labels(int[] labels) {
        return current.labels(labels);
    }

    @Override
    public boolean removedLabels(String[] labels) {
        return current.removedLabels(labels);
    }

    @Override
    public boolean removedLabels(int[] labels) {
        return current.removedLabels(labels);
    }

    @Override
    public boolean labelField(long labelField) {
        return current.labelField(labelField);
    }

    @Override
    public boolean startId(long id) {
        return current.startId(id);
    }

    @Override
    public boolean startId(Object id, Group group) {
        return current.startId(id, group);
    }

    @Override
    public boolean endId(long id) {
        return current.endId(id);
    }

    @Override
    public boolean endId(Object id, Group group) {
        return current.endId(id, group);
    }

    @Override
    public boolean type(int type) {
        return current.type(type);
    }

    @Override
    public boolean type(String type) {
        return current.type(type);
    }

    @Override
    public boolean applicationMode(ApplicationMode mode) {
        return current.applicationMode(mode);
    }

    @Override
    public void endOfEntity() throws IOException {
        batch.add(current);
        current = new InputEntity();

        if (batch.size() >= batchSize) {
            handle(batch.toArray(new InputEntity[0]));
            batch.clear();
        }
    }

    @Override
    public void reset() {
        current = new InputEntity();
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    @Override
    public void flush() throws IOException {
        if (!batch.isEmpty()) {
            var entities = batch.toArray(new InputEntity[0]);
            batch.clear();
            handle(entities);
        }
    }

    protected abstract void handle(InputEntity[] entities) throws IOException;
}
