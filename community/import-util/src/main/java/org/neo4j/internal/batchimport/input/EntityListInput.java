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
import java.util.Iterator;
import java.util.List;
import org.neo4j.batchimport.api.InputIterable;
import org.neo4j.batchimport.api.InputIterator;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.batchimport.api.input.InputChunk;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.batchimport.api.input.PropertySizeCalculator;
import org.neo4j.batchimport.api.input.ReadableGroups;
import org.neo4j.importer.SchemaCommandSource;

/**
 * An {@link Input} backed by in-memory lists of {@link InputEntity}, with optional schema commands.
 * Estimates are computed from the lists.
 */
public class EntityListInput implements Input {
    public static final int DEFAULT_BATCH_SIZE = 10;

    private final IdType idType;
    private final int batchSize;
    private final ReadableGroups groups;
    private final List<InputEntity> nodes;
    private final List<InputEntity> relationships;
    private final SchemaCommandSource schemaCommands;

    public EntityListInput(
            ReadableGroups groups,
            List<InputEntity> nodes,
            List<InputEntity> relationships,
            SchemaCommandSource schemaCommands) {
        this(IdType.INTEGER, DEFAULT_BATCH_SIZE, groups, nodes, relationships, schemaCommands);
    }

    public EntityListInput(
            IdType idType,
            int batchSize,
            ReadableGroups groups,
            List<InputEntity> nodes,
            List<InputEntity> relationships,
            SchemaCommandSource schemaCommands) {
        this.idType = idType;
        this.batchSize = batchSize;
        this.groups = groups;
        this.nodes = nodes;
        this.relationships = relationships;
        this.schemaCommands = schemaCommands;
    }

    @Override
    public InputIterable nodes(Collector badCollector) {
        return new EntityListIterable(batchSize, nodes);
    }

    @Override
    public InputIterable relationships(Collector badCollector) {
        return new EntityListIterable(batchSize, relationships);
    }

    @Override
    public IdType idType() {
        return idType;
    }

    @Override
    public ReadableGroups groups() {
        return groups;
    }

    @Override
    public SchemaCommandSource schemaCommandSource() {
        return schemaCommands;
    }

    @Override
    public Estimates validateAndEstimate(PropertySizeCalculator valueSizeCalculator, int numberOfThreads) {
        long nodeProperties =
                nodes.stream().mapToLong(entity -> entity.properties.size()).sum();
        long relationshipProperties = relationships.stream()
                .mapToLong(entity -> entity.properties.size())
                .sum();
        long labels = nodes.stream().mapToLong(entity -> entity.labels().length).sum();
        return Input.knownEstimates(
                nodes.size(),
                relationships.size(),
                nodeProperties,
                relationshipProperties,
                nodeProperties * Long.BYTES,
                relationshipProperties * Long.BYTES,
                labels,
                false,
                false);
    }

    @Override
    public boolean containsVectorData() {
        return false;
    }

    private record EntityListIterable(int batchSize, List<InputEntity> entities) implements InputIterable {
        @Override
        public InputIterator iterator() {
            return new EntityListIterator(batchSize, entities);
        }
    }

    private static class EntityListIterator implements InputIterator {
        private final List<InputEntity> entities;
        private final int batchSize;
        private int cursor;

        EntityListIterator(int batchSize, List<InputEntity> entities) {
            this.entities = entities;
            this.batchSize = batchSize;
        }

        @Override
        public InputChunk newChunk() {
            return new EntityListChunk();
        }

        @Override
        public synchronized boolean next(InputChunk chunk) {
            if (cursor >= entities.size()) {
                return false;
            }
            int to = Math.min(entities.size(), cursor + batchSize);
            ((EntityListChunk) chunk).feed(entities.subList(cursor, to));
            cursor = to;
            return true;
        }

        @Override
        public void close() {}
    }

    private static class EntityListChunk implements InputChunk {
        private Iterator<InputEntity> entities;

        void feed(List<InputEntity> entities) {
            this.entities = entities.iterator();
        }

        @Override
        public boolean next(InputEntityVisitor visitor) throws IOException {
            if (!entities.hasNext()) {
                return false;
            }
            entities.next().replayOnto(visitor);
            return true;
        }

        @Override
        public void close() {}
    }
}
