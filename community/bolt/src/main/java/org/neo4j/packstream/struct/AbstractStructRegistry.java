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
package org.neo4j.packstream.struct;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractStructRegistry<S> implements StructRegistry<S> {
    protected final Map<Short, StructReader<? extends S>> tagToReaderMap;
    protected final Map<Class<?>, StructWriter<? super S>> typeToWriterMap;

    public AbstractStructRegistry(
            Map<Short, StructReader<? extends S>> tagToReaderMap,
            Map<Class<?>, StructWriter<? super S>> typeToWriterMap) {
        this.tagToReaderMap = tagToReaderMap;
        this.typeToWriterMap = typeToWriterMap;
    }

    @Override
    public StructRegistry.Builder<S> builderOf() {
        return StructRegistry.<S>builder().registerReaders(this.tagToReaderMap).registerWriters(this.typeToWriterMap);
    }

    @Override
    public Optional<? extends StructReader<? extends S>> getReader(StructHeader header) {
        return Optional.ofNullable(this.tagToReaderMap.get(header.tag()));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <O extends S> Optional<? extends StructWriter<? super O>> getWriter(O payload) {
        var payloadType = payload.getClass();

        var directMatch = (StructWriter<? super O>) this.typeToWriterMap.get(payloadType);
        if (directMatch != null) {
            return Optional.of(directMatch);
        }

        return this.typeToWriterMap.entrySet().stream()
                .filter(entry -> entry.getKey().isAssignableFrom(payloadType))
                .map(entry -> (StructWriter<? super O>) entry.getValue())
                .findAny();
    }

    public abstract static class Builder<S> implements StructRegistry.Builder<S> {
        protected final Map<Short, StructReader<? extends S>> tagToReaderMap;
        protected final Map<Class<?>, StructWriter<? super S>> typeToWriterMap;

        protected Builder() {
            this(new HashMap<>(), new HashMap<>());
        }

        protected Builder(
                Map<Short, StructReader<? extends S>> tagToReaderMap,
                Map<Class<?>, StructWriter<? super S>> typeToWriterMap) {
            this.tagToReaderMap = new HashMap<>(tagToReaderMap);
            this.typeToWriterMap = new HashMap<>(typeToWriterMap);
        }

        @Override
        public StructRegistry.Builder<S> register(short tag, StructReader<? extends S> reader) {
            this.tagToReaderMap.put(tag, reader);
            return this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends S> StructRegistry.Builder<S> register(Class<T> type, StructWriter<? super T> writer) {
            this.typeToWriterMap.put(type, (StructWriter<? super S>) writer);
            return this;
        }
    }
}
