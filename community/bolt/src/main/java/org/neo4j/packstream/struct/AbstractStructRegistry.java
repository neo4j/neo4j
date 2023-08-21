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
package org.neo4j.packstream.struct;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractStructRegistry<CTX, S> implements StructRegistry<CTX, S> {
    protected final Map<Short, StructReader<? super CTX, ? extends S>> tagToReaderMap;
    protected final Map<Class<?>, StructWriter<? super CTX, ? super S>> typeToWriterMap;

    public AbstractStructRegistry(
            Map<Short, StructReader<? super CTX, ? extends S>> tagToReaderMap,
            Map<Class<?>, StructWriter<? super CTX, ? super S>> typeToWriterMap) {
        this.tagToReaderMap = tagToReaderMap;
        this.typeToWriterMap = typeToWriterMap;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <C extends CTX> StructRegistry.Builder<C, S> builderOf() {
        return StructRegistry.<C, S>builder()
                .registerReaders((Map) this.tagToReaderMap)
                .registerWriters(this.typeToWriterMap);
    }

    @Override
    public Optional<? extends StructReader<? super CTX, ? extends S>> getReader(StructHeader header) {
        return Optional.ofNullable(this.tagToReaderMap.get(header.tag()));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <O extends S> Optional<? extends StructWriter<? super CTX, ? super O>> getWriter(O payload) {
        var payloadType = payload.getClass();

        var directMatch = (StructWriter<? super CTX, ? super O>) this.typeToWriterMap.get(payloadType);
        if (directMatch != null) {
            return Optional.of(directMatch);
        }

        return this.typeToWriterMap.entrySet().stream()
                .filter(entry -> entry.getKey().isAssignableFrom(payloadType))
                .map(entry -> (StructWriter<? super CTX, ? super O>) entry.getValue())
                .findAny();
    }

    public abstract static class Builder<CTX, S> implements StructRegistry.Builder<CTX, S> {
        protected final Map<Short, StructReader<? super CTX, ? extends S>> tagToReaderMap;
        protected final Map<Class<?>, StructWriter<? super CTX, ? super S>> typeToWriterMap;

        protected Builder() {
            this(Collections.emptyMap(), Collections.emptyMap());
        }

        protected Builder(
                Map<Short, StructReader<? super CTX, ? extends S>> tagToReaderMap,
                Map<Class<?>, StructWriter<? super CTX, ? super S>> typeToWriterMap) {
            this.tagToReaderMap = new HashMap<>(tagToReaderMap);
            this.typeToWriterMap = new HashMap<>(typeToWriterMap);
        }

        @Override
        public StructRegistry.Builder<CTX, S> register(short tag, StructReader<? super CTX, ? extends S> reader) {
            this.tagToReaderMap.put(tag, reader);
            return this;
        }

        @Override
        public StructRegistry.Builder<CTX, S> unregisterReader(short tag) {
            this.tagToReaderMap.remove(tag);
            return this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends S> StructRegistry.Builder<CTX, S> register(
                Class<T> type, StructWriter<? super CTX, ? super T> writer) {
            this.typeToWriterMap.put(type, (StructWriter<? super CTX, ? super S>) writer);
            return this;
        }
    }
}
