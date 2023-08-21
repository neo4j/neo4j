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

import java.util.Map;

public class ImmutableStructRegistry<CTX, S> extends AbstractStructRegistry<CTX, S> {

    private ImmutableStructRegistry(
            Map<Short, StructReader<? super CTX, ? extends S>> tagToReaderMap,
            Map<Class<?>, StructWriter<? super CTX, ? super S>> typeToWriterMap) {
        super(tagToReaderMap, typeToWriterMap);
    }

    /**
     * Creates a new empty builder capable of creating a new immutable struct registry.
     *
     * @param <CTX> a context type.
     * @param <S> a struct type.
     * @return an empty factory.
     */
    public static <CTX, S> Builder<CTX, S> emptyBuilder() {
        return new Builder<>();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <C extends CTX> AbstractStructRegistry.Builder<C, S> builderOf() {
        return new Builder<>((Map) this.tagToReaderMap, this.typeToWriterMap);
    }

    public static class Builder<CTX, S> extends AbstractStructRegistry.Builder<CTX, S> {

        private Builder() {}

        private Builder(
                Map<Short, StructReader<? super CTX, ? extends S>> tagToReaderMap,
                Map<Class<?>, StructWriter<? super CTX, ? super S>> typeToWriterMap) {
            super(tagToReaderMap, typeToWriterMap);
        }

        @Override
        public ImmutableStructRegistry<CTX, S> build() {
            return new ImmutableStructRegistry<CTX, S>(
                    Map.copyOf(this.tagToReaderMap), Map.copyOf(this.typeToWriterMap));
        }
    }
}
