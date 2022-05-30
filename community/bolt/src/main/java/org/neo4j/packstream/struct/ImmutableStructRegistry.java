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

import java.util.Map;

public class ImmutableStructRegistry<S> extends AbstractStructRegistry<S> {

    private ImmutableStructRegistry(
            Map<Short, StructReader<? extends S>> tagToReaderMap,
            Map<Class<?>, StructWriter<? super S>> typeToWriterMap) {
        super(tagToReaderMap, typeToWriterMap);
    }

    /**
     * Creates a new empty builder capable of creating a new immutable struct registry.
     *
     * @param <S> a struct type.
     * @return an empty factory.
     */
    public static <S> ImmutableStructRegistry.Builder<S> emptyBuilder() {
        return new Builder<>();
    }

    @Override
    public StructRegistry.Builder<S> builderOf() {
        return new Builder<>(this.tagToReaderMap, this.typeToWriterMap);
    }

    public static class Builder<S> extends AbstractStructRegistry.Builder<S> {

        private Builder() {}

        private Builder(
                Map<Short, StructReader<? extends S>> tagToReaderMap,
                Map<Class<?>, StructWriter<? super S>> typeToWriterMap) {
            super(tagToReaderMap, typeToWriterMap);
        }

        @Override
        public ImmutableStructRegistry<S> build() {
            return new ImmutableStructRegistry<S>(Map.copyOf(this.tagToReaderMap), Map.copyOf(this.typeToWriterMap));
        }
    }
}
