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
import java.util.Optional;

public interface StructRegistry<S> {

    /**
     * Creates a new empty struct registry factory.
     *
     * @param <S> a struct type.
     * @return a registry factory.
     */
    static <S> Builder<S> builder() {
        return ImmutableStructRegistry.emptyBuilder();
    }

    /**
     * Retrieves an empty struct registry which will reject any struct tags or payloads given to it.
     *
     * @param <S> an arbitrary struct type.
     * @return a struct registry.
     */
    static <S> StructRegistry<S> empty() {
        return EmptyStructRegistry.getInstance();
    }

    /**
     * Returns a builder which mimics the configuration of this registry.
     *
     * @return a registry builder.
     */
    Builder<S> builderOf();

    /**
     * Retrieves the registered struct reader for a given header.
     *
     * @param header a struct header.
     * @return a struct reader or, if none of the registered readers matches, an empty optional.
     */
    Optional<? extends StructReader<? extends S>> getReader(StructHeader header);

    /**
     * Retrieves the registered struct writer for a given payload object.
     *
     * @param payload a struct payload.
     * @param <O>     a struct POJO type.
     * @return a struct writer or, if none of the registered writers matches, an empty optional.
     */
    <O extends S> Optional<? extends StructWriter<? super O>> getWriter(O payload);

    /**
     * Provides a factory for arbitrary immutable registry instances.
     *
     * @param <S> a struct base type.
     */
    interface Builder<S> {

        /**
         * Creates a new registry using a snapshot of the configuration present within this builder.
         *
         * @return a struct registry.
         */
        StructRegistry<S> build();

        /**
         * Registers a new reader with this builder using its self-identified tag.
         *
         * @param reader a reader.
         * @return a reference to this builder.
         */
        default Builder<S> register(StructReader<? extends S> reader) {
            return this.register(reader.getTag(), reader);
        }

        /**
         * Registers a new reader for a given specific tag.
         *
         * @param tag    a structure tag.
         * @param reader a reader implementation.
         * @return a reference to this builder.
         */
        Builder<S> register(short tag, StructReader<? extends S> reader);

        /**
         * Registers a set of readers.
         *
         * @param readers a reader implementation.
         * @return a reference to this builder.
         */
        default Builder<S> registerReaders(Map<Short, StructReader<? extends S>> readers) {
            readers.forEach(this::register);
            return this;
        }

        /**
         * Registers a new writer with this builder using its self-identified return type.
         *
         * @param writer a writer implementation.
         * @param <T>    a value type.
         * @return a reference to this builder.
         */
        default <T extends S> Builder<S> register(StructWriter<T> writer) {
            return this.register(writer.getType(), writer);
        }

        /**
         * Registers a writer for a given base type.
         *
         * @param type   a specific value type.
         * @param writer a writer implementation.
         * @param <T>    a value type.
         * @return a reference to this builder.
         */
        <T extends S> Builder<S> register(Class<T> type, StructWriter<? super T> writer);

        /**
         * Registers a set of writers.
         *
         * @param writers a map of writers.
         * @return a reference to this builder.
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        default Builder<S> registerWriters(Map<Class<?>, StructWriter<? super S>> writers) {
            writers.forEach((type, writer) -> this.register((Class) type, writer));
            return this;
        }
    }
}
