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
import java.util.Optional;

public interface StructRegistry<CTX, S> {

    /**
     * Creates a new empty struct registry factory.
     *
     * @param <CTX> a context object.
     * @param <S> a struct type.
     * @return a registry factory.
     */
    static <CTX, S> Builder<CTX, S> builder() {
        return ImmutableStructRegistry.emptyBuilder();
    }

    /**
     * Retrieves an empty struct registry which will reject any struct tags or payloads given to it.
     *
     * @param <CTX> a context object.
     * @param <S> an arbitrary struct type.
     * @return a struct registry.
     */
    static <CTX, S> StructRegistry<CTX, S> empty() {
        return EmptyStructRegistry.getInstance();
    }

    /**
     * Returns a builder which mimics the configuration of this registry.
     *
     * @param <C> a context type.
     * @return a registry builder.
     */
    <C extends CTX> Builder<C, S> builderOf();

    /**
     * Retrieves the registered struct reader for a given header.
     *
     * @param header a struct header.
     * @return a struct reader or, if none of the registered readers matches, an empty optional.
     */
    Optional<? extends StructReader<? super CTX, ? extends S>> getReader(StructHeader header);

    /**
     * Retrieves the registered struct writer for a given payload object.
     *
     * @param payload a struct payload.
     * @param <O>     a struct POJO type.
     * @return a struct writer or, if none of the registered writers matches, an empty optional.
     */
    <O extends S> Optional<? extends StructWriter<? super CTX, ? super O>> getWriter(O payload);

    /**
     * Provides a factory for arbitrary immutable registry instances.
     *
     * @param <CTX> a context type.
     * @param <S> a struct base type.
     */
    interface Builder<CTX, S> {

        /**
         * Creates a new registry using a snapshot of the configuration present within this builder.
         *
         * @return a struct registry.
         */
        StructRegistry<CTX, S> build();

        /**
         * Registers a new reader with this builder using its self-identified tag.
         *
         * @param reader a reader.
         * @return a reference to this builder.
         */
        default Builder<CTX, S> register(StructReader<? super CTX, ? extends S> reader) {
            return this.register(reader.getTag(), reader);
        }

        /**
         * Removes a previously registered reader.
         * @param reader a reader.
         * @return a reference to this builder.
         */
        default Builder<CTX, S> unregister(StructReader<? super CTX, ? extends S> reader) {
            return this.unregisterReader(reader.getTag());
        }

        /**
         * Registers a new reader for a given specific tag.
         *
         * @param tag    a structure tag.
         * @param reader a reader implementation.
         * @return a reference to this builder.
         */
        Builder<CTX, S> register(short tag, StructReader<? super CTX, ? extends S> reader);

        /**
         * Removes a previously registered reader for a given specific tag.
         * <p />
         * When no reader with the given tag has previously been registered, this method acts as a noop.
         *
         * @param tag a structure tag.
         * @return a reference to this builder.
         */
        StructRegistry.Builder<CTX, S> unregisterReader(short tag);

        /**
         * Registers a set of readers.
         *
         * @param readers a reader implementation.
         * @return a reference to this builder.
         */
        default Builder<CTX, S> registerReaders(Map<Short, StructReader<? super CTX, ? extends S>> readers) {
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
        default <T extends S> Builder<CTX, S> register(StructWriter<? super CTX, T> writer) {
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
        <T extends S> Builder<CTX, S> register(Class<T> type, StructWriter<? super CTX, ? super T> writer);

        /**
         * Registers a set of writers.
         *
         * @param writers a map of writers.
         * @return a reference to this builder.
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        default Builder<CTX, S> registerWriters(Map<Class<?>, StructWriter<? super CTX, ? super S>> writers) {
            writers.forEach((type, writer) -> this.register((Class) type, writer));
            return this;
        }
    }
}
