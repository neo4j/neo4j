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

import java.util.Optional;

/**
 * Provides a no-op struct registry which rejects any struct values given to it.
 *
 * @param <S> an arbitrary value type.
 */
final class EmptyStructRegistry<S> implements StructRegistry<S> {
    @SuppressWarnings("rawtypes")
    private static final EmptyStructRegistry INSTANCE = new EmptyStructRegistry();

    private EmptyStructRegistry() {}

    @Override
    public Builder<S> builderOf() {
        return StructRegistry.builder();
    }

    @SuppressWarnings("unchecked")
    static <S> EmptyStructRegistry<S> getInstance() {
        return INSTANCE;
    }

    @Override
    public Optional<? extends StructReader<? extends S>> getReader(StructHeader header) {
        return Optional.empty();
    }

    @Override
    public <O extends S> Optional<? extends StructWriter<? super O>> getWriter(O payload) {
        return Optional.empty();
    }
}
