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
package org.neo4j.index.internal.gbptree;

import org.neo4j.common.DependencyResolver;

/**
 * Default {@link TreeNodeSelector} creating fixed or dynamic size node behaviours.
 */
public class DefaultTreeNodeSelector {

    /**
     * Creates instances for fixed size node behaviours.
     */
    private static final TreeNodeSelector.Factory FIXED = new TreeNodeSelector.Factory() {

        static final byte FORMAT_IDENTIFIER = 2;
        static final byte FORMAT_VERSION = 0;

        @Override
        public <KEY, VALUE> LeafNodeBehaviour<KEY, VALUE> createLeafBehaviour(
                int payloadSize,
                Layout<KEY, VALUE> layout,
                OffloadStore<KEY, VALUE> offloadStore,
                DependencyResolver dependencyResolver) {
            return new LeafNodeFixedSize<>(payloadSize, layout);
        }

        @Override
        public <KEY, VALUE> InternalNodeBehaviour<KEY> createInternalBehaviour(
                int payloadSize,
                Layout<KEY, VALUE> layout,
                OffloadStore<KEY, VALUE> offloadStore,
                DependencyResolver dependencyResolver) {
            return new InternalNodeFixedSize<>(payloadSize, layout);
        }

        @Override
        public byte formatIdentifier() {
            return FORMAT_IDENTIFIER;
        }

        @Override
        public byte formatVersion() {
            return FORMAT_VERSION;
        }
    };

    /**
     * Creates instances for dynamic size node behaviours.
     */
    private static final TreeNodeSelector.Factory DYNAMIC = new TreeNodeSelector.Factory() {
        static final byte FORMAT_IDENTIFIER = 3;
        static final byte FORMAT_VERSION = 0;

        @Override
        public <KEY, VALUE> LeafNodeBehaviour<KEY, VALUE> createLeafBehaviour(
                int payloadSize,
                Layout<KEY, VALUE> layout,
                OffloadStore<KEY, VALUE> offloadStore,
                DependencyResolver dependencyResolver) {
            return new LeafNodeDynamicSize<>(payloadSize, layout, offloadStore);
        }

        @Override
        public <KEY, VALUE> InternalNodeBehaviour<KEY> createInternalBehaviour(
                int payloadSize,
                Layout<KEY, VALUE> layout,
                OffloadStore<KEY, VALUE> offloadStore,
                DependencyResolver dependencyResolver) {
            return new InternalNodeDynamicSize<>(payloadSize, layout, offloadStore);
        }

        @Override
        public byte formatIdentifier() {
            return FORMAT_IDENTIFIER;
        }

        @Override
        public byte formatVersion() {
            return FORMAT_VERSION;
        }
    };

    /**
     * Returns {@link TreeNodeSelector} that selects a format based on the given {@link Layout}.
     *
     * @return a {@link TreeNodeSelector} capable of instantiating the selected format.
     */
    public static TreeNodeSelector selector() {
        // For now the selection is done in a simple fashion, by looking at layout.fixedSize().
        return (Layout<?, ?> layout) -> layout.fixedSize() ? FIXED : DYNAMIC;
    }
}
