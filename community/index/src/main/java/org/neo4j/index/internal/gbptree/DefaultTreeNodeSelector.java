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
package org.neo4j.index.internal.gbptree;

import org.neo4j.common.DependencyResolver;

/**
 * Able to select implementation of {@link TreeNode} to use in different scenarios, should be used in favor of directly
 * instantiating {@link TreeNode} instances.
 */
public class DefaultTreeNodeSelector {
    /**
     * Creates {@link TreeNodeFixedSize} instances.
     */
    private static final TreeNodeSelector.Factory FIXED = new TreeNodeSelector.Factory() {
        @Override
        public <KEY, VALUE> TreeNode<KEY, VALUE> create(
                int pageSize,
                Layout<KEY, VALUE> layout,
                OffloadStore<KEY, VALUE> offloadStore,
                DependencyResolver dependencyResolver) {
            return new TreeNodeFixedSize<>(pageSize, layout);
        }

        @Override
        public byte formatIdentifier() {
            return TreeNodeFixedSize.FORMAT_IDENTIFIER;
        }

        @Override
        public byte formatVersion() {
            return TreeNodeFixedSize.FORMAT_VERSION;
        }
    };

    /**
     * Creates {@link TreeNodeDynamicSize} instances.
     */
    private static final TreeNodeSelector.Factory DYNAMIC = new TreeNodeSelector.Factory() {
        @Override
        public <KEY, VALUE> TreeNode<KEY, VALUE> create(
                int pageSize,
                Layout<KEY, VALUE> layout,
                OffloadStore<KEY, VALUE> offloadStore,
                DependencyResolver dependencyResolver) {
            return new TreeNodeDynamicSize<>(pageSize, layout, offloadStore);
        }

        @Override
        public byte formatIdentifier() {
            return TreeNodeDynamicSize.FORMAT_IDENTIFIER;
        }

        @Override
        public byte formatVersion() {
            return TreeNodeDynamicSize.FORMAT_VERSION;
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
