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
 * Able to select implementation of {@link LeafNodeBehaviour} and {@link InternalNodeBehaviour} to use in different scenarios.
 */
public interface TreeNodeSelector {

    /**
     * Selects a format based on the given {@link Layout}.
     *
     * @param layout {@link Layout} dictating which {@link LeafNodeBehaviour} and {@link InternalNodeBehaviour} to instantiate.
     * @return a {@link Factory} capable of instantiating the selected format.
     */
    Factory selectByLayout(Layout<?, ?> layout);

    /**
     * Able to instantiate {@link LeafNodeBehaviour} and {@link InternalNodeBehaviour} of a specific format and version.
     */
    interface Factory {
        /**
         * Instantiates a {@link LeafNodeBehaviour} of a specific format and version that this factory represents.
         *
         * @param pageSize           page size, i.e. size of tree nodes.
         * @param layout             {@link Layout} that will be used in this format.
         * @param offloadStore       {@link OffloadStore} that could be used for larger entries
         * @param dependencyResolver {@link DependencyResolver} to access various database components
         * @return the instantiated {@link LeafNodeBehaviour}.
         */
        <KEY, VALUE> LeafNodeBehaviour<KEY, VALUE> createLeafBehaviour(
                int pageSize,
                Layout<KEY, VALUE> layout,
                OffloadStore<KEY, VALUE> offloadStore,
                DependencyResolver dependencyResolver);
        /**
         * Instantiates a {@link InternalNodeBehaviour} of a specific format and version that this factory represents.
         *
         * @param pageSize           page size, i.e. size of tree nodes.
         * @param layout             {@link Layout} that will be used in this format.
         * @param offloadStore       {@link OffloadStore} that could be used for larger entries
         * @param dependencyResolver {@link DependencyResolver} to access various database components
         * @return the instantiated {@link InternalNodeBehaviour}.
         */
        <KEY, VALUE> InternalNodeBehaviour<KEY> createInternalBehaviour(
                int pageSize,
                Layout<KEY, VALUE> layout,
                OffloadStore<KEY, VALUE> offloadStore,
                DependencyResolver dependencyResolver);

        /**
         * Specifies the format identifier of the physical layout of tree nodes.
         * A format identifier must be unique among all possible existing format identifiers.
         * It's used to differentiate between different types of formats.
         * On top of this a specific {@link #formatVersion() format version} can specify a version of this format.
         *
         * @return format identifier for the specific tree node that this factory represents.
         * Can return this w/o instantiating the node behaviour.
         */
        byte formatIdentifier();

        /**
         * Specifies the version of this particular {@link #formatIdentifier() format}. It must be unique
         * among all other versions of this {@link #formatIdentifier() format}.
         *
         * @return format version for the specific tree node that this factory represents.
         * Can return this w/o instantiating the node behaviour.
         */
        byte formatVersion();
    }
}
