/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.api.index;

import java.util.function.Consumer;

import org.neo4j.kernel.api.index.IndexProvider;

/**
 * Contains mapping from {@link IndexProvider.Descriptor} or provider name to {@link IndexProvider}.
 */
public interface IndexProviderMap
{
    /**
     * Looks up and returns the {@link IndexProvider} for the given {@link IndexProvider.Descriptor}.
     *
     * @param providerDescriptor the descriptor identifying the {@link IndexProvider}.
     * @return the {@link IndexProvider} with the given {@link IndexProvider.Descriptor}.
     * @throws IndexProviderNotFoundException if no such {@link IndexProvider} was found.
     */
    IndexProvider lookup( IndexProvider.Descriptor providerDescriptor ) throws IndexProviderNotFoundException;

    /**
     * Looks up and returns the {@link IndexProvider} for the given index provider name. The name is what
     * an {@link IndexProvider.Descriptor#name()} call would return.
     *
     * @param providerDescriptorName the descriptor name identifying the {@link IndexProvider}.
     * @return the {@link IndexProvider} with the given name.
     * @throws IndexProviderNotFoundException if no such {@link IndexProvider} was found.
     */
    IndexProvider lookup( String providerDescriptorName ) throws IndexProviderNotFoundException;

    /**
     * There's always a default {@link IndexProvider}, this method returns it.
     *
     * @return the default index provider for this instance.
     */
    IndexProvider getDefaultProvider();

    /**
     * Visits all the {@link IndexProvider} with the visitor.
     *
     * @param visitor {@link Consumer} visiting all the {@link IndexProvider index providers} in this map.
     */
    void accept( Consumer<IndexProvider> visitor );
}
