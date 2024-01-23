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
package org.neo4j.kernel.impl.api;

import static org.eclipse.collections.impl.block.factory.HashingStrategies.identityStrategy;

import org.eclipse.collections.api.factory.set.strategy.MutableHashingStrategySetFactory;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.impl.set.strategy.mutable.MutableHashingStrategySetFactoryImpl;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.exceptions.ResourceCloseFailureException;

public class CloseableResourceManager implements ResourceTracker {
    private static final MutableHashingStrategySetFactory SET_FACTORY = MutableHashingStrategySetFactoryImpl.INSTANCE;
    private MutableSet<AutoCloseable> closeableResources;

    // ResourceTracker

    @Override
    public final void registerCloseableResource(AutoCloseable closeable) {
        if (closeableResources == null) {
            closeableResources = SET_FACTORY.withInitialCapacity(identityStrategy(), 8);
        }
        closeableResources.add(closeable);
    }

    @Override
    public final void unregisterCloseableResource(AutoCloseable closeable) {
        if (closeableResources != null) {
            closeableResources.remove(closeable);
        }
    }

    // ResourceManager

    @Override
    public final void closeAllCloseableResources() {
        // Make sure we reset closeableResource before doing anything which may throw an exception that
        // _may_ result in a recursive call to this close-method
        if (closeableResources != null) {
            MutableSet<AutoCloseable> resources = this.closeableResources;
            closeableResources = null;

            IOUtils.close(ResourceCloseFailureException::new, resources);
        }
    }
}
