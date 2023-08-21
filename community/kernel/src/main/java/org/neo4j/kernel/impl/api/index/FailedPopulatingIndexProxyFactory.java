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
package org.neo4j.kernel.impl.api.index;

import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;

import org.neo4j.kernel.api.index.MinimalIndexAccessor;
import org.neo4j.logging.InternalLogProvider;

public class FailedPopulatingIndexProxyFactory implements FailedIndexProxyFactory {
    private final IndexProxyStrategy indexProxyStrategy;
    private final MinimalIndexAccessor minimalIndexAccessor;
    private final InternalLogProvider logProvider;

    FailedPopulatingIndexProxyFactory(
            IndexProxyStrategy indexProxyStrategy,
            MinimalIndexAccessor minimalIndexAccessor,
            InternalLogProvider logProvider) {
        this.indexProxyStrategy = indexProxyStrategy;
        this.minimalIndexAccessor = minimalIndexAccessor;
        this.logProvider = logProvider;
    }

    @Override
    public IndexProxy create(Throwable failure) {
        return new FailedIndexProxy(indexProxyStrategy, minimalIndexAccessor, failure(failure), logProvider);
    }
}
