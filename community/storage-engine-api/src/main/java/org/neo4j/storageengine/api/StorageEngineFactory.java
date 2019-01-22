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
package org.neo4j.storageengine.api;

import org.neo4j.common.DependencyResolver;
import org.neo4j.common.DependencySatisfier;

/**
 * A factory suitable for something like service-loading to load {@link StorageEngine} instances.
 */
public interface StorageEngineFactory
{
    /**
     * Instantiates a {@link StorageEngine} where all dependencies can be retrieved from the supplied {@code dependencyResolver}.
     *
     * @param dependencyResolver {@link DependencyResolver} used to get all required dependencies to instantiate the {@link StorageEngine}.
     * @param dependencySatisfier {@link DependencySatisfier} providing ways to let the storage engine provide dependencies
     * back to the instantiator. This is a hack with the goal to be removed completely when graph storage abstraction in kernel is properly in place.
     * @return the instantiated {@link StorageEngine}.
     */
    StorageEngine instantiate( DependencyResolver dependencyResolver, DependencySatisfier dependencySatisfier );
}
