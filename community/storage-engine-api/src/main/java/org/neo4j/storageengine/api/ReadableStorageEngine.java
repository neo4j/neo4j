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

import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * A {@link StorageEngine} that only has the reading parts, i.e. ability to instantiate {@link #newReader() readers}.
 */
public interface ReadableStorageEngine extends Lifecycle
{
    /**
     * Creates a new {@link StorageReader} for reading committed data from the underlying storage.
     * The returned instance is intended to be used by one transaction at a time, although can and should be reused
     * for multiple transactions.
     *
     * @return an interface for accessing data in the storage.
     */
    StorageReader newReader();
}
