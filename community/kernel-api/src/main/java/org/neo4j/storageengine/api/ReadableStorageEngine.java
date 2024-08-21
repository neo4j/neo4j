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
package org.neo4j.storageengine.api;

import java.nio.file.OpenOption;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * A minimal sub-set of a {@link StorageEngine} which can be used to read data from.
 * @see StorageEngine
 */
public interface ReadableStorageEngine {
    /**
     * Provide access level for underlying store cursors
     * @param initialContext cursor context to use for store cursors before reset call
     */
    StoreCursors createStorageCursors(CursorContext initialContext);

    /**
     * Creates a new {@link StorageReader} for reading committed data from the underlying storage.
     * The returned instance is intended to be used by one transaction at a time, although can and should be reused
     * for multiple transactions.
     *
     * @return an interface for accessing data in the storage.
     */
    StorageReader newReader();

    /**
     * @return which indexing behaviour this storage engine implementation has.
     * @see StorageEngineIndexingBehaviour
     */
    StorageEngineIndexingBehaviour indexingBehaviour();

    /**
     * Open options used for related store files and to be used for other files managed outside of StorageEngine but related to the same database
     * @return set of open options
     */
    default ImmutableSet<OpenOption> getOpenOptions() {
        return Sets.immutable.empty();
    }

    /**
     * @return cost characteristics for accessing data in this storage engine.
     */
    StorageEngineCostCharacteristics costCharacteristics();
}
