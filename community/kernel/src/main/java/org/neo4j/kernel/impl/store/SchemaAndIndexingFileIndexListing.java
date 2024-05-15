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
package org.neo4j.kernel.impl.store;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.storageengine.api.StoreFileMetadata;

public class SchemaAndIndexingFileIndexListing {

    private final IndexingService indexingService;

    SchemaAndIndexingFileIndexListing(IndexingService indexingService) {
        this.indexingService = indexingService;
    }

    Resource gatherSchemaIndexFiles(Collection<StoreFileMetadata> targetFiles) throws IOException {
        ResourceIterator<Path> snapshot = indexingService.snapshotIndexFiles();
        getSnapshotFilesMetadata(snapshot, targetFiles);
        // Intentionally don't close the snapshot here, return it for closing by the consumer of
        // the targetFiles list.
        return snapshot;
    }

    private static void getSnapshotFilesMetadata(
            ResourceIterator<Path> snapshot, Collection<StoreFileMetadata> targetFiles) {
        snapshot.stream().map(StoreFileMetadata::new).forEach(targetFiles::add);
    }
}
