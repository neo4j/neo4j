/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.state;

import org.eclipse.collections.api.set.primitive.LongSet;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.function.Function;

import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStore;
import org.neo4j.storageengine.api.StoreFileMetadata;

public class SchemaAndIndexingFileIndexListing
{
    private static final Function<Path,StoreFileMetadata> toStoreFileMetadata = path -> new StoreFileMetadata( path, 1 );

    private final LabelScanStore labelScanStore;
    private final RelationshipTypeScanStore relationshipTypeScanStore;
    private final IndexingService indexingService;

    SchemaAndIndexingFileIndexListing( LabelScanStore labelScanStore, RelationshipTypeScanStore relationshipTypeScanStore,
            IndexingService indexingService )
    {
        this.labelScanStore = labelScanStore;
        this.relationshipTypeScanStore = relationshipTypeScanStore;
        this.indexingService = indexingService;
    }

    public LongSet getIndexIds()
    {
        return indexingService.getIndexIds();
    }

    Resource gatherSchemaIndexFiles( Collection<StoreFileMetadata> targetFiles ) throws IOException
    {
        ResourceIterator<Path> snapshot = indexingService.snapshotIndexFiles();
        getSnapshotFilesMetadata( snapshot, targetFiles );
        // Intentionally don't close the snapshot here, return it for closing by the consumer of
        // the targetFiles list.
        return snapshot;
    }

    Resource gatherLabelScanStoreFiles( Collection<StoreFileMetadata> targetFiles )
    {
        ResourceIterator<Path> snapshot = labelScanStore.snapshotStoreFiles();
        getSnapshotFilesMetadata( snapshot, targetFiles );
        // Intentionally don't close the snapshot here, return it for closing by the consumer of
        // the targetFiles list.
        return snapshot;
    }

    Resource gatherRelationshipTypeScanStoreFiles( Collection<StoreFileMetadata> targetFiles )
    {
        ResourceIterator<Path> snapshot = relationshipTypeScanStore.snapshotStoreFiles();
        getSnapshotFilesMetadata( snapshot, targetFiles );
        // Intentionally don't close the snapshot here, return it for closing by the consumer of
        // the targetFiles list.
        return snapshot;
    }

    private void getSnapshotFilesMetadata( ResourceIterator<Path> snapshot, Collection<StoreFileMetadata> targetFiles )
    {
        snapshot.stream().map( toStoreFileMetadata ).forEach( targetFiles::add );
    }
}
