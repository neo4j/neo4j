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

import static org.neo4j.internal.helpers.collection.Iterators.resourceIterator;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.CommonDatabaseStores;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.util.MultiResource;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageFileSelection;

public class StoreFileListing implements FileStoreProviderRegistry {
    private final DatabaseLayout databaseLayout;
    private final FileSystemAbstraction fs;
    private final LogFiles logFiles;
    private final StorageEngine storageEngine;
    private final SchemaAndIndexingFileIndexListing fileIndexListing;
    private final Collection<StoreFileProvider> additionalProviders;

    public StoreFileListing(
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fs,
            LogFiles logFiles,
            IndexingService indexingService,
            StorageEngine storageEngine) {
        this.databaseLayout = databaseLayout;
        this.fs = fs;
        this.logFiles = logFiles;
        this.storageEngine = storageEngine;
        this.fileIndexListing = new SchemaAndIndexingFileIndexListing(indexingService);
        this.additionalProviders = new CopyOnWriteArraySet<>();
    }

    public Builder builder() {
        return new Builder();
    }

    @Override
    public void registerStoreFileProvider(StoreFileProvider provider) {
        additionalProviders.add(provider);
    }

    private void placeMetaDataStoreLast(List<Path> files) {
        int index = 0;
        for (Path file : files) {
            if (databaseLayout
                    .pathForStore(CommonDatabaseStores.METADATA)
                    .baseSegment()
                    .equals(file)) {
                break;
            }
            index++;
        }
        if (index < files.size() - 1) {
            Path metaDataStoreFile = files.remove(index);
            files.add(metaDataStoreFile);
        }
    }

    private void gatherLogFiles(Collection<Path> files) throws IOException {
        Collections.addAll(files, logFiles.logFiles());
    }

    public class Builder {
        private boolean excludeLogFiles;
        private boolean excludeAtomicStorageFiles;
        private boolean excludeReplayableStorageFiles;
        private boolean excludeSchemaIndexStoreFiles;
        private boolean excludeAdditionalProviders;
        private boolean excludeIdFiles;

        private Builder() {}

        private void excludeAll(boolean initiateInclusive) {
            this.excludeLogFiles = initiateInclusive;
            this.excludeAtomicStorageFiles = initiateInclusive;
            this.excludeReplayableStorageFiles = initiateInclusive;
            this.excludeSchemaIndexStoreFiles = initiateInclusive;
            this.excludeAdditionalProviders = initiateInclusive;
            this.excludeIdFiles = initiateInclusive;
        }

        public Builder excludeAll() {
            excludeAll(true);
            return this;
        }

        public Builder includeAll() {
            excludeAll(false);
            return this;
        }

        public Builder excludeLogFiles() {
            excludeLogFiles = true;
            return this;
        }

        public Builder excludeAtomicStorageFiles() {
            excludeAtomicStorageFiles = true;
            return this;
        }

        public Builder excludeReplayableStorageFiles() {
            excludeReplayableStorageFiles = true;
            return this;
        }

        public Builder excludeSchemaIndexStoreFiles() {
            excludeSchemaIndexStoreFiles = true;
            return this;
        }

        public Builder excludeAdditionalProviders() {
            excludeAdditionalProviders = true;
            return this;
        }

        public Builder excludeIdFiles() {
            excludeIdFiles = true;
            return this;
        }

        public Builder includeLogFiles() {
            excludeLogFiles = false;
            return this;
        }

        public Builder includeAtomicStorageFiles() {
            excludeAtomicStorageFiles = false;
            return this;
        }

        public Builder includeReplayableStorageFiles() {
            excludeReplayableStorageFiles = false;
            return this;
        }

        public Builder includeSchemaIndexStoreFiles() {
            excludeSchemaIndexStoreFiles = false;
            return this;
        }

        public Builder includeAdditionalProviders() {
            excludeAdditionalProviders = false;
            return this;
        }

        public Builder includeIdFiles() {
            excludeIdFiles = false;
            return this;
        }

        public ResourceIterator<Path> build() throws IOException {
            List<Path> files = new ArrayList<>();
            List<Resource> resources = new ArrayList<>();
            try {
                if (!excludeLogFiles) {
                    gatherLogFiles(files);
                }

                files.addAll(storageEngine.listStorageFiles(new StorageFileSelection(
                        !excludeAtomicStorageFiles, !excludeReplayableStorageFiles, !excludeIdFiles)));

                if (!excludeSchemaIndexStoreFiles) {
                    resources.add(fileIndexListing.gatherSchemaIndexFiles(fs, files));
                }
                if (!excludeAdditionalProviders) {
                    for (StoreFileProvider additionalProvider : additionalProviders) {
                        resources.add(additionalProvider.addFilesTo(files));
                    }
                }
                placeMetaDataStoreLast(files);
            } catch (IOException e) {
                try {
                    IOUtils.closeAll(resources);
                } catch (IOException e1) {
                    throw Exceptions.chain(e, e1);
                }
                throw e;
            }

            return resourceIterator(files.iterator(), new MultiResource(resources));
        }
    }
}
