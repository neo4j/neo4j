/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.IOUtils.closeAll;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.ONLINE;

import java.io.IOException;
import java.nio.file.OpenOption;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.internal.batchimport.IndexImporter;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.IndexEntryUpdate;

public class TokenIndexImporter implements IndexImporter {
    private static final String INDEX_TOKEN_IMPORTER_TAG = "indexTokenImporter";
    private final IndexUpdater updater;
    private final IndexDescriptor index;
    private final TokenIndexAccessor accessor;
    private final CursorContext cursorContext;

    TokenIndexImporter(
            IndexDescriptor index,
            DatabaseLayout layout,
            FileSystemAbstraction fs,
            PageCache cache,
            CursorContextFactory contextFactory,
            ImmutableSet<OpenOption> openOptions) {
        this.index = index;
        this.accessor = tokenIndexAccessor(layout, fs, cache, contextFactory, openOptions);
        this.cursorContext = contextFactory.create(INDEX_TOKEN_IMPORTER_TAG);
        this.updater = accessor.newUpdater(ONLINE, cursorContext, false);
    }

    @Override
    public void add(long entity, long[] tokens) {
        try {
            updater.process(IndexEntryUpdate.change(entity, index, EMPTY_LONG_ARRAY, tokens));
        } catch (IndexEntryConflictException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        closeAll(updater, () -> accessor.force(cursorContext), accessor);
    }

    private TokenIndexAccessor tokenIndexAccessor(
            DatabaseLayout layout,
            FileSystemAbstraction fs,
            PageCache pageCache,
            CursorContextFactory contextFactory,
            ImmutableSet<OpenOption> openOptions) {
        var context = DatabaseIndexContext.builder(pageCache, fs, contextFactory, layout.getDatabaseName())
                .build();
        IndexDirectoryStructure indexDirectoryStructure = IndexDirectoryStructure.directoriesByProvider(
                        layout.databaseDirectory())
                .forProvider(TokenIndexProvider.DESCRIPTOR);
        IndexFiles indexFiles = TokenIndexProvider.indexFiles(index, fs, indexDirectoryStructure);
        return new TokenIndexAccessor(context, indexFiles, index, immediate(), openOptions);
    }
}
