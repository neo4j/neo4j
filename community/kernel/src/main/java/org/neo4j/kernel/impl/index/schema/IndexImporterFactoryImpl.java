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
package org.neo4j.kernel.impl.index.schema;

import java.nio.file.OpenOption;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.batchimport.api.IndexImporter;
import org.neo4j.batchimport.api.IndexImporterFactory;
import org.neo4j.batchimport.api.IndexesCreator;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.BulkIndexCreationContext;
import org.neo4j.util.Preconditions;

public class IndexImporterFactoryImpl implements IndexImporterFactory {
    @Override
    public IndexImporter getImporter(
            IndexDescriptor index,
            DatabaseLayout layout,
            FileSystemAbstraction fs,
            PageCache pageCache,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            ImmutableSet<OpenOption> openOptions,
            StorageEngineIndexingBehaviour indexingBehaviour) {
        return new TokenIndexImporter(
                index, layout, fs, pageCache, contextFactory, pageCacheTracer, openOptions, indexingBehaviour);
    }

    @Override
    public IndexesCreator getCreator(CreationContext context) {
        Preconditions.checkState(
                context instanceof BulkIndexCreationContext,
                "Index creation requires an instance of BulkIndexCreationContext");
        return new BulkIndexesCreator((BulkIndexCreationContext) context);
    }
}
