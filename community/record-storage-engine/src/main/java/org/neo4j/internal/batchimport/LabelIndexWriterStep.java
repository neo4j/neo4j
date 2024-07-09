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
package org.neo4j.internal.batchimport;

import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.io.IOUtils.closeAll;
import static org.neo4j.kernel.impl.store.NodeLabelsField.get;

import java.util.function.Function;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.IndexImporter;
import org.neo4j.batchimport.api.IndexImporterFactory;
import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class LabelIndexWriterStep extends IndexWriterStep<NodeRecord[]> {
    private static final String LABEL_INDEX_WRITE_STEP_TAG = "labelIndexWriteStep";
    private final long fromNodeId;
    private final CursorContext cursorContext;
    private final IndexImporter importer;
    private final NodeStore nodeStore;
    private final StoreCursors cachedStoreCursors;
    private final IndexImporter.Writer writer;

    public LabelIndexWriterStep(
            StageControl control,
            Configuration config,
            BatchingNeoStores neoStores,
            IndexImporterFactory indexImporterFactory,
            long fromNodeId,
            MemoryTracker memoryTracker,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            Function<CursorContext, StoreCursors> storeCursorsCreator) {
        super(control, "LABEL INDEX", config, 1, contextFactory);
        this.fromNodeId = fromNodeId;
        this.cursorContext = contextFactory.create(LABEL_INDEX_WRITE_STEP_TAG);
        this.importer = indexImporter(
                config.indexConfig(),
                indexImporterFactory,
                neoStores,
                NODE,
                memoryTracker,
                contextFactory,
                pageCacheTracer,
                storeCursorsCreator);
        this.writer = importer.writer(false);
        this.cachedStoreCursors = storeCursorsCreator.apply(cursorContext);
        this.nodeStore = neoStores.getNodeStore();
    }

    @Override
    protected void process(NodeRecord[] batch, BatchSender sender, CursorContext cursorContext) throws Throwable {
        cachedStoreCursors.reset(cursorContext);
        for (NodeRecord node : batch) {
            if (node.inUse() && node.getId() >= fromNodeId) {
                writer.add(node.getId(), get(node, nodeStore, cachedStoreCursors));
            }
        }
        sender.send(batch);
    }

    @Override
    public void close() throws Exception {
        super.close();
        closeAll(writer, importer, cursorContext, cachedStoreCursors);
    }
}
