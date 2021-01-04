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
package org.neo4j.internal.batchimport;

import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.ProcessorStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.kernel.impl.index.schema.TokenScanWriter;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.io.IOUtils.closeAll;
import static org.neo4j.kernel.impl.store.NodeLabelsField.get;
import static org.neo4j.storageengine.api.EntityTokenUpdate.tokenChanges;

public class LabelIndexWriterStep extends ProcessorStep<NodeRecord[]>
{
    private static final String INDEX_WRITE_STEP_TAG = "indexWriteStep";
    private final TokenScanWriter writer;
    private final NodeStore nodeStore;
    private final PageCursorTracer cursorTracer;

    public LabelIndexWriterStep( StageControl control, Configuration config, LabelScanStore store,
            NodeStore nodeStore, PageCacheTracer pageCacheTracer )
    {
        super( control, "LABEL INDEX", config, 1, pageCacheTracer );
        this.cursorTracer = pageCacheTracer.createPageCursorTracer( INDEX_WRITE_STEP_TAG );
        this.writer = store.newBulkAppendWriter( cursorTracer );
        this.nodeStore = nodeStore;
    }

    @Override
    protected void process( NodeRecord[] batch, BatchSender sender, PageCursorTracer cursorTracer ) throws Throwable
    {
        for ( NodeRecord node : batch )
        {
            if ( node.inUse() )
            {
                writer.write( tokenChanges( node.getId(), EMPTY_LONG_ARRAY, get( node, nodeStore, cursorTracer ) ) );
            }
        }
        sender.send( batch );
    }

    @Override
    public void close() throws Exception
    {
        super.close();
        closeAll( writer, cursorTracer );
    }
}
