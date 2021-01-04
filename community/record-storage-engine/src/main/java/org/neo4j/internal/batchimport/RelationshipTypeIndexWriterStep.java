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
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStore;
import org.neo4j.kernel.impl.index.schema.TokenScanWriter;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.EntityTokenUpdate;

import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.io.IOUtils.closeAll;

public class RelationshipTypeIndexWriterStep extends ProcessorStep<RelationshipRecord[]>
{
    private static final String INDEX_WRITE_STEP_TAG = "indexWriteStep";
    private final PageCursorTracer cursorTracer;
    private final TokenScanWriter writer;

    public RelationshipTypeIndexWriterStep( StageControl control, Configuration config, RelationshipTypeScanStore relationshipTypeIndex,
            PageCacheTracer pageCacheTracer )
    {
        super( control, "RELATIONSHIP TYPE INDEX", config, 1, pageCacheTracer );
        this.cursorTracer = pageCacheTracer.createPageCursorTracer( INDEX_WRITE_STEP_TAG );
        this.writer = relationshipTypeIndex.newBulkAppendWriter( cursorTracer );
    }

    @Override
    protected void process( RelationshipRecord[] batch, BatchSender sender, PageCursorTracer cursorTracer ) throws Throwable
    {
        for ( RelationshipRecord relationship : batch )
        {
            if ( relationship.inUse() )
            {
                writer.write( EntityTokenUpdate.tokenChanges( relationship.getId(), EMPTY_LONG_ARRAY, new long[]{relationship.getType()} ) );
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
