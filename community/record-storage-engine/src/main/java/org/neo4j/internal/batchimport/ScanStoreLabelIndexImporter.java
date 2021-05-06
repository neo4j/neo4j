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
package org.neo4j.internal.batchimport;

import java.io.Closeable;
import java.io.IOException;

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.kernel.impl.index.schema.TokenScanWriter;

import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.storageengine.api.EntityTokenUpdate.tokenChanges;

public class ScanStoreLabelIndexImporter implements IndexImporter, Closeable
{
    private final TokenScanWriter nodeWriter;

    public ScanStoreLabelIndexImporter( LabelScanStore scanStore, CursorContext cursorContext )
    {
        this.nodeWriter = scanStore.newBulkAppendWriter( cursorContext );
    }

    @Override
    public void add( long entity, long[] tokens )
    {
        nodeWriter.write( tokenChanges( entity, EMPTY_LONG_ARRAY, tokens ) );
    }

    @Override
    public void close() throws IOException
    {
        nodeWriter.close();
    }
}
