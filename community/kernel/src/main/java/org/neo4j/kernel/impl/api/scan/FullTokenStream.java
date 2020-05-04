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
package org.neo4j.kernel.impl.api.scan;

import java.io.IOException;

import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.index.label.FullStoreChangeStream;
import org.neo4j.internal.index.label.TokenScanWriter;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.storageengine.api.EntityTokenUpdate;

/**
 * {@link FullStoreChangeStream} using a {@link IndexStoreView} to get its data.
 *
 * Connects the provided {@link TokenScanWriter writer} to the receiving end of a full store scan,
 * feeding {@link EntityTokenUpdate entity tokens} into the writer.
 */
public abstract class FullTokenStream implements FullStoreChangeStream, Visitor<EntityTokenUpdate,IOException>
{
    private final IndexStoreView indexStoreView;
    private TokenScanWriter writer;
    private long count;

    FullTokenStream( IndexStoreView indexStoreView )
    {
        this.indexStoreView = indexStoreView;
    }

    abstract StoreScan<IOException> getStoreScan( IndexStoreView indexStoreView, Visitor<EntityTokenUpdate,IOException> tokenUpdateVisitor,
            PageCursorTracer cursorTracer );

    @Override
    public long applyTo( TokenScanWriter writer, PageCursorTracer cursorTracer ) throws IOException
    {
        // Keep the writer for using it in "visit"
        this.writer = writer;
        StoreScan<IOException> scan = getStoreScan( indexStoreView, this, cursorTracer );
        scan.run();
        return count;
    }

    @Override
    public boolean visit( EntityTokenUpdate update ) throws IOException
    {
        writer.write( update );
        count++;
        return false;
    }
}
