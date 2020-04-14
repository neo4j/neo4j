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
package org.neo4j.cypher.internal.codegen;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.values.storable.Values;

import static org.neo4j.cypher.internal.codegen.CompiledConversionUtils.makeValueNeoSafe;
import static org.neo4j.internal.kernel.api.IndexQuery.exact;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;

/**
 * Utility for dealing with indexes from compiled code
 */
public final class CompiledIndexUtils
{
    /**
     * Do not instantiate this class
     */
    private CompiledIndexUtils()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Performs an index seek.
     *
     * @param read The Read instance to use for seeking
     * @param cursors Used for cursor allocation
     * @param index A reference to an index
     * @param value The value to seek for
     * @param cursorTracer underlying page cursor tracer
     * @return A cursor positioned at the data found in index.
     */
    public static NodeValueIndexCursor indexSeek( Read read, CursorFactory cursors, IndexDescriptor index, Object value, PageCursorTracer cursorTracer )
            throws KernelException
    {
        assert index.schema().getPropertyIds().length == 1;
        if ( value == Values.NO_VALUE || value == null )
        {
            return NodeValueIndexCursor.EMPTY;
        }
        else
        {
            NodeValueIndexCursor cursor = cursors.allocateNodeValueIndexCursor( cursorTracer );
            IndexQuery.ExactPredicate query = exact( index.schema().getPropertyIds()[0], makeValueNeoSafe( value ) );
            IndexReadSession indexSession = read.indexReadSession( index );
            read.nodeIndexSeek( indexSession, cursor, unconstrained(), query );
            return cursor;
        }
    }
}
