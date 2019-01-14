/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.values.storable.Values;

import static org.neo4j.cypher.internal.codegen.CompiledConversionUtils.makeValueNeoSafe;
import static org.neo4j.internal.kernel.api.IndexQuery.exact;

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
     * @return A cursor positioned at the data found in index.
     */
    public static NodeValueIndexCursor indexSeek( Read read, CursorFactory cursors, CapableIndexReference index, Object value )
            throws KernelException
    {
        assert index.properties().length == 1;
        if ( value == Values.NO_VALUE || value == null )
        {
            return NodeValueIndexCursor.EMPTY;
        }
        else
        {
            NodeValueIndexCursor cursor = cursors.allocateNodeValueIndexCursor();
            IndexQuery.ExactPredicate query = exact( index.properties()[0], makeValueNeoSafe( value ) );
            read.nodeIndexSeek( index, cursor, IndexOrder.NONE, query );
            return cursor;
        }
    }
}
