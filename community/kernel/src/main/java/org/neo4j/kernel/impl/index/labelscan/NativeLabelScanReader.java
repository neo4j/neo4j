/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.index.labelscan;

import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.Hit;
import org.neo4j.index.Index;
import org.neo4j.storageengine.api.schema.LabelScanReader;

class NativeLabelScanReader implements LabelScanReader
{
    private final Index<LabelScanKey,LabelScanValue> index;
    private final int rangeSize;
    private RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor;

    NativeLabelScanReader( Index<LabelScanKey,LabelScanValue> index, int rangeSize )
    {
        this.index = index;
        this.rangeSize = rangeSize;
    }

    @Override
    public void close()
    {
        try
        {
            ensureCurrentCursorClosed();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public PrimitiveLongIterator nodesWithLabel( int labelId )
    {
        LabelScanKey from = new LabelScanKey().set( labelId, 0 );
        LabelScanKey to = new LabelScanKey().set( labelId, Long.MAX_VALUE );
        try
        {
            ensureCurrentCursorClosed();
            cursor = index.seek( from, to );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        return new LabelScanValueIterator( rangeSize, cursor );
    }

    private void ensureCurrentCursorClosed() throws IOException
    {
        if ( cursor != null )
        {
            cursor.close();
        }
    }

    @Override
    public PrimitiveLongIterator labelsForNode( long nodeId )
    {
        throw new UnsupportedOperationException( "Use your db..." );
    }
}
