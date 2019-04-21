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
package org.neo4j.internal.index.label;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.index.internal.gbptree.Seeker;

/**
 * Base class for iterator and index-progressor of label scans.
 */
abstract class LabelScanValueIndexAccessor
{
    /**
     * {@link Seeker} to lazily read new {@link LabelScanValue} from.
     */
    protected final Seeker<LabelScanKey,LabelScanValue> cursor;

    /**
     * Current base nodeId, i.e. the {@link LabelScanKey#idRange} of the current {@link LabelScanKey}.
     */
    long baseNodeId;
    /**
     * Bit set of the current {@link LabelScanValue}.
     */
    protected long bits;
    /**
     * LabelId of previously retrieved {@link LabelScanKey}, for debugging and asserting purposes.
     */
    private int prevLabel = -1;
    /**
     * IdRange of previously retrieved {@link LabelScanKey}, for debugging and asserting purposes.
     */
    private long prevRange = -1;
    /**
     * Indicate provided cursor has been closed.
     */
    protected boolean closed;

    LabelScanValueIndexAccessor( Seeker<LabelScanKey,LabelScanValue> cursor )
    {
        this.cursor = cursor;
    }

    boolean keysInOrder( LabelScanKey key )
    {
        assert key.labelId >= prevLabel : "Expected to get ordered results, got " + key +
                " where previous label was " + prevLabel;
        assert key.idRange > prevRange : "Expected to get ordered results, got " + key +
                " where previous range was " + prevRange;
        prevLabel = key.labelId;
        prevRange = key.idRange;
        // Made as a method returning boolean so that it can participate in an assert call.
        return true;
    }

    public void close()
    {
        if ( !closed )
        {
            try
            {
                cursor.close();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
            finally
            {
                closed = true;
            }
        }
    }
}
