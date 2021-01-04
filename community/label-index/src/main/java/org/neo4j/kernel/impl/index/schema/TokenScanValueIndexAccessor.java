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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.schema.IndexOrder;

/**
 * Base class for iterator and index-progressor of token scans.
 */
abstract class TokenScanValueIndexAccessor
{
    /**
     * {@link Seeker} to lazily read new {@link TokenScanValue} from.
     */
    protected final Seeker<TokenScanKey,TokenScanValue> cursor;

    /**
     * Current base entityId, i.e. the {@link TokenScanKey#idRange} of the current {@link TokenScanKey}.
     */
    long baseEntityId;
    /**
     * Bit set of the current {@link TokenScanValue}.
     */
    protected long bits;
    /**
     * TokenId of previously retrieved {@link TokenScanKey}, for debugging and asserting purposes.
     */
    private int prevToken = -1;
    /**
     * IdRange of previously retrieved {@link TokenScanKey}, for debugging and asserting purposes.
     */
    private long prevRange = -1;
    /**
     * Indicate provided cursor has been closed.
     */
    protected boolean closed;

    TokenScanValueIndexAccessor( Seeker<TokenScanKey,TokenScanValue> cursor )
    {
        this.cursor = cursor;
    }

    boolean keysInOrder( TokenScanKey key, IndexOrder order )
    {
        if ( order == IndexOrder.NONE )
        {
            return true;
        }
        else if ( prevToken != -1 && prevRange != -1 && order == IndexOrder.ASCENDING )
        {
            assert key.tokenId >= prevToken : "Expected to get ascending ordered results, got " + key +
                                              " where previous token was " + prevToken;
            assert key.idRange > prevRange : "Expected to get ascending ordered results, got " + key +
                                             " where previous range was " + prevRange;
        }
        else if ( prevToken != -1 && prevRange != -1 && order == IndexOrder.DESCENDING )
        {
            assert key.tokenId <= prevToken : "Expected to get descending ordered results, got " + key +
                                              " where previous token was " + prevToken;
            assert key.idRange < prevRange : "Expected to get descending ordered results, got " + key +
                                             " where previous range was " + prevRange;
        }
        prevToken = key.tokenId;
        prevRange = key.idRange;
        // Made as a method returning boolean so that it can participate in an assert-call.
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
