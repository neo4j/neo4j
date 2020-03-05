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
package org.neo4j.internal.index.label;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.graphdb.Resource;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.kernel.api.AutoCloseablePlus;
import org.neo4j.kernel.api.index.IndexProgressor;

/**
 * {@link IndexProgressor} which steps over multiple {@link TokenScanValue} and for each
 * iterate over each set bit, returning actual node ids, i.e. {@code nodeIdRange+bitOffset}.
 *
 */
public class TokenScanValueIndexProgressor extends TokenScanValueIndexAccessor implements IndexProgressor, Resource
{
    private final NodeLabelClient client;

    TokenScanValueIndexProgressor( Seeker<TokenScanKey,TokenScanValue> cursor, NodeLabelClient client )
    {
        super( cursor );
        this.client = client;
    }

    /**
     *  Progress through the index until the next accepted entry.
     *
     *  Progress the cursor to the current {@link TokenScanValue}, if this is not accepted by the client or if current
     *  value has been exhausted it continues to the next {@link TokenScanValue} by progressing the {@link Seeker}.
     * @return <code>true</code> if it found an accepted entry, <code>false</code> otherwise
     */
    @Override
    public boolean next()
    {
        for ( ; ; )
        {
            while ( bits != 0 )
            {
                int delta = Long.numberOfTrailingZeros( bits );
                bits &= bits - 1;
                if ( client.acceptNode( baseNodeId + delta, null ) )
                {
                    return true;
                }
            }
            try
            {
                if ( !cursor.next() )
                {
                    close();
                    return false;
                }
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }

            TokenScanKey key = cursor.key();
            baseNodeId = key.idRange * TokenScanValue.RANGE_SIZE;
            bits = cursor.value().bits;

            //noinspection AssertWithSideEffects
            assert keysInOrder( key );
        }
    }

    @Override
    public void close()
    {
        super.close();
        if ( client instanceof AutoCloseablePlus )
        {
            ((AutoCloseablePlus) client).close();
        }
    }
}
