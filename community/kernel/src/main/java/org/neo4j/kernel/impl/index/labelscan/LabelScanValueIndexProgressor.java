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
package org.neo4j.kernel.impl.index.labelscan;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;

import org.neo4j.cursor.RawCursor;
import org.neo4j.graphdb.Resource;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.storageengine.api.schema.IndexProgressor;

/**
 * {@link IndexProgressor} which steps over multiple {@link LabelScanValue} and for each
 * iterate over each set bit, returning actual node ids, i.e. {@code nodeIdRange+bitOffset}.
 *
 */
public class LabelScanValueIndexProgressor extends LabelScanValueIndexAccessor implements IndexProgressor, Resource
{

    private final NodeLabelClient client;

    LabelScanValueIndexProgressor( RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor,
            Collection<RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException>> toRemoveFromWhenClosed,
            NodeLabelClient client )
    {
        super( toRemoveFromWhenClosed, cursor );
        this.client = client;
    }

    /**
     *  Progress through the index until the next accepted entry.
     *
     *  Progress the cursor to the current {@link LabelScanValue}, if this is not accepted by the client or if current
     *  value is exhausted it continues to the next {@link LabelScanValue}  from {@link RawCursor}.
     * @return <code>true</code> if an accepted entry was found, <code>false</code> otherwise
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

            Hit<LabelScanKey,LabelScanValue> hit = cursor.get();
            baseNodeId = hit.key().idRange * LabelScanValue.RANGE_SIZE;
            bits = hit.value().bits;

            //noinspection AssertWithSideEffects
            assert keysInOrder( hit.key() );
        }
    }
}
