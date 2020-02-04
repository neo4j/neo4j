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
package org.neo4j.kernel.api.impl.fulltext;

import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.values.storable.Value;

class FulltextIndexProgressor implements IndexProgressor
{
    private final ValuesIterator itr;
    private final EntityValueClient client;
    private long limit;

    FulltextIndexProgressor( ValuesIterator itr, EntityValueClient client, IndexQueryConstraints constraints )
    {
        this.itr = itr;
        this.client = client;
        if ( constraints.skip().isPresent() )
        {
            long skip = constraints.skip().getAsLong();
            while ( skip > 0 && itr.hasNext() )
            {
                itr.next();
                skip--;
            }
        }
        if ( constraints.limit().isPresent() )
        {
            this.limit = constraints.limit().getAsLong();
        }
        else
        {
            this.limit = Long.MAX_VALUE;
        }
    }

    @Override
    public boolean next()
    {
        if ( !itr.hasNext() || limit == 0 )
        {
            return false;
        }
        boolean accepted;
        do
        {
            long entityId = itr.next();
            float score = itr.currentScore();
            accepted = client.acceptEntity( entityId, score, (Value[]) null );
        }
        while ( !accepted && itr.hasNext() );
        limit--;
        return accepted;
    }

    @Override
    public void close()
    {
    }
}
