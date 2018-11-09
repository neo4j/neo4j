/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.values.storable.Value;

public class NativeHitIndexProgressor<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> implements IndexProgressor
{
    private final RawCursor<Hit<KEY,VALUE>,IOException> seeker;
    private final EntityValueClient client;
    private boolean closed;

    NativeHitIndexProgressor( RawCursor<Hit<KEY,VALUE>,IOException> seeker, EntityValueClient client )
    {
        this.seeker = seeker;
        this.client = client;
    }

    @Override
    public boolean next()
    {
        try
        {
            while ( seeker.next() )
            {
                KEY key = seeker.get().key();
                Value[] values = extractValues( key );
                if ( acceptValue( values ) && client.acceptEntity( key.getEntityId(), Float.NaN, values ) )
                {
                    return true;
                }
            }
            return false;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    protected boolean acceptValue( Value[] values )
    {
        return true;
    }

    Value[] extractValues( KEY key )
    {
        return client.needsValues() ? key.asValues() : null;
    }

    @Override
    public void close()
    {
        if ( !closed )
        {
            closed = true;
            IOUtils.closeAllUnchecked( seeker );
        }
    }
}
