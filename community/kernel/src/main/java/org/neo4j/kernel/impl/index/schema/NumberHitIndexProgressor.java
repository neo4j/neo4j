/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.storageengine.api.schema.IndexProgressor;

public class NumberHitIndexProgressor<KEY extends SchemaNumberKey, VALUE extends SchemaNumberValue> implements IndexProgressor
{
    private final RawCursor<Hit<KEY,VALUE>,IOException> seeker;
    private final NodeValueClient client;
    private final Collection<RawCursor<Hit<KEY,VALUE>,IOException>> toRemoveFromOnClose;
    private boolean closed;

    NumberHitIndexProgressor( RawCursor<Hit<KEY,VALUE>,IOException> seeker, NodeValueClient client,
            Collection<RawCursor<Hit<KEY,VALUE>,IOException>> toRemoveFromOnClose )
    {
        this.seeker = seeker;
        this.client = client;
        this.toRemoveFromOnClose = toRemoveFromOnClose;
    }

    @Override
    public boolean next()
    {
        try
        {
            if ( seeker.next() )
            {
                KEY key = seeker.get().key();
                client.acceptNode( key.entityId, key.asValue() );
                return true;
            }
            return false;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void close()
    {
        if ( !closed )
        {
            closed = true;
            try
            {
                seeker.close();
                toRemoveFromOnClose.remove( seeker );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }
    }
}
