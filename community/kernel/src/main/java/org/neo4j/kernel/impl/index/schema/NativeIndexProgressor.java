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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.values.storable.Value;

abstract class NativeIndexProgressor<KEY extends NativeSchemaKey<KEY>, VALUE extends NativeSchemaValue> implements IndexProgressor
{
    final RawCursor<Hit<KEY,VALUE>,IOException> seeker;
    final NodeValueClient client;
    private final Collection<RawCursor<Hit<KEY,VALUE>,IOException>> toRemoveFromOnClose;
    private boolean closed;

    NativeIndexProgressor( RawCursor<Hit<KEY,VALUE>,IOException> seeker, NodeValueClient client,
            Collection<RawCursor<Hit<KEY,VALUE>,IOException>> toRemoveFromOnClose )
    {
        this.seeker = seeker;
        this.client = client;
        this.toRemoveFromOnClose = toRemoveFromOnClose;
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

    Value[] extractValues( KEY key )
    {
        return client.needsValues() ? new Value[]{ key.asValue()} : null;
    }
}
