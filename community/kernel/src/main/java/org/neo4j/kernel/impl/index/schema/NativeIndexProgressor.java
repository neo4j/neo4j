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
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.values.storable.Value;

abstract class NativeIndexProgressor<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> implements IndexProgressor
{
    final Seeker<KEY,VALUE> seeker;
    final EntityValueClient client;
    private boolean closed;

    NativeIndexProgressor( Seeker<KEY,VALUE> seeker, EntityValueClient client )
    {
        this.seeker = seeker;
        this.client = client;
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
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }
    }

    Value[] extractValues( KEY key )
    {
        return client.needsValues() ? key.asValues() : null;
    }
}
