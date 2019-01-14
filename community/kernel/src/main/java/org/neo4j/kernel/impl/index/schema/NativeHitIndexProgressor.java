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
import org.neo4j.values.storable.Value;

public class NativeHitIndexProgressor<KEY extends NativeSchemaKey<KEY>, VALUE extends NativeSchemaValue> extends NativeIndexProgressor<KEY,VALUE>
{
    NativeHitIndexProgressor( RawCursor<Hit<KEY,VALUE>,IOException> seeker, NodeValueClient client,
            Collection<RawCursor<Hit<KEY,VALUE>,IOException>> toRemoveFromOnClose )
    {
        super( seeker, client, toRemoveFromOnClose );
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
                if ( acceptValue( values ) && client.acceptNode( key.getEntityId(), values ) )
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
}
