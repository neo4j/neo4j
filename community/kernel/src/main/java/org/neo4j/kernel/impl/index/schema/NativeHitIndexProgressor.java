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

import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.values.storable.Value;

public class NativeHitIndexProgressor<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> extends NativeIndexProgressor<KEY,VALUE>
{
    NativeHitIndexProgressor( Seeker<KEY,VALUE> seeker, IndexProgressor.EntityValueClient client )
    {
        super( seeker, client );
    }

    @Override
    public boolean next()
    {
        try
        {
            while ( seeker.next() )
            {
                KEY key = seeker.key();
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
}
