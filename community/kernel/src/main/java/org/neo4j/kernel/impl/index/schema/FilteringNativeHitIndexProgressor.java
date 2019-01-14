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
import java.util.Collection;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.values.storable.Value;

class FilteringNativeHitIndexProgressor<KEY extends NativeSchemaKey<KEY>, VALUE extends NativeSchemaValue> extends NativeHitIndexProgressor<KEY,VALUE>
{
    private final IndexQuery[] filter;

    FilteringNativeHitIndexProgressor( RawCursor<Hit<KEY,VALUE>,IOException> seeker, NodeValueClient client,
            Collection<RawCursor<Hit<KEY,VALUE>,IOException>> toRemoveFromOnClose, IndexQuery[] filter )
    {
        super( seeker, client, toRemoveFromOnClose );
        this.filter = filter;
    }

    @Override
    protected boolean acceptValue( Value[] values )
    {
        for ( int i = 0; i < values.length; i++ )
        {
            if ( !filter[i].acceptsValue( values[i] ) )
            {
                return false;
            }
        }
        return true;
    }
}
