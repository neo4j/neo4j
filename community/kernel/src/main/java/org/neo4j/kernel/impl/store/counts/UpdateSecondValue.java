/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.counts;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.kvstore.AbstractKeyValueStore;
import org.neo4j.kernel.impl.store.kvstore.WritableBuffer;

class UpdateSecondValue extends AbstractKeyValueStore.Update<CountsKey>
{
    private static final int OFFSET = 8;
    private final long delta;

    UpdateSecondValue( CountsKey key, long delta )
    {
        super( key );
        this.delta = delta;
    }

    @Override
    protected void update( WritableBuffer value )
    {
        value.putLong( OFFSET, value.getLong( OFFSET ) + delta );
    }
}
