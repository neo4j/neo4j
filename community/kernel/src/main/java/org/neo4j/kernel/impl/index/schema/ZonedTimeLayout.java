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

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

/**
 * {@link Layout} for absolute times.
 */
class ZonedTimeLayout extends IndexLayout<ZonedTimeIndexKey,NativeIndexValue>
{
    ZonedTimeLayout()
    {
        super( "Tzt", 0, 1 );
    }

    @Override
    public ZonedTimeIndexKey newKey()
    {
        return new ZonedTimeIndexKey();
    }

    @Override
    public ZonedTimeIndexKey copyKey( ZonedTimeIndexKey key, ZonedTimeIndexKey into )
    {
        into.nanosOfDayUTC = key.nanosOfDayUTC;
        into.zoneOffsetSeconds = key.zoneOffsetSeconds;
        into.setEntityId( key.getEntityId() );
        into.setCompareId( key.getCompareId() );
        return into;
    }

    @Override
    public int keySize( ZonedTimeIndexKey key )
    {
        return ZonedTimeIndexKey.SIZE;
    }

    @Override
    public void writeKey( PageCursor cursor, ZonedTimeIndexKey key )
    {
        cursor.putLong( key.nanosOfDayUTC );
        cursor.putInt( key.zoneOffsetSeconds );
        cursor.putLong( key.getEntityId() );
    }

    @Override
    public void readKey( PageCursor cursor, ZonedTimeIndexKey into, int keySize )
    {
        into.nanosOfDayUTC = cursor.getLong();
        into.zoneOffsetSeconds = cursor.getInt();
        into.setEntityId( cursor.getLong() );
    }
}
