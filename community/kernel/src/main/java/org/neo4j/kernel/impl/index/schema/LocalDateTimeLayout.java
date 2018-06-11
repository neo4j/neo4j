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

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

/**
 * {@link Layout} for local date times.
 */
class LocalDateTimeLayout extends SchemaLayout<LocalDateTimeIndexKey>
{
    LocalDateTimeLayout()
    {
        super( "Tld", 0, 1 );
    }

    @Override
    public LocalDateTimeIndexKey newKey()
    {
        return new LocalDateTimeIndexKey();
    }

    @Override
    public LocalDateTimeIndexKey copyKey( LocalDateTimeIndexKey key, LocalDateTimeIndexKey into )
    {
        into.epochSecond = key.epochSecond;
        into.nanoOfSecond = key.nanoOfSecond;
        into.setEntityId( key.getEntityId() );
        into.setCompareId( key.getCompareId() );
        return into;
    }

    @Override
    public int keySize( LocalDateTimeIndexKey key )
    {
        return LocalDateTimeIndexKey.SIZE;
    }

    @Override
    public void writeKey( PageCursor cursor, LocalDateTimeIndexKey key )
    {
        cursor.putLong( key.epochSecond );
        cursor.putInt( key.nanoOfSecond );
        cursor.putLong( key.getEntityId() );
    }

    @Override
    public void readKey( PageCursor cursor, LocalDateTimeIndexKey into, int keySize )
    {
        into.epochSecond = cursor.getLong();
        into.nanoOfSecond = cursor.getInt();
        into.setEntityId( cursor.getLong() );
    }
}
