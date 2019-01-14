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

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

/**
 * {@link Layout} for durations.
 */
class DurationLayout extends SchemaLayout<DurationSchemaKey>
{
    DurationLayout()
    {
        super( "Tdu", 0, 1 );
    }

    @Override
    public DurationSchemaKey newKey()
    {
        return new DurationSchemaKey();
    }

    @Override
    public DurationSchemaKey copyKey( DurationSchemaKey key, DurationSchemaKey into )
    {
        into.totalAvgSeconds = key.totalAvgSeconds;
        into.nanosOfSecond = key.nanosOfSecond;
        into.months = key.months;
        into.days = key.days;
        into.setEntityId( key.getEntityId() );
        into.setCompareId( key.getCompareId() );
        return into;
    }

    @Override
    public int keySize( DurationSchemaKey key )
    {
        return DurationSchemaKey.SIZE;
    }

    @Override
    public void writeKey( PageCursor cursor, DurationSchemaKey key )
    {
        cursor.putLong( key.totalAvgSeconds );
        cursor.putInt( key.nanosOfSecond );
        cursor.putLong( key.months );
        cursor.putLong( key.days );
        cursor.putLong( key.getEntityId() );
    }

    @Override
    public void readKey( PageCursor cursor, DurationSchemaKey into, int keySize )
    {
        into.totalAvgSeconds = cursor.getLong();
        into.nanosOfSecond = cursor.getInt();
        into.months = cursor.getLong();
        into.days = cursor.getLong();
        into.setEntityId( cursor.getLong() );
    }
}
