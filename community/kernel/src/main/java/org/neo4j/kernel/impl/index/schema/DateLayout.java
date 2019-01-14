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
 * {@link Layout} for dates.
 */
class DateLayout extends SchemaLayout<DateSchemaKey>
{
    DateLayout()
    {
        super( "Tda", 0, 1 );
    }

    @Override
    public DateSchemaKey newKey()
    {
        return new DateSchemaKey();
    }

    @Override
    public DateSchemaKey copyKey( DateSchemaKey key, DateSchemaKey into )
    {
        into.epochDay = key.epochDay;
        into.setEntityId( key.getEntityId() );
        into.setCompareId( key.getCompareId() );
        return into;
    }

    @Override
    public int keySize( DateSchemaKey key )
    {
        return DateSchemaKey.SIZE;
    }

    @Override
    public void writeKey( PageCursor cursor, DateSchemaKey key )
    {
        cursor.putLong( key.epochDay );
        cursor.putLong( key.getEntityId() );
    }

    @Override
    public void readKey( PageCursor cursor, DateSchemaKey into, int keySize )
    {
        into.epochDay = cursor.getLong();
        into.setEntityId( cursor.getLong() );
    }
}
