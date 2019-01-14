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
package org.neo4j.kernel.impl.api.index.stats;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

/**
 * {@link GBPTree} layout in {@link IndexStatisticsStore}, using {@link IndexStatisticsKey} and {@link IndexStatisticsValue}.
 * Basically a 1B type + 16B key and a 16B value.
 */
class IndexStatisticsLayout extends Layout.Adapter<IndexStatisticsKey,IndexStatisticsValue>
{
    @Override
    public IndexStatisticsKey newKey()
    {
        return new IndexStatisticsKey();
    }

    @Override
    public IndexStatisticsKey copyKey( IndexStatisticsKey key, IndexStatisticsKey into )
    {
        into.type = key.type;
        into.indexId = key.indexId;
        into.additional = key.additional;
        return into;
    }

    @Override
    public IndexStatisticsValue newValue()
    {
        return new IndexStatisticsValue();
    }

    IndexStatisticsValue copyValue( IndexStatisticsValue value, IndexStatisticsValue into )
    {
        into.first = value.first;
        into.second = value.second;
        return into;
    }

    @Override
    public int keySize( IndexStatisticsKey key )
    {
        return IndexStatisticsKey.SIZE;
    }

    @Override
    public int valueSize( IndexStatisticsValue value )
    {
        return IndexStatisticsValue.SIZE;
    }

    @Override
    public void writeKey( PageCursor cursor, IndexStatisticsKey key )
    {
        cursor.putByte( key.type );
        cursor.putLong( key.indexId );
        cursor.putLong( key.additional );
    }

    @Override
    public void writeValue( PageCursor cursor, IndexStatisticsValue value )
    {
        cursor.putLong( value.first );
        cursor.putLong( value.second );
    }

    @Override
    public void readKey( PageCursor cursor, IndexStatisticsKey into, int keySize )
    {
        into.type = cursor.getByte();
        into.indexId = cursor.getLong();
        into.additional = cursor.getLong();
    }

    @Override
    public void readValue( PageCursor cursor, IndexStatisticsValue into, int valueSize )
    {
        into.first = cursor.getLong();
        into.second = cursor.getLong();
    }

    @Override
    public boolean fixedSize()
    {
        return true;
    }

    @Override
    public long identifier()
    {
        return 556677;
    }

    @Override
    public int majorVersion()
    {
        return 0;
    }

    @Override
    public int minorVersion()
    {
        return 1;
    }

    @Override
    public int compare( IndexStatisticsKey o1, IndexStatisticsKey o2 )
    {
        int typeCompare = Byte.compare( o1.type, o2.type );
        if ( typeCompare != 0 )
        {
            return typeCompare;
        }
        int keyCompare = Long.compare( o1.indexId, o2.indexId );
        if ( keyCompare != 0 )
        {
            return keyCompare;
        }
        return Long.compare( o1.additional, o2.additional );
    }
}
