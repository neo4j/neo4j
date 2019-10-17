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
 */
public class IndexStatisticsLayout extends Layout.Adapter<IndexStatisticsKey,IndexStatisticsValue>
{
    public IndexStatisticsLayout()
    {
        super( true, 556677, 0, 2 );
    }

    @Override
    public IndexStatisticsKey newKey()
    {
        return new IndexStatisticsKey();
    }

    @Override
    public IndexStatisticsKey copyKey( IndexStatisticsKey key, IndexStatisticsKey into )
    {
        into.setIndexId( key.getIndexId() );
        return into;
    }

    @Override
    public IndexStatisticsValue newValue()
    {
        return new IndexStatisticsValue();
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
        cursor.putLong( key.getIndexId() );
    }

    @Override
    public void writeValue( PageCursor cursor, IndexStatisticsValue value )
    {
        cursor.putLong( value.getSampleUniqueValues() );
        cursor.putLong( value.getSampleSize() );
        cursor.putLong( value.getUpdatesCount() );
        cursor.putLong( value.getIndexSize() );
    }

    @Override
    public void readKey( PageCursor cursor, IndexStatisticsKey into, int keySize )
    {
        into.setIndexId( cursor.getLong() );
    }

    @Override
    public void readValue( PageCursor cursor, IndexStatisticsValue into, int valueSize )
    {
        into.setSampleUniqueValues( cursor.getLong() );
        into.setSampleSize( cursor.getLong() );
        into.setUpdatesCount( cursor.getLong() );
        into.setIndexSize( cursor.getLong() );
    }

    @Override
    public int compare( IndexStatisticsKey o1, IndexStatisticsKey o2 )
    {
        return Long.compare( o1.getIndexId(), o2.getIndexId() );
    }

    @Override
    public void initializeAsLowest( IndexStatisticsKey key )
    {
        key.setIndexId( Long.MIN_VALUE );
    }

    @Override
    public void initializeAsHighest( IndexStatisticsKey key )
    {
        key.setIndexId( Long.MAX_VALUE );
    }
}
