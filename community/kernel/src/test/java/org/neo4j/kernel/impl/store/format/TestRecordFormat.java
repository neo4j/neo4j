/*
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
package org.neo4j.kernel.impl.store.format;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.standard.StoreFormat;

public class TestRecordFormat implements StoreFormat.RecordFormat<TestRecord>
{
    @Override
    public String recordName()
    {
        return "MyRecord";
    }

    @Override
    public long id( TestRecord myRecord )
    {
        return myRecord.id;
    }

    @Override
    public TestRecord newRecord( long id )
    {
        return new TestRecord( id, 0 );
    }

    @Override
    public void serialize( PageCursor cursor, int offset, TestRecord myRecord )
    {
        cursor.putLong(offset, myRecord.value);
    }

    @Override
    public void deserialize( PageCursor cursor, int offset, long id, TestRecord record )
    {
        record.id = id;
        record.value = cursor.getLong( offset );
    }

    @Override
    public boolean inUse( PageCursor pageCursor, int offset )
    {
        return pageCursor.getLong(offset) != 0;
    }
}
