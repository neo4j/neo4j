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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.standard.StoreFormat;
import org.neo4j.kernel.impl.store.standard.StoreToolkit;

public class TestFormatWithHeader implements StoreFormat<TestRecord, TestCursor>
{
    private final int configuredRecordSize;
    private final TestRecordFormat recordFormat;

    public TestFormatWithHeader( int configuredRecordSize )
    {
        this.configuredRecordSize = configuredRecordSize;
        this.recordFormat = new TestRecordFormat();
    }

    @Override
    public TestCursor createCursor( PagedFile file, StoreToolkit toolkit, int flags )
    {
        return new TestCursor( file, toolkit, recordFormat, flags );
    }

    @Override
    public RecordFormat<TestRecord> recordFormat()
    {
        return recordFormat;
    }

    @Override
    public int recordSize( StoreChannel channel ) throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate( 4 );
        channel.read( buf, 0 );
        buf.flip();
        return buf.getInt();
    }

    @Override
    public void createStore( StoreChannel channel ) throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate( 4 );
        buf.putInt( configuredRecordSize );
        buf.flip();
        channel.write( buf, 0 );
    }

    @Override
    public int headerSize()
    {
        return 4;
    }

    @Override
    public String version()
    {
        return "v0.1.0";
    }

    @Override
    public String type()
    {
        return "MyFormatWithHeader";
    }
}
