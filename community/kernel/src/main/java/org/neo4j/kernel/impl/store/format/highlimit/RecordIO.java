/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format.highlimit;

import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.format.highlimit.Reference.DataAdapter;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

/**
 * Defines the logic of how to store records as record units.
 */
public interface RecordIO<RECORD extends AbstractBaseRecord>
{

    void read( RECORD record, PageCursor primaryCursor, int recordSize, PagedFile storeFile,
            Consumer<DataAdapter<PageCursor>> reader ) throws IOException;

    DataAdapter<PageCursor> getWriteAdapter( RECORD record, PageCursor primaryCursor, int recordSize,
            PagedFile storeFile ) throws IOException;

    /**
     * Community implementation only supports single record units.
     */
    class CommunityRecordIO<RECORD extends AbstractBaseRecord> implements RecordIO<RECORD>
    {

        @Override
        public void read( RECORD record, PageCursor primaryCursor, int recordSize, PagedFile storeFile,
                Consumer<DataAdapter<PageCursor>> reader ) throws IOException
        {
            throw new IOException( "Community edition only supports single record units" );
        }

        @Override
        public DataAdapter<PageCursor> getWriteAdapter( RECORD record, PageCursor primaryCursor, int recordSize,
                PagedFile storeFile ) throws IOException
        {
            throw new IOException( "Community edition only supports single record units" );
        }
    }
}
