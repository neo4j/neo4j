/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store.format.highlimit;

import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.format.highlimit.Reference.DataAdapter;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

import static org.neo4j.kernel.impl.store.RecordPageLocationCalculator.offsetForId;
import static org.neo4j.kernel.impl.store.RecordPageLocationCalculator.pageIdForRecord;
import static org.neo4j.kernel.impl.store.format.highlimit.Reference.PAGE_CURSOR_ADAPTER;

/**
 * Enterprise supports double record units for records.
 */
public class EnterpriseRecordIO<RECORD extends AbstractBaseRecord> implements RecordIO<RECORD>
{

    @Override
    public void read( RECORD record, PageCursor primaryCursor, int recordSize, PagedFile storeFile,
            Consumer<DataAdapter<PageCursor>> reader ) throws IOException
    {
        int primaryEndOffset = primaryCursor.getOffset() + recordSize - 1 /*we've already read the header byte*/;

        // This is a record that is split into multiple record units. We need a bit more clever
        // data structures here. For the time being this means instantiating one object,
        // but the trade-off is a great reduction in complexity.
        long secondaryId = Reference.decode( primaryCursor, PAGE_CURSOR_ADAPTER );
        @SuppressWarnings( "resource" ) SecondaryPageCursorReadDataAdapter readAdapter =
                new SecondaryPageCursorReadDataAdapter( primaryCursor, storeFile,
                        pageIdForRecord( secondaryId, storeFile.pageSize(), recordSize ),
                        offsetForId( secondaryId, storeFile.pageSize(), recordSize ), primaryEndOffset,
                        PagedFile.PF_SHARED_READ_LOCK );

        try ( SecondaryPageCursorControl secondaryPageCursorControl = readAdapter )
        {
            do
            {
                // (re)sets offsets for both cursors
                secondaryPageCursorControl.reposition();
                //doReadInternal( record, primaryCursor, recordSize, headerByte, inUse, readAdapter );
                reader.accept( readAdapter );
            }
            while ( secondaryPageCursorControl.shouldRetry() );

            record.setSecondaryId( secondaryId );
        }
    }

    @Override
    public void write( RECORD record, PageCursor primaryCursor, int recordSize, PagedFile storeFile,
            ThrowingConsumer<DataAdapter<PageCursor>, IOException> writer ) throws IOException
    {
        int primaryEndOffset = primaryCursor.getOffset() + recordSize - 1 /*we've already written the header byte*/;

        // Write using the normal adapter since the first reference we write cannot really overflow
        // into the secondary record
        Reference.encode( record.getSecondaryId(), primaryCursor, PAGE_CURSOR_ADAPTER );
        DataAdapter<PageCursor> dataAdapter = new SecondaryPageCursorWriteDataAdapter(
                pageIdForRecord( record.getSecondaryId(), storeFile.pageSize(), recordSize ),
                offsetForId( record.getSecondaryId(), storeFile.pageSize(), recordSize ), primaryEndOffset );

        writer.accept( dataAdapter );
    }
}
