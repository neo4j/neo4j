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

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.format.highlimit.Reference.DataAdapter;

import static org.neo4j.kernel.impl.store.format.BaseRecordFormat.IN_USE_BIT;
import static org.neo4j.kernel.impl.store.format.highlimit.BaseHighLimitRecordFormat.HEADER_BIT_RECORD_UNIT;

/**
 * {@link DataAdapter} able to move the {@link PageCursor} to another record, potentially on another page,
 * for continued writing of contents to a secondary record unit.
 */
class SecondaryPageCursorWriteDataAdapter implements DataAdapter<PageCursor>
{
    private boolean switched;
    private final long pageIdForSecondaryRecord;
    private final int offsetForSecondaryId;
    private final int primaryEndOffset;

    SecondaryPageCursorWriteDataAdapter( long pageIdForSecondaryRecord,
            int offsetForSecondaryId, int primaryEndOffset )
    {
        this.pageIdForSecondaryRecord = pageIdForSecondaryRecord;
        this.offsetForSecondaryId = offsetForSecondaryId;
        this.primaryEndOffset = primaryEndOffset;
    }

    @Override
    public byte get( PageCursor source )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put( byte oneByte, PageCursor cursor ) throws IOException
    {
        if ( !switched && cursor.getOffset() == primaryEndOffset )
        {
            if ( !cursor.next( pageIdForSecondaryRecord ) )
            {
                throw new UnderlyingStorageException( "Couldn't move to secondary page " + pageIdForSecondaryRecord );
            }
            cursor.setOffset( offsetForSecondaryId );
            cursor.putByte( (byte) (IN_USE_BIT | HEADER_BIT_RECORD_UNIT) );
            switched = true;
        }

        cursor.putByte( oneByte );
    }
}
