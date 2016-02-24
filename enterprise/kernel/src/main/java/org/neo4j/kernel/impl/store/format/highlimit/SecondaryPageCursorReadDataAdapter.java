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

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.format.highlimit.BaseHighLimitRecordFormat;
import org.neo4j.kernel.impl.store.format.highlimit.Reference.DataAdapter;

/**
 * {@link DataAdapter} able to acquire a secondary {@link PageCursor} on potentially a different page
 * for continuing reading contents belonging to the primary record.
 */
class SecondaryPageCursorReadDataAdapter implements DataAdapter<PageCursor>, SecondaryPageCursorControl
{
    private final PageCursor primaryCursor;
    private final int primaryInitialOffset;
    private final int primaryEndOffset;
    private final PageCursor secondaryCursor;
    private final int secondaryOffset;
    private boolean switched;

    SecondaryPageCursorReadDataAdapter( PageCursor cursor, PagedFile storeFile,
            long secondaryPageId, int secondaryOffset, int primaryEndOffset, int pfFlags ) throws IOException
    {
        this.primaryCursor = cursor;
        this.primaryEndOffset = primaryEndOffset;
        this.primaryInitialOffset = cursor.getOffset();
        this.secondaryCursor = storeFile.io( secondaryPageId, pfFlags );
        this.secondaryCursor.next();
        this.secondaryOffset = secondaryOffset;
    }

    @Override
    public byte get( PageCursor primaryCursor /*same as the one we have*/ )
    {
        if ( primaryCursor.getOffset() == primaryEndOffset )
        {
            // We've come to the end of the primary cursor, use the secondary cursor instead
            if ( !switched )
            {
                // Just read out the header, get it out of the way and verify that this secondary record
                // is in fact a secondary record.
                // TODO can we do this in BaseHighLimitRecordFormat (the place where this adapter is created) instead?
                byte secondaryHeaderByte = secondaryCursor.getByte();
                assert (secondaryHeaderByte & BaseHighLimitRecordFormat.HEADER_BIT_RECORD_UNIT) != 0;
                assert (secondaryHeaderByte & BaseHighLimitRecordFormat.HEADER_BIT_FIRST_RECORD_UNIT) == 0;
                switched = true;
            }
            return secondaryCursor.getByte();
        }

        // There's still data to be read from the primary cursor
        return primaryCursor.getByte();
    }

    @Override
    public void put( byte oneByte, PageCursor primaryCursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reposition()
    {
        primaryCursor.setOffset( primaryInitialOffset );
        secondaryCursor.setOffset( secondaryOffset );
        switched = false;
    }

    @Override
    public boolean shouldRetry() throws IOException
    {
        // Don't check shouldRetry on primary here since that will happen in the outer loop
        // and will guard both units.
        return secondaryCursor.shouldRetry();
    }

    @Override
    public void close()
    {
        secondaryCursor.close();
    }
}
