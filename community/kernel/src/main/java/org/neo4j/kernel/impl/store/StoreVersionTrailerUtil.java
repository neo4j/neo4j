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
package org.neo4j.kernel.impl.store;

import java.io.IOException;

import org.neo4j.helpers.UTF8;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.util.Charsets;

public abstract class StoreVersionTrailerUtil
{
    /**
     * Get the offset of the store trailer, from the beginning of the file.
     *
     * @param pagedFile The paged file that to find the trailer offset of
     * @param expectedTrailer The trailer is expected to be found in the file
     * @return The offset of the trailer, or -1 if no trailer was found
     * @throws IOException
     */
    public static long getTrailerOffset( PagedFile pagedFile, String expectedTrailer ) throws IOException
    {
        int trailerPositionRelativeToFirstPageTrailerMightBeIn;
        int pageSize = pagedFile.pageSize();
        int expectedTrailerLength = UTF8.encode( expectedTrailer ).length;
        long lastPageId = pagedFile.getLastPageId();

        long firstPageThatTrailerMightBeIn =
                getFirstPageThatTrailerMightBeIn( lastPageId, pageSize, expectedTrailerLength );

        try ( PageCursor pageCursor = pagedFile.io( firstPageThatTrailerMightBeIn, PagedFile.PF_SHARED_LOCK ) )
        {
            byte[] allData = getTheBytesThatWillContainTheTrailer( pageSize, firstPageThatTrailerMightBeIn, lastPageId,
                    pageCursor );
            trailerPositionRelativeToFirstPageTrailerMightBeIn =
                    findTrailerPositionInArray( allData, UTF8.encode( expectedTrailer.split( " " )[0] ) );
        }
        if ( trailerPositionRelativeToFirstPageTrailerMightBeIn == -1 )
        {
            return trailerPositionRelativeToFirstPageTrailerMightBeIn;
        }
        return trailerPositionRelativeToFirstPageTrailerMightBeIn + firstPageThatTrailerMightBeIn * pageSize;
    }

    /**
     * Read the trailer present in the given file.
     *
     * @param pagedFile The paged file to look in
     * @param expectedTrailer The trailer is expected to be found in the file
     * @return The found trailer, or null if no trailer was found
     * @throws IOException
     */
    public static String readTrailer( PagedFile pagedFile, String expectedTrailer ) throws IOException
    {
        String version = null;
        int pageSize = pagedFile.pageSize();
        int encodedExpectedTrailerLength = UTF8.encode( expectedTrailer ).length;
        long lastPageId = pagedFile.getLastPageId();
        long firstPageThatTrailerMightBeIn =
                getFirstPageThatTrailerMightBeIn( lastPageId, pageSize, encodedExpectedTrailerLength );

        try ( PageCursor pageCursor = pagedFile.io( firstPageThatTrailerMightBeIn, PagedFile.PF_SHARED_LOCK ) )
        {
            byte[] data = getTheBytesThatWillContainTheTrailer(
                    pageSize, firstPageThatTrailerMightBeIn, lastPageId, pageCursor );
            int trailerPositionRelativeToFirstPageTrailerMightBeIn =
                    findTrailerPositionInArray( data, UTF8.encode( expectedTrailer.split( " " )[0] ) );
            if ( trailerPositionRelativeToFirstPageTrailerMightBeIn != -1 )
            {
                version = new String( data, trailerPositionRelativeToFirstPageTrailerMightBeIn,
                        encodedExpectedTrailerLength, Charsets.UTF_8 );
            }
        }
        return version;
    }

    /**
     * Write the given trailer at the given offset into the file.
     *
     * @param pagedFile The paged file to write the trailer into
     * @param trailer The trailer to be written, encoded in UTF-8
     * @param trailerOffset The position to write the trailer at, from the beginning of the file.
     * @throws IOException
     */
    public static void writeTrailer( PagedFile pagedFile, byte[] trailer, long trailerOffset ) throws IOException
    {
        int pageSize = pagedFile.pageSize();
        long pageIdTrailerStartsIn = trailerOffset / pageSize;

        try ( PageCursor pageCursor = pagedFile.io( pageIdTrailerStartsIn, PagedFile.PF_EXCLUSIVE_LOCK ) )
        {
            int writtenOffset = 0;
            while ( writtenOffset < trailer.length )
            {
                pageCursor.next();
                pageCursor.setOffset( (int) ((writtenOffset + trailerOffset) % pageSize) );
                do
                {
                    do
                    {
                        pageCursor.putByte( trailer[writtenOffset] );
                    }
                    while ( pageCursor.shouldRetry() );
                    writtenOffset++;
                }
                while ( (writtenOffset + trailerOffset) % pageSize != 0 && writtenOffset < trailer.length );
            }
        }
    }

    private static long getFirstPageThatTrailerMightBeIn( long lastPageId, int pageSize, int expectedTrailerLength )
            throws IOException
    {
        int maximumNumberOfPagesVersionSpans = getMaximumNumberOfPagesVersionSpans( expectedTrailerLength, pageSize );
        return Math.max( lastPageId + 1 - maximumNumberOfPagesVersionSpans, 0 );
    }

    private static int findTrailerPositionInArray( byte[] dataThatShouldContainTrailer, byte[] trailer )
    {
        for ( int i = dataThatShouldContainTrailer.length - trailer.length; i >= 0; i-- )
        {
            int pos = 0;
            while ( pos < trailer.length && dataThatShouldContainTrailer[i + pos] == trailer[pos] )
            {
                pos++;
            }
            if ( pos == trailer.length )
            {
                return i;
            }
        }
        return -1;
    }

    private static int getMaximumNumberOfPagesVersionSpans( int trailerLength, int pageSize )
    {
        return trailerLength / pageSize + 2;
    }

    private static byte[] getTheBytesThatWillContainTheTrailer( int pageSize, long firstPageThatTrailerMightBeIn,
            long lastPageId, PageCursor pageCursor ) throws IOException
    {
        byte[] allData = new byte[(int) (pageSize * (lastPageId - firstPageThatTrailerMightBeIn + 1))];
        byte[] data = new byte[pageSize];
        int currentPage = 0;
        while ( pageCursor.next() )
        {
            do
            {
                pageCursor.getBytes( data );
            }
            while ( pageCursor.shouldRetry() );
            System.arraycopy( data, 0, allData, currentPage * data.length, data.length );
            currentPage++;
        }
        return allData;
    }
}
