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
package org.neo4j.kernel.impl.store.format;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

public class RecordBoundaryCheckingPagedFile implements PagedFile
{
    private final PagedFile actual;
    private final int recordSize;
    private int ioCalls;
    private int nextCalls;
    private int setOffsetCalls;
    private int unusedBytes;
    private int retries;

    public RecordBoundaryCheckingPagedFile( PagedFile actual, int enforcedRecordSize )
    {
        this.actual = actual;
        this.recordSize = enforcedRecordSize;
    }

    @Override
    public int pageSize()
    {
        return actual.pageSize();
    }

    @Override
    public PageCursor io( long pageId, int pf_flags ) throws IOException
    {
        ioCalls++;
        return new RecordBoundaryCheckingPageCursor( actual.io( pageId, pf_flags ) );
    }

    @Override
    public long getLastPageId() throws IOException
    {
        return actual.getLastPageId();
    }

    @Override
    public void flushAndForce() throws IOException
    {
        actual.flushAndForce();
    }

    @Override
    public void flushAndForce( IOLimiter limiter ) throws IOException
    {
        actual.flushAndForce( limiter );
    }

    @Override
    public void close() throws IOException
    {
        actual.close();
    }

    public int ioCalls()
    {
        return ioCalls;
    }

    public int nextCalls()
    {
        return nextCalls;
    }

    public int unusedBytes()
    {
        return unusedBytes;
    }

    public void resetMeasurements()
    {
        ioCalls = unusedBytes = nextCalls = 0;
    }

    class RecordBoundaryCheckingPageCursor implements PageCursor
    {
        private final PageCursor actual;
        private int start = -10_000;
        private boolean shouldReport;

        RecordBoundaryCheckingPageCursor( PageCursor actual )
        {
            this.actual = actual;
        }

        private void checkBoundary( int size )
        {
            shouldReport = true; // since the cursor is moving
            if ( size > recordSize )
            {
                throw new IllegalStateException( "Tried to go beyond record boundaries. We seem to be on the " +
                        (nextCalls == 1 ? "first" : "second") + " page start offset:" + start + " record size:" +
                        recordSize + " and tried to go to " + size );
            }
        }

        private void checkRelativeBoundary( int add )
        {
            checkBoundary( getOffset() - start + add );
        }

        private void checkAbsoluteBoundary( int offset )
        {
            checkBoundary( offset - start );
        }

        @Override
        public byte getByte()
        {
            checkRelativeBoundary( Byte.BYTES );
            return actual.getByte();
        }

        @Override
        public byte getByte( int offset )
        {
            checkAbsoluteBoundary( Byte.BYTES );
            return actual.getByte( offset );
        }

        @Override
        public void putByte( byte value )
        {
            checkRelativeBoundary( Byte.BYTES );
            actual.putByte( value );
        }

        @Override
        public void putByte( int offset, byte value )
        {
            checkAbsoluteBoundary( Byte.BYTES );
            actual.putByte( offset, value );
        }

        @Override
        public long getLong()
        {
            checkRelativeBoundary( Long.BYTES );
            return actual.getLong();
        }

        @Override
        public long getLong( int offset )
        {
            checkAbsoluteBoundary( Long.BYTES );
            return actual.getLong( offset );
        }

        @Override
        public void putLong( long value )
        {
            checkRelativeBoundary( Long.BYTES );
            actual.putLong( value );
        }

        @Override
        public void putLong( int offset, long value )
        {
            checkAbsoluteBoundary( Long.BYTES );
            actual.putLong( offset, value );
        }

        @Override
        public int getInt()
        {
            checkRelativeBoundary( Integer.BYTES );
            return actual.getInt();
        }

        @Override
        public int getInt( int offset )
        {
            checkAbsoluteBoundary( Integer.BYTES );
            return actual.getInt( offset );
        }

        @Override
        public void putInt( int value )
        {
            checkRelativeBoundary( Integer.BYTES );
            actual.putInt( value );
        }

        @Override
        public void putInt( int offset, int value )
        {
            checkAbsoluteBoundary( Integer.BYTES );
            actual.putInt( offset, value );
        }

        @Override
        public void getBytes( byte[] data )
        {
            checkRelativeBoundary( data.length );
            actual.getBytes( data );
        }

        @Override
        public void getBytes( byte[] data, int arrayOffset, int length )
        {
            checkRelativeBoundary( length );
            actual.getBytes( data, arrayOffset, length );
        }

        @Override
        public void putBytes( byte[] data )
        {
            checkRelativeBoundary( data.length );
            actual.putBytes( data );
        }

        @Override
        public void putBytes( byte[] data, int arrayOffset, int length )
        {
            checkRelativeBoundary( length );
            actual.putBytes( data, arrayOffset, length );
        }

        @Override
        public short getShort()
        {
            checkRelativeBoundary( Short.BYTES );
            return actual.getShort();
        }

        @Override
        public short getShort( int offset )
        {
            checkAbsoluteBoundary( Short.BYTES );
            return actual.getShort( offset );
        }

        @Override
        public void putShort( short value )
        {
            checkRelativeBoundary( Short.BYTES );
            actual.putShort( value );
        }

        @Override
        public void putShort( int offset, short value )
        {
            checkAbsoluteBoundary( Short.BYTES );
            actual.putShort( offset, value );
        }

        @Override
        public int copyTo( int sourceOffset, PageCursor targetCursor, int targetOffset, int lengthInBytes )
        {
            return actual.copyTo( sourceOffset, targetCursor, targetOffset, lengthInBytes );
        }

        @Override
        public boolean checkAndClearBoundsFlag()
        {
            return actual.checkAndClearBoundsFlag();
        }

        @Override
        public void raiseOutOfBounds()
        {
            actual.raiseOutOfBounds();
        }

        @Override
        public PageCursor openLinkedCursor( long pageId )
        {
            return new RecordBoundaryCheckingPageCursor( actual.openLinkedCursor( pageId ) );
        }

        @Override
        public void setOffset( int offset )
        {
            if ( offset < start || offset >= start + recordSize )
            {
                reportBeforeLeavingRecord();
                start = offset;
            }
            setOffsetCalls++;
            actual.setOffset( offset );
        }

        private void reportBeforeLeavingRecord()
        {
            if ( shouldReport )
            {
                int currentUnused = recordSize - (getOffset() - start);
                unusedBytes += currentUnused;
                shouldReport = false;
            }
        }

        @Override
        public int getOffset()
        {
            return actual.getOffset();
        }

        @Override
        public long getCurrentPageId()
        {
            return actual.getCurrentPageId();
        }

        @Override
        public int getCurrentPageSize()
        {
            return actual.getCurrentPageSize();
        }

        @Override
        public File getCurrentFile()
        {
            return actual.getCurrentFile();
        }

        @Override
        public void rewind()
        {
            actual.rewind();
            start = getOffset();
        }

        @Override
        public boolean next() throws IOException
        {
            reportBeforeLeavingRecord();
            nextCalls++;
            return actual.next();
        }

        @Override
        public boolean next( long pageId ) throws IOException
        {
            reportBeforeLeavingRecord();
            nextCalls++;
            return actual.next( pageId );
        }

        @Override
        public void close()
        {
            reportBeforeLeavingRecord();
            actual.close();
        }

        @Override
        public boolean shouldRetry() throws IOException
        {
            boolean result = actual.shouldRetry();
            if ( result )
            {
                retries++;
            }
            return result;
        }
    }
}
