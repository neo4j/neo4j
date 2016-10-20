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
package org.neo4j.causalclustering.core.consensus.log;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * A log that uses the in-memory RAFT log as the reference implementation
 * for verifying another RAFT log that is under test. All operations are
 * mirrored to both logs and return values are checked for equality.
 *
 * At the end of a test the content of both logs can be compared for
 * equality using {@link VerifyingRaftLog#verify()}.
 */
class VerifyingRaftLog implements RaftLog
{
    private InMemoryRaftLog expected = new InMemoryRaftLog();
    private final RaftLog other;

    VerifyingRaftLog( RaftLog other )
    {
        this.other = other;
    }

    @Override
    public long append( RaftLogEntry... entries ) throws IOException
    {
        long appendIndex = expected.append( entries );
        assertEquals( appendIndex, other.append( entries ) );
        return appendIndex;
    }

    @Override
    public void truncate( long fromIndex ) throws IOException
    {
        expected.truncate( fromIndex );
        other.truncate( fromIndex );
    }

    @Override
    public long prune( long safeIndex ) throws IOException
    {
        // the expected one should be able to prune exactly, while others are not required to
        long pruneIndex = other.prune( safeIndex );
        assertEquals( pruneIndex, expected.prune( pruneIndex ) );
        return pruneIndex;
    }

    @Override
    public long appendIndex()
    {
        long appendIndex = expected.appendIndex();
        assertEquals( appendIndex, other.appendIndex() );
        return appendIndex;
    }

    @Override
    public long prevIndex()
    {
        long prevIndex = expected.appendIndex();
        assertEquals( prevIndex, other.appendIndex() );
        return prevIndex;
    }

    @Override
    public long readEntryTerm( long logIndex ) throws IOException
    {
        long term = expected.readEntryTerm( logIndex );
        assertEquals( term, other.readEntryTerm( logIndex ) );
        return term;
    }

    @Override
    public RaftLogCursor getEntryCursor( long fromIndex ) throws IOException
    {
        return other.getEntryCursor( fromIndex );
    }

    @Override
    public long skip( long index, long term ) throws IOException
    {
        long expectedAppendIndex = expected.skip( index, term );
        assertEquals( expectedAppendIndex, other.skip( index, term ) );
        return expectedAppendIndex;
    }

    public void verify() throws IOException
    {
        verifyUsing( other );
    }

    public void verifyUsing( RaftLog other ) throws IOException
    {
        assertEquals( expected.appendIndex(), other.appendIndex() );

        verifyTraversalUsingCursor( expected, other );
        verifyDirectLookupForwards( expected, other );
        verifyDirectLookupBackwards( expected, other );
    }

    private void verifyDirectLookupForwards( InMemoryRaftLog expected, RaftLog other ) throws IOException
    {
        for ( long logIndex = expected.prevIndex()+1; logIndex <= expected.appendIndex(); logIndex++ )
        {
            directAssertions( expected, other, logIndex );
        }
    }

    private void verifyDirectLookupBackwards( InMemoryRaftLog expected, RaftLog other ) throws IOException
    {
        for ( long logIndex = expected.appendIndex(); logIndex > expected.prevIndex(); logIndex-- )
        {
            directAssertions( expected, other, logIndex );
        }
    }

    private void directAssertions( InMemoryRaftLog expected, RaftLog other, long logIndex ) throws IOException
    {
        assertEquals( expected.readEntryTerm( logIndex ), other.readEntryTerm( logIndex ) );
    }

    private void verifyTraversalUsingCursor( RaftLog expected, RaftLog other ) throws IOException
    {
        long startIndex = expected.prevIndex() + 1;
        try( RaftLogCursor expectedCursor = expected.getEntryCursor( startIndex) )
        {
            try( RaftLogCursor otherCursor = other.getEntryCursor( startIndex ) )
            {
                boolean expectedNext;
                do
                {
                    expectedNext = expectedCursor.next();
                    assertEquals( expectedNext, otherCursor.next() );
                    if( expectedNext )
                    {
                        assertEquals( expectedCursor.get(), otherCursor.get() );
                        assertEquals( expectedCursor.index(), otherCursor.index() );
                    }
                } while( expectedNext );
            }
        }
    }
}
