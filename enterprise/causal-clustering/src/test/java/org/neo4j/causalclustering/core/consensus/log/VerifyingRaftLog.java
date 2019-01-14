/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
        for ( long logIndex = expected.prevIndex() + 1; logIndex <= expected.appendIndex(); logIndex++ )
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
        try ( RaftLogCursor expectedCursor = expected.getEntryCursor( startIndex) )
        {
            try ( RaftLogCursor otherCursor = other.getEntryCursor( startIndex ) )
            {
                boolean expectedNext;
                do
                {
                    expectedNext = expectedCursor.next();
                    assertEquals( expectedNext, otherCursor.next() );
                    if ( expectedNext )
                    {
                        assertEquals( expectedCursor.get(), otherCursor.get() );
                        assertEquals( expectedCursor.index(), otherCursor.index() );
                    }
                }
                while ( expectedNext );
            }
        }
    }
}
