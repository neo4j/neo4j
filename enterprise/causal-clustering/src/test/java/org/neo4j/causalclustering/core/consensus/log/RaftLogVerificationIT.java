/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.log;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;

public abstract class RaftLogVerificationIT
{
    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    private VerifyingRaftLog raftLog;

    protected abstract RaftLog createRaftLog() throws Throwable;

    protected abstract long operations();

    @Before
    public void before() throws Throwable
    {
        raftLog = new VerifyingRaftLog( createRaftLog() );
    }

    @After
    public void after() throws Throwable
    {
        raftLog.verify();
    }

    @Test
    public void verifyAppend() throws Throwable
    {
        for ( int i = 0; i < operations(); i++ )
        {
            raftLog.append( new RaftLogEntry( i * 3, valueOf( i * 7 ) ) );
        }
    }

    @Test
    public void verifyAppendWithIntermittentTruncation() throws Throwable
    {
        for ( int i = 0; i < operations(); i++ )
        {
            raftLog.append( new RaftLogEntry( i * 3, valueOf( i * 7 ) ) );
            if ( i > 0 && i % 13 == 0 )
            {
                raftLog.truncate( raftLog.appendIndex() - 10 );
            }
        }
    }

    @Test
    public void randomAppendAndTruncate() throws Exception
    {
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        // given
        for ( int i = 0; i < operations(); i++ )
        {
            final int finalAppendIndex = tlr.nextInt( 10 ) + 1;
            int appendIndex = finalAppendIndex;
            while ( appendIndex-- > 0 )
            {
                raftLog.append( new RaftLogEntry( i, valueOf( i ) ) );
            }

            int truncateIndex = tlr.nextInt( finalAppendIndex ); // truncate index must be strictly less than append index
            while ( truncateIndex-- > 0 )
            {
                raftLog.truncate( truncateIndex );
            }
        }
    }

    @Test
    public void shouldBeAbleToAppendAfterSkip() throws Throwable
    {
        int term = 0;
        raftLog.append( new RaftLogEntry( term, valueOf( 10 ) ) );

        int newTerm = 3;
        raftLog.skip( 5, newTerm );
        RaftLogEntry newEntry = new RaftLogEntry( newTerm, valueOf( 20 ) );
        raftLog.append( newEntry ); // this will be logIndex 6

        assertEquals( newEntry, RaftLogHelper.readLogEntry( raftLog, 6 ) );
    }
}
