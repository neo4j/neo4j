/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
