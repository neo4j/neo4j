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
package org.neo4j.coreedge.raft.log;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.test.EphemeralFileSystemRule;

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
            raftLog.append( new RaftLogEntry( i * 3, ReplicatedInteger.valueOf( i * 7 ) ) );
        }
    }

    @Test
    public void verifyAppendWithIntermittentTruncation() throws Throwable
    {
        for ( int i = 0; i < operations(); i++ )
        {
            raftLog.append( new RaftLogEntry( i * 3, ReplicatedInteger.valueOf( i * 7 ) ) );
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
            int r = tlr.nextInt( 10 );
            while( r -->0 )
            {
                raftLog.append( new RaftLogEntry( i, ReplicatedInteger.valueOf( i ) ) );
            }

            r = tlr.nextInt( 10 );
            while( r -->0 )
            {
                raftLog.truncate( r );
            }
        }
    }
}
