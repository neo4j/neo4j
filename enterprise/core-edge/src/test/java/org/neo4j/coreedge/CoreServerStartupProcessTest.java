/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge;

import java.util.concurrent.CompletableFuture;

import org.junit.Test;

import org.neo4j.coreedge.catchup.CatchupServer;
import org.neo4j.coreedge.raft.membership.MembershipWaiter;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdGeneratorFactory;
import org.neo4j.coreedge.raft.RaftInstance;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.RaftServer;
import org.neo4j.coreedge.raft.state.ReadableRaftState;
import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.core.CoreServerStartupProcess;
import org.neo4j.coreedge.raft.ScheduledTimeoutService;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CoreServerStartupProcessTest
{
    @Test
    public void startShouldDeleteStoreAndStartNewDatabase() throws Throwable
    {
        // given
        LocalDatabase localDatabase = mock( LocalDatabase.class );
        DataSourceManager dataSourceManager = mock( DataSourceManager.class );
        ReplicatedIdGeneratorFactory idGeneratorFactory = mock( ReplicatedIdGeneratorFactory.class );
        RaftLog raftLog = mock( RaftLog.class );
        RaftServer<CoreMember> raftServer = mock( RaftServer.class );
        CatchupServer catchupServer = mock( CatchupServer.class );
        ScheduledTimeoutService timeoutService = mock( ScheduledTimeoutService.class );
        MembershipWaiter<CoreMember> membershipWaiter = mock( MembershipWaiter.class );
        when(membershipWaiter.waitUntilCaughtUpMember( any(ReadableRaftState.class) ))
                .thenReturn( mock( CompletableFuture.class ) );

        CoreServerStartupProcess leaderProcess = new CoreServerStartupProcess(
                localDatabase, dataSourceManager, idGeneratorFactory, mock( RaftInstance.class ), raftLog, raftServer,
                catchupServer, timeoutService, membershipWaiter, 1000 );

        // when
        leaderProcess.start();

        // then
        verify( localDatabase ).deleteStore();
        verify( dataSourceManager ).start();
        verify( idGeneratorFactory ).start();
    }

    @Test
    public void stopShouldStopDatabase() throws Throwable
    {
        // given
        LocalDatabase localDatabase = mock( LocalDatabase.class );
        DataSourceManager dataSourceManager = mock( DataSourceManager.class );
        ReplicatedIdGeneratorFactory idGeneratorFactory = mock( ReplicatedIdGeneratorFactory.class );
        RaftLog raftLog = mock( RaftLog.class );
        RaftServer<CoreMember> raftServer = mock( RaftServer.class );
        CatchupServer catchupServer = mock( CatchupServer.class );
        ScheduledTimeoutService timeoutService = mock( ScheduledTimeoutService.class );
        MembershipWaiter<CoreMember> membershipListener = mock( MembershipWaiter.class );
        when(membershipListener.waitUntilCaughtUpMember( any(ReadableRaftState.class) )).thenReturn( mock(CompletableFuture.class) );

        CoreServerStartupProcess leaderProcess = new CoreServerStartupProcess(
                localDatabase, dataSourceManager, idGeneratorFactory, mock( RaftInstance.class ), raftLog, raftServer,
                catchupServer, timeoutService, membershipListener, 1000 );

        // when
        leaderProcess.start();
        leaderProcess.stop();

        // then
        verify( dataSourceManager ).stop();
        verify( idGeneratorFactory ).stop();
    }
}
