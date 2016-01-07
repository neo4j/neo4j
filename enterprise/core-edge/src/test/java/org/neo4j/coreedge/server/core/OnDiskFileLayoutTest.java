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
package org.neo4j.coreedge.server.core;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;

import org.neo4j.coreedge.discovery.TestOnlyDiscoveryServiceFactory;
import org.neo4j.coreedge.raft.log.NaiveDurableRaftLog;
import org.neo4j.coreedge.raft.state.id_allocation.OnDiskIdAllocationState;
import org.neo4j.coreedge.raft.state.membership.OnDiskRaftMembershipState;
import org.neo4j.coreedge.raft.state.term.OnDiskTermState;
import org.neo4j.coreedge.raft.state.vote.OnDiskVoteState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.coreedge.server.core.EnterpriseCoreEditionModule.CLUSTER_STATE_DIRECTORY_NAME;
import static org.neo4j.kernel.impl.factory.DatabaseInfo.UNKNOWN;

public class OnDiskFileLayoutTest
{
    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldHaveClusterStateDirectoryWithNoNakedFiles() throws Exception
    {
        // given
        final PlatformModule platformModule = new PlatformModule( dir.graphDbDir(), new HashMap<>(), UNKNOWN,
                GraphDatabaseDependencies.newDependencies(), mock( GraphDatabaseFacade.class ) );
        File baseClusterStateFile = new File( dir.graphDbDir(), CLUSTER_STATE_DIRECTORY_NAME );

        // when
        new EnterpriseCoreEditionModule( platformModule, new TestOnlyDiscoveryServiceFactory() );

        // then
        FileSystemAbstraction fs = platformModule.fileSystem;
        fs.fileExists( new File( baseClusterStateFile, OnDiskVoteState.DIRECTORY_NAME ) );
        fs.fileExists( new File( baseClusterStateFile, OnDiskIdAllocationState.DIRECTORY_NAME ) );
        fs.fileExists( new File( baseClusterStateFile, OnDiskTermState.DIRECTORY_NAME ) );
        fs.fileExists( new File( baseClusterStateFile, OnDiskRaftMembershipState.DIRECTORY_NAME ) );
        fs.fileExists( new File( baseClusterStateFile, NaiveDurableRaftLog.DIRECTORY_NAME ) );

        assertEquals(5, fs.listFiles( baseClusterStateFile ).length);
    }
}
