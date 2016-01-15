package org.neo4j.coreedge.server.core;

import java.io.File;
import java.util.HashMap;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.discovery.TestOnlyDiscoveryServiceFactory;
import org.neo4j.coreedge.raft.log.NaiveDurableRaftLog;
import org.neo4j.coreedge.raft.state.id_allocation.OnDiskIdAllocationState;
import org.neo4j.coreedge.raft.state.membership.OnDiskRaftMembershipState;
import org.neo4j.coreedge.raft.state.term.OnDiskTermState;
import org.neo4j.coreedge.raft.state.vote.OnDiskVoteState;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class OnDiskFileLayoutTest
{
    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldHaveClusterStateDirectoryWithNoNakedFiles() throws Exception
    {
        // given
        final PlatformModule platformModule = new PlatformModule( dir.graphDbDir(), new HashMap<>(),
                GraphDatabaseDependencies.newDependencies(), mock( GraphDatabaseFacade.class ) );
        File baseClusterStateFile = new File( dir.graphDbDir(),
                EnterpriseCoreEditionModule.CLUSTER_STATE_DIRECTORY_NAME );


        // when
        new EnterpriseCoreEditionModule( platformModule, new TestOnlyDiscoveryServiceFactory() );

        // then
        platformModule.fileSystem.fileExists( new File( baseClusterStateFile, OnDiskVoteState.DIRECTORY_NAME ) );
        platformModule.fileSystem.fileExists( new File( baseClusterStateFile,
                OnDiskIdAllocationState.DIRECTORY_NAME ) );
        platformModule.fileSystem.fileExists( new File( baseClusterStateFile, OnDiskTermState.DIRECTORY_NAME ) );
        platformModule.fileSystem.fileExists( new File( baseClusterStateFile, OnDiskRaftMembershipState
                .DIRECTORY_NAME ) );
        platformModule.fileSystem.fileExists( new File( baseClusterStateFile, NaiveDurableRaftLog.DIRECTORY_NAME ) );

        assertEquals(5, platformModule.fileSystem.listFiles( baseClusterStateFile ).length);
    }
}