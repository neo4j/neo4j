/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.management;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.core.state.ClusterStateDirectory;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.ManagementSupport;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.management.CausalClustering;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.core.consensus.log.RaftLog.RAFT_LOG_DIRECTORY_NAME;
import static org.neo4j.causalclustering.core.state.machines.CoreStateMachinesModule.ID_ALLOCATION_NAME;
import static org.neo4j.causalclustering.core.state.machines.CoreStateMachinesModule.LOCK_TOKEN_NAME;

public class CausalClusteringBeanTest
{
    private final FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
    private final GraphDatabaseAPI db = mock( CoreGraphDatabase.class );
    private final File dataDir = new File( "dataDir" );
    private final ClusterStateDirectory clusterStateDirectory = ClusterStateDirectory.withoutInitializing( dataDir );
    private final RaftMachine raftMachine = mock( RaftMachine.class );
    private CausalClustering ccBean;

    @Before
    public void setUp()
    {
        KernelData kernelData = new KernelData( fs, mock( PageCache.class ), new File( "storeDir" ), Config.defaults(), db );

        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( clusterStateDirectory );
        dependencies.satisfyDependency( raftMachine );

        when( db.getDependencyResolver() ).thenReturn( dependencies );
        ManagementData data = new ManagementData( new CausalClusteringBean(), kernelData, ManagementSupport.load() );

        ccBean = (CausalClustering) new CausalClusteringBean().createMBean( data );
    }

    @Test
    public void getCurrentRoleFromRaftMachine()
    {
        when( raftMachine.currentRole() ).thenReturn( Role.LEADER, Role.FOLLOWER, Role.CANDIDATE );
        assertEquals( "LEADER", ccBean.getRole() );
        assertEquals( "FOLLOWER", ccBean.getRole() );
        assertEquals( "CANDIDATE", ccBean.getRole() );
    }

    @Test
    public void returnSumOfRaftLogDirectory() throws Exception
    {
        File raftLogDirectory = new File( clusterStateDirectory.get(), RAFT_LOG_DIRECTORY_NAME );
        fs.mkdirs( raftLogDirectory );

        createFileOfSize( new File( raftLogDirectory, "raftLog1" ), 5 );
        createFileOfSize( new File( raftLogDirectory, "raftLog2" ), 10 );

        assertEquals( 15L, ccBean.getRaftLogSize() );
    }

    @Test
    public void excludeRaftLogFromReplicatedStateSize() throws Exception
    {
        File stateDir = clusterStateDirectory.get();

        // Raft log
        File raftLogDirectory = new File( stateDir, RAFT_LOG_DIRECTORY_NAME );
        fs.mkdirs( raftLogDirectory );
        createFileOfSize( new File( raftLogDirectory, "raftLog1" ), 5 );

        // Other state
        File idAllocationDir = new File( stateDir, ID_ALLOCATION_NAME );
        fs.mkdirs( idAllocationDir );
        createFileOfSize( new File( idAllocationDir, "state" ), 10 );
        File lockTokenDir = new File( stateDir, LOCK_TOKEN_NAME );
        fs.mkdirs( lockTokenDir );
        createFileOfSize( new File( lockTokenDir, "state" ), 20 );

        assertEquals( 30L, ccBean.getReplicatedStateSize() );
    }

    private void createFileOfSize( File file, int size ) throws IOException
    {
        try ( StoreChannel storeChannel = fs.create( file ) )
        {
            byte[] bytes = new byte[size];
            ByteBuffer buffer = ByteBuffer.wrap( bytes );
            storeChannel.writeAll( buffer );
        }
    }
}
