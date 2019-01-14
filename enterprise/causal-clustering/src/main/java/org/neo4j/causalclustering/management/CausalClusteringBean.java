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
package org.neo4j.causalclustering.management;


import java.io.File;
import javax.management.NotCompliantMBeanException;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.state.ClusterStateDirectory;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.management.CausalClustering;

import static org.neo4j.causalclustering.core.consensus.log.RaftLog.RAFT_LOG_DIRECTORY_NAME;

@Service.Implementation( ManagementBeanProvider.class )
public class CausalClusteringBean extends ManagementBeanProvider
{
    @SuppressWarnings( "WeakerAccess" ) // Bean needs public constructor
    public CausalClusteringBean()
    {
        super( CausalClustering.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management )
    {
        if ( isCausalClustering( management ) )
        {
            return new CausalClusteringBeanImpl( management, false );
        }
        return null;
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management )
    {
        if ( isCausalClustering( management ) )
        {
            return new CausalClusteringBeanImpl( management, true );
        }
        return null;
    }

    private static boolean isCausalClustering( ManagementData management )
    {
        return management.getKernelData().graphDatabase() instanceof CoreGraphDatabase;
    }

    private static class CausalClusteringBeanImpl extends Neo4jMBean implements CausalClustering
    {
        private final ClusterStateDirectory clusterStateDirectory;
        private final RaftMachine raftMachine;
        private final FileSystemAbstraction fs;

        CausalClusteringBeanImpl( ManagementData management, boolean isMXBean )
        {
            super( management, isMXBean );
            clusterStateDirectory = management.resolveDependency( ClusterStateDirectory.class );
            raftMachine = management.resolveDependency( RaftMachine.class );

            fs = management.getKernelData().getFilesystemAbstraction();
        }

        @Override
        public String getRole()
        {
            return raftMachine.currentRole().toString();
        }

        @Override
        public long getRaftLogSize()
        {
            File raftLogDirectory = new File( clusterStateDirectory.get(), RAFT_LOG_DIRECTORY_NAME );
            return FileUtils.size( fs, raftLogDirectory );
        }

        @Override
        public long getReplicatedStateSize()
        {
            File replicatedStateDirectory = clusterStateDirectory.get();

            File[] files = fs.listFiles( replicatedStateDirectory );
            if ( files == null )
            {
                return 0L;
            }

            long size = 0L;
            for ( File file : files )
            {
                // Exclude raft log that resides in same directory
                if ( fs.isDirectory( file ) && file.getName().equals( RAFT_LOG_DIRECTORY_NAME ) )
                {
                    continue;
                }

                size += FileUtils.size( fs, file );
            }

            return size;
        }
    }
}
