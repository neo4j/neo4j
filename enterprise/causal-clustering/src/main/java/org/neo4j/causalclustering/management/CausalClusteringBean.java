/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
