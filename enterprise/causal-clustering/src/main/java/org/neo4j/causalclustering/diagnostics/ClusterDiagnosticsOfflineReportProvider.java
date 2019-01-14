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
package org.neo4j.causalclustering.diagnostics;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;

import org.neo4j.causalclustering.core.consensus.log.segmented.FileNames;
import org.neo4j.causalclustering.core.state.ClusterStateDirectory;
import org.neo4j.causalclustering.core.state.ClusterStateException;
import org.neo4j.diagnostics.DiagnosticsOfflineReportProvider;
import org.neo4j.diagnostics.DiagnosticsReportSource;
import org.neo4j.diagnostics.DiagnosticsReportSources;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLog;

import static org.neo4j.causalclustering.core.consensus.log.RaftLog.RAFT_LOG_DIRECTORY_NAME;

public class ClusterDiagnosticsOfflineReportProvider extends DiagnosticsOfflineReportProvider
{
    private FileSystemAbstraction fs;
    private File clusterStateDirectory;
    private ClusterStateException clusterStateException;

    public ClusterDiagnosticsOfflineReportProvider()
    {
        super( "cc", "raft", "ccstate" );
    }

    @Override
    public void init( FileSystemAbstraction fs, Config config, File storeDirectory )
    {
        this.fs = fs;
        final File dataDir = config.get( GraphDatabaseSettings.data_directory );
        try
        {
            clusterStateDirectory = new ClusterStateDirectory( dataDir, storeDirectory, true ).initialize( fs ).get();
        }
        catch ( ClusterStateException e )
        {
            clusterStateException = e;
        }
    }

    @Override
    protected List<DiagnosticsReportSource> provideSources( Set<String> classifiers )
    {
        List<DiagnosticsReportSource> sources = new ArrayList<>();
        if ( classifiers.contains( "raft" ) )
        {
            getRaftLogs( sources );
        }
        if ( classifiers.contains( "ccstate" ) )
        {
            getClusterState( sources );
        }

        return sources;
    }

    private void getRaftLogs( List<DiagnosticsReportSource> sources )
    {
        if ( clusterStateDirectory == null )
        {
            sources.add( DiagnosticsReportSources.newDiagnosticsString( "raft.txt",
                    () -> "error creating ClusterStateDirectory: " + clusterStateException.getMessage() ) );
            return;
        }

        File raftLogDirectory = new File( clusterStateDirectory, RAFT_LOG_DIRECTORY_NAME );
        FileNames fileNames = new FileNames( raftLogDirectory );
        SortedMap<Long,File> allFiles = fileNames.getAllFiles( fs, NullLog.getInstance() );

        for ( File logFile : allFiles.values() )
        {
            sources.add( DiagnosticsReportSources.newDiagnosticsFile( "raft/" + logFile.getName(), fs, logFile ) );
        }
    }

    private void getClusterState( List<DiagnosticsReportSource> sources )
    {
        if ( clusterStateDirectory == null )
        {
            sources.add( DiagnosticsReportSources.newDiagnosticsString( "ccstate.txt",
                    () -> "error creating ClusterStateDirectory: " + clusterStateException.getMessage() ) );
            return;
        }

        for ( File file : fs.listFiles( clusterStateDirectory, ( dir, name ) -> !name.equals( RAFT_LOG_DIRECTORY_NAME ) ) )
        {
            addDirectory( "ccstate", file, sources );
        }
    }

    /**
     * Add all files in a directory recursively.
     *
     * @param path current relative path for destination.
     * @param dir current directory or file.
     * @param sources list of source that will be accumulated.
     */
    private void addDirectory( String path, File dir, List<DiagnosticsReportSource> sources )
    {
        String currentLevel = path + File.separator + dir.getName();
        if ( fs.isDirectory( dir ) )
        {
            File[] files = fs.listFiles( dir );
            if ( files != null )
            {
                for ( File file : files )
                {
                    addDirectory( currentLevel, file, sources );
                }
            }
        }
        else // File
        {
            sources.add( DiagnosticsReportSources.newDiagnosticsFile( currentLevel, fs, dir ) );
        }
    }
}
