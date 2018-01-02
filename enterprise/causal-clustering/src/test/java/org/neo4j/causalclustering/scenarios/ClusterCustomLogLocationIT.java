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
package org.neo4j.causalclustering.scenarios;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.logFilesBasedOnlyBuilder;

public class ClusterCustomLogLocationIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 2 );

    @Test
    public void clusterWithCustomTransactionLogLocation() throws Exception
    {
        Cluster cluster = clusterRule.startCluster();

        for ( int i = 0; i < 10; i++ )
        {
            cluster.coreTx( ( db, tx ) ->
            {
                db.createNode();
                tx.success();
            } );
        }

        Collection<CoreClusterMember> coreClusterMembers = cluster.coreMembers();
        for ( CoreClusterMember coreClusterMember : coreClusterMembers )
        {
            DependencyResolver dependencyResolver = coreClusterMember.database().getDependencyResolver();
            LogFiles logFiles = dependencyResolver.resolveDependency( LogFiles.class );
            assertEquals( logFiles.logFilesDirectory().getName(), "core-tx-logs-" + coreClusterMember.serverId() );
            assertTrue( logFiles.hasAnyEntries( 0 ) );
            File[] coreLogDirectories = coreClusterMember.storeDir().listFiles( file -> file.getName().startsWith( "core" ) );
            assertThat( coreLogDirectories, Matchers.arrayWithSize( 1 ) );

            logFileInStoreDirectoryDoesNotExist( coreClusterMember.storeDir(), dependencyResolver );
        }

        Collection<ReadReplica> readReplicas = cluster.readReplicas();
        for ( ReadReplica readReplica : readReplicas )
        {
            readReplica.txPollingClient().upToDateFuture().get();
            DependencyResolver dependencyResolver = readReplica.database().getDependencyResolver();
            LogFiles logFiles = dependencyResolver.resolveDependency( LogFiles.class );
            assertEquals( logFiles.logFilesDirectory().getName(), "replica-tx-logs-" + readReplica.serverId() );
            assertTrue( logFiles.hasAnyEntries( 0 ) );
            File[] replicaLogDirectories = readReplica.storeDir().listFiles( file -> file.getName().startsWith( "replica" ) );
            assertThat( replicaLogDirectories, Matchers.arrayWithSize( 1 ) );

            logFileInStoreDirectoryDoesNotExist( readReplica.storeDir(), dependencyResolver );
        }
    }

    private void logFileInStoreDirectoryDoesNotExist( File storeDir, DependencyResolver dependencyResolver ) throws IOException
    {
        FileSystemAbstraction fileSystem = dependencyResolver.resolveDependency( FileSystemAbstraction.class );
        LogFiles storeLogFiles = logFilesBasedOnlyBuilder( storeDir, fileSystem ).build();
        assertFalse( storeLogFiles.versionExists( 0 ) );
    }
}
