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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.PageCacheRule;

import static java.util.Collections.singletonList;
import static org.neo4j.causalclustering.discovery.Cluster.dataMatchesEventually;
import static org.neo4j.causalclustering.helpers.DataCreator.createEmptyNodes;

/**
 * Recovery scenarios where the transaction log was only partially written.
 */
public class TransactionLogRecoveryIT
{
    @Rule
    public final PageCacheRule pageCache = new PageCacheRule();

    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 3 );

    private Cluster cluster;
    private FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void coreShouldStartAfterPartialTransactionWriteCrash() throws Exception
    {
        // given: a fully synced cluster with some data
        dataMatchesEventually( createEmptyNodes( cluster, 10 ), cluster.coreMembers() );

        // when: shutting down a core
        CoreClusterMember core = cluster.getCoreMemberById( 0 );
        core.shutdown();

        // and making sure there will be something new to pull
        CoreClusterMember lastWrites = createEmptyNodes( cluster, 10 );

        // and writing a partial tx
        writePartialTx( core.storeDir() );

        // then: we should still be able to start
        core.start();

        // and become fully synced again
        dataMatchesEventually( lastWrites, singletonList( core ) );
    }

    @Test
    public void coreShouldStartWithSeedHavingPartialTransactionWriteCrash() throws Exception
    {
        // given: a fully synced cluster with some data
        dataMatchesEventually( createEmptyNodes( cluster, 10 ), cluster.coreMembers() );

        // when: shutting down a core
        CoreClusterMember core = cluster.getCoreMemberById( 0 );
        core.shutdown();

        // and making sure there will be something new to pull
        CoreClusterMember lastWrites = createEmptyNodes( cluster, 10 );

        // and writing a partial tx
        writePartialTx( core.storeDir() );

        // and deleting the cluster state, making sure a snapshot is required during startup
        // effectively a seeding scenario -- representing the use of the unbind command on a crashed store
        fs.deleteRecursively( core.clusterStateDirectory() );

        // then: we should still be able to start
        core.start();

        // and become fully synced again
        dataMatchesEventually( lastWrites, singletonList( core ) );
    }

    @Test
    public void readReplicaShouldStartAfterPartialTransactionWriteCrash() throws Exception
    {
        // given: a fully synced cluster with some data
        dataMatchesEventually( createEmptyNodes( cluster, 10 ), cluster.readReplicas() );

        // when: shutting down a read replica
        ReadReplica readReplica = cluster.getReadReplicaById( 0 );
        readReplica.shutdown();

        // and making sure there will be something new to pull
        CoreClusterMember lastWrites = createEmptyNodes( cluster, 10 );
        dataMatchesEventually( lastWrites, cluster.coreMembers() );

        // and writing a partial tx
        writePartialTx( readReplica.storeDir() );

        // then: we should still be able to start
        readReplica.start();

        // and become fully synced again
        dataMatchesEventually( lastWrites, singletonList( readReplica ) );
    }

    private void writePartialTx( File storeDir ) throws IOException
    {
        try ( PageCache pageCache = this.pageCache.getPageCache( fs ) )
        {
            LogFiles logFiles = LogFilesBuilder.activeFilesBuilder( storeDir, fs, pageCache ).build();
            try ( Lifespan ignored = new Lifespan( logFiles ) )
            {
                LogEntryWriter writer = new LogEntryWriter( logFiles.getLogFile().getWriter() );
                writer.writeStartEntry( 0, 0, 0x123456789ABCDEFL, logFiles.getLogFileInformation().getLastEntryId() + 1,
                        new byte[]{0} );
            }
        }
    }
}
