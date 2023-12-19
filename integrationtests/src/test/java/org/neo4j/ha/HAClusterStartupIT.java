/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.ha;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertTrue;
import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

@RunWith( Enclosed.class )
public class HAClusterStartupIT
{
    public static class SimpleCluster
    {
        @Rule
        public final ClusterRule clusterRule = new ClusterRule().withCluster( clusterOfSize( 3 ) );
        private HighlyAvailableGraphDatabase oldMaster;
        private HighlyAvailableGraphDatabase oldSlave1;
        private HighlyAvailableGraphDatabase oldSlave2;

        @Before
        public void setup() throws Throwable
        {
            // setup a cluster with some data and entries in log files in fully functional and shutdown state
            ClusterManager.ManagedCluster cluster = clusterRule.startCluster();

            try
            {
                cluster.await( allSeesAllAsAvailable() );

                oldMaster = cluster.getMaster();
                createSomeData( oldMaster );
                cluster.sync();

                oldSlave1 = cluster.getAnySlave();
                oldSlave2 = cluster.getAnySlave( oldSlave1 );
            }
            finally
            {
                clusterRule.shutdownCluster();
            }
            assertAllStoreConsistent( cluster );
        }

        @Test
        public void allClusterNodesShouldSupportTheBuiltInProcedures() throws Throwable
        {
            ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
            try
            {
                for ( HighlyAvailableGraphDatabase gdb : cluster.getAllMembers() )
                {
                    // (1) BuiltInProcedures from community
                    {
                        Result result = gdb.execute( "CALL dbms.procedures()" );
                        assertTrue( result.hasNext() );
                        result.close();
                    }

                    // (2) BuiltInProcedures from enterprise
                    try ( InternalTransaction tx = gdb.beginTransaction(
                        KernelTransaction.Type.explicit,
                        EnterpriseLoginContext.AUTH_DISABLED
                    ) )
                    {
                        Result result = gdb.execute( tx, "CALL dbms.listQueries()", EMPTY_MAP );
                        assertTrue( result.hasNext() );
                        result.close();

                        tx.success();
                    }
                }
            }
            finally
            {
                cluster.shutdown();
            }
        }

        @Test
        public void aSlaveWithoutAnyGraphDBFilesShouldBeAbleToJoinACluster() throws Throwable
        {
            // WHEN removing all the files in graphdb on the slave and restarting the cluster
            deleteAllFilesOn( oldSlave1 );

            // THEN the cluster should work
            restartingTheClusterShouldWork( clusterRule );
        }

        @Test
        public void bothSlavesWithoutAnyGraphDBFilesShouldBeAbleToJoinACluster() throws Throwable
        {
            // WHEN removing all the files in graphdb on both slaves and restarting the cluster
            deleteAllFilesOn( oldSlave1 );
            deleteAllFilesOn( oldSlave2 );

            // THEN the cluster should work
            restartingTheClusterShouldWork( clusterRule );
        }

        @Test
        public void theMasterWithoutAnyGraphDBFilesShouldBeAbleToJoinACluster() throws Throwable
        {
            // WHEN removing all the files in graphdb on the db that was master and restarting the cluster
            deleteAllFilesOn( oldMaster );

            // THEN the cluster should work
            restartingTheClusterShouldWork( clusterRule );
        }

        @Test
        public void aSlaveWithoutLogicalLogFilesShouldBeAbleToJoinACluster() throws Throwable
        {
            // WHEN removing all logical log files in graphdb on the slave and restarting the cluster
            deleteAllLogsOn( oldSlave1 );

            // THEN the cluster should work
            restartingTheClusterShouldWork( clusterRule );
        }

        @Test
        public void bothSlaveWithoutLogicalLogFilesShouldBeAbleToJoinACluster() throws Throwable
        {
            // WHEN removing all logical log files in graphdb on the slave and restarting the cluster
            deleteAllLogsOn( oldSlave1 );
            deleteAllLogsOn( oldSlave2 );

            // THEN the cluster should work
            restartingTheClusterShouldWork( clusterRule );
        }
    }

    public static class ClusterWithSeed
    {
        @Rule
        public final ClusterRule clusterRule = new ClusterRule().withCluster( clusterOfSize( 3 ) )
                .withSeedDir( dbWithOutLogs() );

        public ClusterWithSeed() throws IOException
        {
        }

        @Test
        public void aClusterShouldStartAndRunWhenSeededWithAStoreHavingNoLogicalLogFiles() throws Throwable
        {
            // WHEN removing all logical log files in graphdb on the slave and restarting a new cluster
            // THEN the new cluster should work
            restartingTheClusterShouldWork( clusterRule );
        }

        private static File dbWithOutLogs() throws IOException
        {
            File seedDir;
            try
            {
                seedDir = Files.createTempDirectory( "seed-database" ).toFile();
                seedDir.deleteOnExit();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }

            GraphDatabaseService db = null;
            try
            {
                db = new EnterpriseGraphDatabaseFactory()
                        .newEmbeddedDatabaseBuilder( seedDir )
                        .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                        .newGraphDatabase();
                createSomeData( db );
            }
            finally
            {
                if ( db != null )
                {
                    db.shutdown();
                }
            }

            deleteAllLogsOn( seedDir );

            return seedDir;
        }
    }

    private static void createSomeData( GraphDatabaseService oldMaster )
    {
        try ( Transaction tx = oldMaster.beginTx() )
        {
            oldMaster.createNode();
            tx.success();
        }
    }

    private static void deleteAllFilesOn( HighlyAvailableGraphDatabase instance ) throws IOException
    {
        FileUtils.deleteRecursively( instance.getStoreDirectory() );
    }

    private static void deleteAllLogsOn( HighlyAvailableGraphDatabase instance ) throws IOException
    {
        deleteAllLogsOn( instance.getStoreDirectory() );
    }

    private static void deleteAllLogsOn( File storeDirectory ) throws IOException
    {
        try ( DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( storeDirectory, fileSystem ).build();
            for ( File file : logFiles.logFiles() )
            {
                fileSystem.deleteFile( file );
            }
        }
    }

    private static void restartingTheClusterShouldWork( ClusterRule clusterRule ) throws Exception
    {
        ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
        try
        {
            cluster.await( allSeesAllAsAvailable(), 180 );
        }
        finally
        {
            clusterRule.shutdownCluster();
        }

        assertAllStoreConsistent( cluster );
    }

    private static void assertAllStoreConsistent( ClusterManager.ManagedCluster cluster )
            throws ConsistencyCheckIncompleteException
    {
        for ( HighlyAvailableGraphDatabase slave : cluster.getAllMembers() )
        {
            assertConsistentStore( slave.getStoreDirectory() );
        }
    }
}
