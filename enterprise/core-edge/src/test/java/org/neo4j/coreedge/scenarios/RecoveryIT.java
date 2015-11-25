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
package org.neo4j.coreedge.scenarios;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.test.TargetDirectory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

import static org.junit.Assert.assertTrue;

import static org.neo4j.graphdb.DynamicLabel.label;

@Ignore("in progress")
public class RecoveryIT
{
    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    private Cluster cluster;

    @After
    public void shutdown()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldBeConsistentAfterShutdown() throws Exception
    {
        // given
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, 3, 0 );

        HashSet<File> storeDirs = new HashSet<>();

        for ( int i = 0; i < cluster.numberOfCoreServers(); i++ )
        {
            CoreGraphDatabase theDb = cluster.getCoreServerById( i );

            try ( Transaction tx = theDb.beginTx() )
            {
                Node node = theDb.createNode( label( "demo" ) );
                node.setProperty( "server", i );
                tx.success();
            }

            storeDirs.add( new File( theDb.getStoreDir() ) );
        }

        // when
        cluster.shutdown();

        // then
        for ( File storeDir : storeDirs )
        {
            isConsistent( storeDir );
        }
    }

    @Test
    public void singleServerWithinClusterShouldBeConsistentAfterRestart() throws Exception
    {
        // given
        int clusterSize = 3;
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, clusterSize, 0 );

        ArrayList<String> storeDirs = new ArrayList<>();

        for ( int i = 0; i < clusterSize; i++ )
        {
            CoreGraphDatabase theDb = cluster.getCoreServerById( i );

            try ( Transaction tx = theDb.beginTx() )
            {
                Node node = theDb.createNode( label( "first-round" ) );
                node.setProperty( "server", i );
                tx.success();
            }

            storeDirs.add( theDb.getStoreDir() );
        }

        // when
        for ( int i = 0; i < clusterSize; i++ )
        {
            cluster.removeCoreServerWithServerId( i );
            fireSomeLoadAtTheCluster( cluster );
            cluster.addCoreServerWithServerId( i, clusterSize );
        }

        cluster.shutdown();

        // then
        for ( int i = 0; i < clusterSize; i++ )
        {
            assertTrue( isConsistent( storeDirs.get( i ) ) );
        }
    }

    private boolean isConsistent( File storeDir ) throws IOException
    {
        return isConsistent( storeDir.getCanonicalPath() );
    }

    private boolean isConsistent( String storeDir ) throws IOException
    {
        ConsistencyCheckService.Result result;

        try
        {
            result = new ConsistencyCheckService().runFullConsistencyCheck( new File( storeDir ), new Config(),
                    ProgressMonitorFactory.NONE, FormattedLogProvider.toOutputStream( System.err ), true );
        }
        catch ( ConsistencyCheckIncompleteException e )
        {
            throw new RuntimeException( e );
        }

        return result.isSuccessful();
    }

    private void fireSomeLoadAtTheCluster( Cluster cluster )
    {
        for ( CoreGraphDatabase theDb : cluster.coreServers() )
        {
            boolean tryAgain;
            do
            {
                tryAgain = false;
                try ( Transaction tx = theDb.beginTx() )
                {
                    Node node = theDb.createNode( label( "second-round" ) );
                    node.setProperty( "server", "val" );
                    tx.success();
                }
                catch ( Throwable t )
                {
                    tryAgain = true;
                    parkNanos( MILLISECONDS.toNanos( 100 ) );
                }
            } while ( tryAgain );
        }
    }
}
