/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.ha;

import java.io.File;

import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.ha.BranchedDataPolicy;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.SillyUtils.nonNull;

public class TestBranchedData
{
    private final File dir = TargetDirectory.forTest( getClass() ).graphDbDir( true );

    @Test
    public void migrationOfBranchedDataDirectories() throws Exception
    {
        long[] timestamps = new long[3];
        for ( int i = 0; i < timestamps.length; i++ )
        {
            startDbAndCreateNode();
            timestamps[i] = moveAwayToLookLikeOldBranchedDirectory();
            Thread.sleep( 1 ); // To make sure we get different timestamps
        }

        new HighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( dir.getAbsolutePath() )
                .setConfig( ClusterSettings.server_id, "1" )
                .setConfig( ClusterSettings.initial_hosts, ":5001" )
                .newGraphDatabase().shutdown();
        // It should have migrated those to the new location. Verify that.
        for ( long timestamp : timestamps )
        {
            assertFalse( "directory branched-" + timestamp + " still exists.",
                    new File( dir, "branched-" + timestamp ).exists() );
            assertTrue( "directory " + timestamp + " is not there",
                    BranchedDataPolicy.getBranchedDataDirectory( dir, timestamp ).exists() );
        }
    }

    private long moveAwayToLookLikeOldBranchedDirectory()
    {
        long timestamp = System.currentTimeMillis();
        File branchDir = new File( dir, "branched-" + timestamp );
        assertTrue( "create directory: " + branchDir, branchDir.mkdirs() );
        for ( File file : nonNull( dir.listFiles() ) )
        {
            String fileName = file.getName();
            if ( !fileName.equals( StringLogger.DEFAULT_NAME ) && !file.getName().startsWith( "branched-" ) )
            {
                assertTrue( FileUtils.renameFile( file, new File( branchDir, file.getName() ) ) );
            }
        }
        return timestamp;
    }

    private void startDbAndCreateNode()
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( dir.getAbsolutePath() );
        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();
        tx.finish();
        db.shutdown();
    }
}
