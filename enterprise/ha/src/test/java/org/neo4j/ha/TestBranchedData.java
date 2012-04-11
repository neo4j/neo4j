/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.io.File;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.HAGraphDb;
import org.neo4j.kernel.HAGraphDb.BranchedDataPolicy;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.BrokerFactory;
import org.neo4j.kernel.ha.FakeClusterClient;
import org.neo4j.kernel.ha.FakeMasterBroker;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.TargetDirectory;

import slavetest.PlaceHolderGraphDatabaseService;

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
        
        final PlaceHolderGraphDatabaseService placeHolderDb = new PlaceHolderGraphDatabaseService( dir.getAbsolutePath() );
        final Broker broker = new FakeMasterBroker( 1, placeHolderDb, MapUtil.stringMap() );
        HAGraphDb db = new HAGraphDb( dir.getAbsolutePath(), stringMap( HaConfig.CONFIG_KEY_SERVER_ID, "1" ),
                new BrokerFactory()
                {
                    @Override
                    public Broker create( AbstractGraphDatabase graphDb, Map<String, String> graphDbConfig )
                    {
                        placeHolderDb.setDb( graphDb );
                        return broker;
                    }
                },
                new FakeClusterClient( broker ) );
        db.shutdown();
        
        // It should have migrated those to the new location. Verify that.
        for ( long timestamp : timestamps )
        {
            assertFalse( new File( dir, "branched-" + timestamp ).exists() );
            assertTrue( BranchedDataPolicy.getBranchedDataDirectory( dir.getAbsolutePath(), timestamp ).exists() );
        }
    }

    private long moveAwayToLookLikeOldBranchedDirectory()
    {
        long timestamp = System.currentTimeMillis();
        File branchDir = new File( dir, "branched-" + System.currentTimeMillis() );
        branchDir.mkdirs();
        for ( File file : dir.listFiles() )
        {
            if ( !file.equals( StringLogger.DEFAULT_NAME ) && !file.getName().startsWith( "branched-" ) )
                assertTrue( FileUtils.renameFile( file, new File( branchDir, file.getName() ) ) );
        }
        return timestamp;
    }

    private void startDbAndCreateNode()
    {
        EmbeddedGraphDatabase db = new EmbeddedGraphDatabase( dir.getAbsolutePath() );
        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();
        tx.finish();
        db.shutdown();
    }
}
