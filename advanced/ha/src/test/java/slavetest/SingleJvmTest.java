/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package slavetest;

import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.FakeMasterBroker;
import org.neo4j.kernel.ha.FakeSlaveBroker;
import org.neo4j.kernel.ha.MasterImpl;

public class SingleJvmTest extends AbstractHaTest
{
    private MasterImpl master;
    private List<GraphDatabaseService> haDbs;

    protected GraphDatabaseService getSlave( int nr )
    {
        return haDbs.get( nr );
    }

    @Override
    protected void addDb( Map<String, String> config )
    {
        haDbs = haDbs != null ? haDbs : new ArrayList<GraphDatabaseService>();
        int machineId = haDbs.size()+1;
        File slavePath = dbPath( machineId );
        PlaceHolderGraphDatabaseService placeHolderDb = new PlaceHolderGraphDatabaseService( slavePath.getAbsolutePath() );
        Broker broker = makeSlaveBroker( master, 0, machineId, placeHolderDb );
        Map<String,String> cfg = new HashMap<String, String>(config);
        cfg.put( HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID, Integer.toString(machineId) );
        cfg.put( Config.KEEP_LOGICAL_LOGS, "true" );
        HighlyAvailableGraphDatabase db = new HighlyAvailableGraphDatabase(
                slavePath.getAbsolutePath(), cfg, wrapBrokerAndSetPlaceHolderDb( placeHolderDb, broker ) );
        placeHolderDb.setDb( db );
        haDbs.add( db );
    }

    @Override
    protected void awaitAllStarted() throws Exception
    {
    }

    @Override
    protected void startUpMaster( Map<String, String> extraConfig ) throws Exception
    {
        int masterId = 0;
        Map<String, String> config = MapUtil.stringMap( extraConfig,
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID, String.valueOf( masterId ) );
        String path = dbPath( 0 ).getAbsolutePath();
        PlaceHolderGraphDatabaseService placeHolderDb = new PlaceHolderGraphDatabaseService( path );
        Broker broker = makeMasterBroker( master, masterId, placeHolderDb );
        HighlyAvailableGraphDatabase db = new HighlyAvailableGraphDatabase( path,
                config, wrapBrokerAndSetPlaceHolderDb( placeHolderDb, broker ) );
        placeHolderDb.setDb( db );
        // db.newMaster( null, new Exception() );
        master = new MasterImpl( db );
    }

    protected Broker makeMasterBroker( MasterImpl master, int masterId, GraphDatabaseService graphDb )
    {
        return new FakeMasterBroker( masterId, graphDb );
    }

    protected Broker makeSlaveBroker( MasterImpl master, int masterId, int id, GraphDatabaseService graphDb )
    {
        return new FakeSlaveBroker( master, masterId, id, graphDb );
    }

    protected MasterImpl getMaster()
    {
        return master;
    }

    @Override
    protected void shutdownDbs()
    {
        for ( GraphDatabaseService haDb : haDbs )
        {
            haDb.shutdown();
        }
        master.getGraphDb().shutdown();
    }

    @After
    public void verifyAndShutdownDbs()
    {
        if ( Config.osIsWindows() ) return;
        try
        {
            verify( master.getGraphDb(), haDbs.toArray( new GraphDatabaseService[haDbs.size()] ) );
        }
        finally
        {
            shutdownDbs();
        }

        GraphDatabaseService masterOfflineDb =
                new EmbeddedGraphDatabase( dbPath( 0 ).getAbsolutePath() );
        GraphDatabaseService[] slaveOfflineDbs = new GraphDatabaseService[haDbs.size()];
        for ( int i = 1; i <= haDbs.size(); i++ )
        {
            slaveOfflineDbs[i-1] = new EmbeddedGraphDatabase( dbPath( i ).getAbsolutePath() );
        }
        try
        {
            verify( masterOfflineDb, slaveOfflineDbs );
        }
        finally
        {
            masterOfflineDb.shutdown();
            for ( GraphDatabaseService db : slaveOfflineDbs )
            {
                db.shutdown();
            }
        }
    }

    @Override
    protected <T> T executeJob( Job<T> job, int slave ) throws Exception
    {
        return job.execute( haDbs.get( slave ) );
    }

    @Override
    protected <T> T executeJobOnMaster( Job<T> job ) throws Exception
    {
        return job.execute( master.getGraphDb() );
    }

    @Override
    protected void pullUpdates( int... ids )
    {
        if ( ids.length == 0 )
        {
            for ( GraphDatabaseService db : haDbs )
            {
                ((HighlyAvailableGraphDatabase) db).pullUpdates();
            }
        }
        else
        {
            for ( int id : ids )
            {
                ((HighlyAvailableGraphDatabase) haDbs.get( id )).pullUpdates();
            }
        }
    }

    @Test
    public void testMixingEntitiesFromWrongDbs() throws Exception
    {
        if ( Config.osIsWindows() ) return;

        initializeDbs( 1 );
        GraphDatabaseService haDb1 = haDbs.get( 0 );
        GraphDatabaseService mDb = master.getGraphDb();

        Transaction tx = mDb.beginTx();
        Node masterNode;
        try
        {
            masterNode = mDb.createNode();
            mDb.getReferenceNode().createRelationshipTo( masterNode, CommonJobs.REL_TYPE );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        tx = haDb1.beginTx();
        // try throw in node that does not exist and no tx on mdb
        try
        {
            Node node = haDb1.createNode();
            mDb.getReferenceNode().createRelationshipTo( node, CommonJobs.KNOWS );
            fail( "Should throw not found exception" );
        }
        catch ( NotFoundException e )
        {
            // good
        }
        finally
        {
            tx.finish();
        }
    }

    @Override
    protected CommonJobs.ShutdownDispatcher getMasterShutdownDispatcher()
    {
        return new CommonJobs.ShutdownDispatcher()
        {
            public void doShutdown()
            {
                master.getGraphDb().shutdown();
            }
        };
    }

    @Override
    protected Fetcher<DoubleLatch> getDoubleLatch()
    {
        return new Fetcher<DoubleLatch>()
        {
            private final DoubleLatch latch = new DoubleLatch()
            {
                private final CountDownLatch first = new CountDownLatch( 1 );
                private final CountDownLatch second = new CountDownLatch( 1 );

                public void countDownSecond()
                {
                    second.countDown();
                }

                public void countDownFirst()
                {
                    first.countDown();
                }

                public void awaitSecond()
                {
                    await( second );
                }

                public void awaitFirst()
                {
                    await( first );
                }

                private void await( CountDownLatch latch )
                {
                    try
                    {
                        latch.await();
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.interrupted();
                        e.printStackTrace();
                    }
                }
            };

            public DoubleLatch fetch()
            {
                return latch;
            }

            public void close()
            {
            }
        };
    }
}
