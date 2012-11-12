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
package slavetest;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Ignore;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.HAGraphDb;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.ClusterClient;
import org.neo4j.kernel.ha.FakeClusterClient;
import org.neo4j.kernel.ha.FakeMasterBroker;
import org.neo4j.kernel.ha.FakeSlaveBroker;
import org.neo4j.kernel.ha.MasterImpl;

@Ignore( "SingleJvmWithNettyTest covers this and more" )
public class SingleJvmTest extends AbstractHaTest
{
    private MasterImpl master;
    private List<GraphDatabaseService> haDbs;

    protected GraphDatabaseService getSlave( int nr )
    {
        return haDbs.get( nr );
    }

    @Override
    protected int addDb( Map<String, String> config, boolean awaitStarted )
    {
        haDbs = haDbs != null ? haDbs : new ArrayList<GraphDatabaseService>();
        int machineId = haDbs.size()+1;
        haDbs.add( null );
        startDb( machineId, config, awaitStarted );
        return machineId;
    }

    @Override
    protected void startDb( int machineId, Map<String, String> config, boolean awaitStarted )
    {
        haDbs = haDbs != null ? haDbs : new ArrayList<GraphDatabaseService>();
        File slavePath = dbPath( machineId );
        PlaceHolderGraphDatabaseService placeHolderDb = new PlaceHolderGraphDatabaseService( slavePath.getAbsolutePath() );
        Broker broker = makeSlaveBroker( master, 0, machineId, placeHolderDb, config );
        Map<String,String> cfg = new HashMap<String, String>(config);
        cfg.put( HaConfig.CONFIG_KEY_SERVER_ID, Integer.toString(machineId) );
        cfg.put( Config.KEEP_LOGICAL_LOGS, "true" );
        addDefaultReadTimeout( cfg );
        HighlyAvailableGraphDatabase db = new HighlyAvailableGraphDatabase(
                new HAGraphDb( slavePath.getAbsolutePath(), cfg,
                        wrapBrokerAndSetPlaceHolderDb( placeHolderDb, broker ),
                        makeMasterClusterClientFromBroker( broker ) ) );
        placeHolderDb.setDb( db );
        haDbs.set( machineId-1, db );
    }

    @Override
    protected void awaitAllStarted() throws Exception
    {
    }

    @Override
    protected void shutdownDb( int machineId )
    {
        haDbs.get( machineId-1 ).shutdown();
    }

    @Override
    protected void startUpMaster( Map<String, String> extraConfig ) throws Exception
    {
        master = new MasterImpl( startUpMasterDb( extraConfig ), extraConfig );
    }

    protected PlaceHolderGraphDatabaseService startUpMasterDb( Map<String, String> extraConfig ) throws Exception
    {
        int masterId = 0;
        Map<String, String> config = MapUtil.stringMap( extraConfig,
                HaConfig.CONFIG_KEY_SERVER_ID, String.valueOf( masterId ) );
        addDefaultReadTimeout( config );
        String path = dbPath( 0 ).getAbsolutePath();
        PlaceHolderGraphDatabaseService placeHolderDb = new PlaceHolderGraphDatabaseService( path );
        Broker broker = makeMasterBroker( masterId, placeHolderDb, config );
        HighlyAvailableGraphDatabase db = new HighlyAvailableGraphDatabase(
                new HAGraphDb( path, config, wrapBrokerAndSetPlaceHolderDb(
                        placeHolderDb, broker ),
                        makeMasterClusterClientFromBroker( broker ) ) );
        placeHolderDb.setDb( db );
        return placeHolderDb;
    }

    private void addDefaultReadTimeout( Map<String, String> config )
    {
        if (!config.containsKey( HaConfig.CONFIG_KEY_READ_TIMEOUT ))
        {
            config.put( HaConfig.CONFIG_KEY_READ_TIMEOUT, String.valueOf( TEST_READ_TIMEOUT ) );
        }
    }

    protected Broker makeMasterBroker( int masterId,
            AbstractGraphDatabase graphDb, Map<String, String> config )
    {
        return new FakeMasterBroker( masterId, graphDb, config );
    }

    protected Broker makeSlaveBroker( MasterImpl master, int masterId, int id, AbstractGraphDatabase graphDb, Map<String, String> config )
    {
        return new FakeSlaveBroker( master, masterId, id, graphDb );
    }

    protected ClusterClient makeMasterClusterClientFromBroker( Broker broker )
    {
        return new FakeClusterClient( broker );
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
        try
        {
            verify( master.getGraphDb(), haDbs.toArray( new GraphDatabaseService[haDbs.size()] ) );
        }
        finally
        {
            shutdownDbs();
        }

        if ( !shouldDoVerificationAfterTests() )
        {
            return;
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
        shutdownMaster();
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

    @Override
    protected CommonJobs.ShutdownDispatcher getMasterShutdownDispatcher()
    {
        return new CommonJobs.ShutdownDispatcher()
        {
            public void doShutdown()
            {
                shutdownMaster();
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

    protected void shutdownMaster()
    {
        getMasterHaDb().shutdown();
        getMaster().shutdown();
    }

    protected HighlyAvailableGraphDatabase getMasterHaDb()
    {
        return (HighlyAvailableGraphDatabase) getMaster().getGraphDb();
    }
}
