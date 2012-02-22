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
import org.neo4j.com.Client;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.ConfigProxy;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.GraphDatabaseSPI;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.AbstractBroker;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.ClusterClient;
import org.neo4j.kernel.ha.FakeClusterClient;
import org.neo4j.kernel.ha.FakeMasterBroker;
import org.neo4j.kernel.ha.FakeSlaveBroker;
import org.neo4j.kernel.ha.MasterImpl;
import org.neo4j.kernel.ha.zookeeper.ZooClient;
import org.neo4j.test.BatchTransaction;

@Ignore( "SingleJvmWithNettyTest covers this and more" )
public class SingleJvmTest extends AbstractHaTest
{
    private TestMaster master;
    private List<GraphDatabaseSPI> haDbs;

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
    }

    protected GraphDatabaseSPI getSlave( int nr )
    {
        return haDbs.get( nr );
    }

    @Override
    protected int addDb( Map<String, String> config, boolean awaitStarted )
    {
        haDbs = haDbs != null ? haDbs : new ArrayList<GraphDatabaseSPI>();
        int machineId = haDbs.size()+1;
        haDbs.add( null );
        startDb( machineId, config, awaitStarted );
        return machineId;
    }
    
    @Override
    protected void createBigMasterStore( int numberOfMegabytes )
    {
        GraphDatabaseSPI db = getMaster().getGraphDb();
        BatchTransaction tx = BatchTransaction.beginBatchTx( db );
        try
        {
            byte[] array = new byte[100000];
            for ( int i = 0; i < numberOfMegabytes*10; i++ )
            {
                Node node = db.createNode();
                node.setProperty( "array", array );
                tx.increment();
            }
        }
        finally
        {
            tx.finish();
        }
    }

    @Override
    protected void startDb( final int machineId, final Map<String, String> config, boolean awaitStarted )
    {
        haDbs = haDbs != null ? haDbs : new ArrayList<GraphDatabaseSPI>();
        File slavePath = dbPath( machineId );
        final Map<String,String> cfg = new HashMap<String, String>(config);
        cfg.put( HaConfig.CONFIG_KEY_SERVER_ID, Integer.toString(machineId) );
        cfg.put( Config.KEEP_LOGICAL_LOGS, "true" );
        addDefaultReadTimeout( cfg );
        HighlyAvailableGraphDatabase haGraphDb = new HighlyAvailableGraphDatabase(slavePath.getAbsolutePath(), cfg)
        {
            @Override
            protected Broker createBroker()
            {
                return makeSlaveBroker( master, 0, machineId, this, cfg );
            }
            
            @Override
            protected ClusterClient createClusterClient()
            {
                return makeMasterClusterClientFromBroker( getBroker() );
            }
        };
        
        haDbs.set( machineId-1, haGraphDb );
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
        ZooClient.Configuration zooConfig = ConfigProxy.config( extraConfig , ZooClient.Configuration.class );
        int timeOut = zooConfig.lock_read_timeout( zooConfig.read_timeout( Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS ) );
        HighlyAvailableGraphDatabase db = startUpMasterDb( extraConfig );
        master = new TestMaster( new MasterImpl( db, timeOut ), db );
    }
    
    protected HighlyAvailableGraphDatabase startUpMasterDb( Map<String, String> extraConfig ) throws Exception
    {
        final int masterId = 0;
        final Map<String, String> config = MapUtil.stringMap( extraConfig,
                HaConfig.CONFIG_KEY_SERVER_ID, String.valueOf( masterId ) );
        addDefaultReadTimeout( config );
        String path = dbPath( 0 ).getAbsolutePath();
        HighlyAvailableGraphDatabase haGraphDb = new HighlyAvailableGraphDatabase(
            path, config)
        {
            @Override
            protected Broker createBroker()
            {
                return makeMasterBroker( masterId, this, config );
            }
            
            @Override
            protected ClusterClient createClusterClient()
            {
                return makeMasterClusterClientFromBroker( getBroker() );
            }
        };
        return haGraphDb;
    }

    private void addDefaultReadTimeout( Map<String, String> config )
    {
        if (!config.containsKey( HaConfig.CONFIG_KEY_READ_TIMEOUT ))
        {
            config.put( HaConfig.CONFIG_KEY_READ_TIMEOUT, String.valueOf( TEST_READ_TIMEOUT ) );
        }
    }

    protected Broker makeMasterBroker( int masterId, GraphDatabaseSPI graphDb, Map<String, String> config )
    {
        ZooClient.Configuration zooConfig = ConfigProxy.config( config , ZooClient.Configuration.class );
        AbstractBroker.Configuration brokerConfig = ConfigProxy.config( config , AbstractBroker.Configuration.class );

        return new FakeMasterBroker( brokerConfig, zooConfig);
    }

    protected Broker makeSlaveBroker( TestMaster master, int masterId, int id, HighlyAvailableGraphDatabase graphDb, Map<String, String> config )
    {
        return new FakeSlaveBroker( master, masterId, ConfigProxy.config( config, AbstractBroker.Configuration.class ) );
    }

    protected ClusterClient makeMasterClusterClientFromBroker( Broker broker )
    {
        return new FakeClusterClient( broker );
    }

    protected TestMaster getMaster()
    {
        return master;
    }

    @Override
    protected void shutdownDbs()
    {
        if (haDbs != null)
            for ( GraphDatabaseService haDb : haDbs )
            {
                haDb.shutdown();
            }
        master.getGraphDb().shutdown();
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
