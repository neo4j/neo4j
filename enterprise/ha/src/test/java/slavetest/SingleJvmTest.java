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
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.ha.FakeClusterChecker;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ClusterChecker;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.ClusterClient;
import org.neo4j.kernel.ha.FakeClusterClient;
import org.neo4j.kernel.ha.FakeMasterBroker;
import org.neo4j.kernel.ha.FakeSlaveBroker;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.MasterImpl;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.test.BatchTransaction;

@Ignore( "SingleJvmWithNettyTest covers this and more" )
public class SingleJvmTest extends AbstractHaTest
{
    private TestMaster master;
    private List<GraphDatabaseAPI> haDbs;
    protected FakeMasterBroker masterBroker;

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

    protected GraphDatabaseAPI getSlave( int nr )
    {
        return haDbs.get( nr );
    }

    @Override
    protected int addDb( Map<String, String> config, boolean awaitStarted )
    {
        haDbs = haDbs != null ? haDbs : new ArrayList<GraphDatabaseAPI>();
        int machineId = haDbs.size()+1;
        haDbs.add( null );
        GraphDatabaseAPI db = startDb( machineId, config, awaitStarted );
        return machineId;
    }
    
    @Override
    protected void createBigMasterStore( int numberOfMegabytes )
    {
        GraphDatabaseAPI db = getMaster().getGraphDb();
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
    protected GraphDatabaseAPI startDb( final int machineId, final Map<String, String> config, boolean awaitStarted )
    {
        haDbs = haDbs != null ? haDbs : new ArrayList<GraphDatabaseAPI>();
        File slavePath = dbPath( machineId );
        final Map<String,String> cfg = new HashMap<String, String>( config );
        cfg.put( HaSettings.server_id.name(), Integer.toString(machineId) );
        cfg.put( GraphDatabaseSettings.keep_logical_logs.name(), GraphDatabaseSetting.TRUE );
        addDefaultConfig( cfg );
        HighlyAvailableGraphDatabase haGraphDb = new HighlyAvailableGraphDatabase(slavePath.getAbsolutePath(), cfg, 
                Service.load( IndexProvider.class ), Service.load( KernelExtension.class ), Service.load( CacheProvider.class ) )
        {
            public Broker slaveBroker;

            @Override
            protected Broker createBroker()
            {
                return slaveBroker;
            }
            
            @Override
            protected ClusterClient createClusterClient()
            {
                slaveBroker = makeSlaveBroker( master, 0, machineId, this, cfg );
                return makeMasterClusterClientFromBroker( slaveBroker );
            }

            @Override
            protected ClusterChecker createClusterChecker()
            {
                return new FakeClusterChecker();
            }
        };
        
        haDbs.set( machineId-1, haGraphDb );
        return haGraphDb;
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
        extraConfig = new ConfigurationDefaults( HaSettings.class ).apply( extraConfig );

        int timeOut = Integer.parseInt(extraConfig.containsKey( HaSettings.lock_read_timeout.name() ) ? extraConfig.get( HaSettings.lock_read_timeout.name() ) : extraConfig.get( HaSettings.read_timeout.name() ));
        HighlyAvailableGraphDatabase db = startUpMasterDb( extraConfig );
        master = new TestMaster( new MasterImpl( db, timeOut ), db );
    }
    
    protected HighlyAvailableGraphDatabase startUpMasterDb( Map<String, String> extraConfig ) throws Exception
    {
        final int masterId = 0;
        final Map<String, String> config = MapUtil.stringMap( extraConfig,
                HaSettings.server_id.name(), String.valueOf( masterId ) );
        addDefaultConfig( config );
        String path = dbPath( 0 ).getAbsolutePath();
        HighlyAvailableGraphDatabase haGraphDb = new HighlyAvailableGraphDatabase(
            path, config, Service.load( IndexProvider.class ), Service.load( KernelExtension.class ), Service.load( CacheProvider.class ) )
        {
            @Override
            protected Broker createBroker()
            {
                return masterBroker;
            }
            
            @Override
            protected ClusterClient createClusterClient()
            {
                masterBroker = makeMasterBroker( config );

                return makeMasterClusterClientFromBroker( masterBroker );
            }

            @Override
            protected ClusterChecker createClusterChecker()
            {
                return new FakeClusterChecker();
            }
        };
        return haGraphDb;
    }

    protected FakeMasterBroker makeMasterBroker( Map<String, String> config )
    {
        config = new ConfigurationDefaults(GraphDatabaseSettings.class, HaSettings.class ).apply( config );
        Config configuration = new Config( config );

        return new FakeMasterBroker( configuration );
    }

    protected Broker makeSlaveBroker( TestMaster master, int masterId, int id, HighlyAvailableGraphDatabase graphDb, Map<String, String> config )
    {
        config = new ConfigurationDefaults(GraphDatabaseSettings.class, HaSettings.class ).apply( config );
        Config configuration = new Config( config );
        return new FakeSlaveBroker( master, masterId, configuration );
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
