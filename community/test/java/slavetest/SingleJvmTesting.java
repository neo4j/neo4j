package slavetest;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
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
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.AbstractBroker;
import org.neo4j.kernel.ha.FakeBroker;
import org.neo4j.kernel.ha.MasterImpl;

public class SingleJvmTesting extends AbstractHaTest
{
    private MasterImpl master;
    private List<GraphDatabaseService> haDbs;
    
    protected GraphDatabaseService getSlave( int nr )
    {
        return haDbs.get( nr );
    }
    
    protected void initializeDbs( int numSlaves, Map<String,String> config )
    {
        try
        {
            createDeadDbs( numSlaves );
            haDbs = new ArrayList<GraphDatabaseService>();
            startUpMaster( config );
            FakeBroker broker = new FakeBroker( master ); 
            for ( int i = 1; i <= numSlaves; i++ )
            {
                File slavePath = dbPath( i );
                Map<String,String> cfg = new HashMap<String, String>(config);
                cfg.put( HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID, Integer.toString(i) );
                cfg.put( Config.KEEP_LOGICAL_LOGS, "true" );
                GraphDatabaseService db = new HighlyAvailableGraphDatabase(
                        slavePath.getAbsolutePath(), cfg, AbstractBroker.wrapSingleBroker( broker ) );
                haDbs.add( db );
                broker.setDb( db );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    @Override
    protected void startUpMaster( Map<String, String> config )
    {
        master = new MasterImpl( new EmbeddedGraphDatabase( dbPath( 0 ).getAbsolutePath() ) );
    }

    protected MasterImpl getMaster()
    {
        return master;
    }
    
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
        verify( new VerifyDbContext( master.getGraphDb() ), getVerifyDbArray() );
        shutdownDbs();
        
        VerifyDbContext masterOfflineDb =
                new VerifyDbContext( new EmbeddedGraphDatabase( dbPath( 0 ).getAbsolutePath() ) );
        VerifyDbContext[] slaveOfflineDbs = new VerifyDbContext[haDbs.size()];
        for ( int i = 1; i <= haDbs.size(); i++ )
        {
            slaveOfflineDbs[i-1] = new VerifyDbContext( new EmbeddedGraphDatabase( dbPath( i ).getAbsolutePath() ) );
        }
        verify( masterOfflineDb, slaveOfflineDbs );
        masterOfflineDb.db.shutdown();
        for ( VerifyDbContext db : slaveOfflineDbs )
        {
            db.db.shutdown();
        }
    }
    
    private VerifyDbContext[] getVerifyDbArray()
    {
        VerifyDbContext[] contexts = new VerifyDbContext[haDbs.size()];
        for ( int i = 0; i < contexts.length; i++ )
        {
            contexts[i] = new VerifyDbContext( haDbs.get( i ) );
        }
        return contexts;
    }

    protected <T> T executeJob( Job<T> job, int slave ) throws Exception
    {
        return job.execute( haDbs.get( slave ) );
    }
    
    protected <T> T executeJobOnMaster( Job<T> job ) throws Exception
    {
        return job.execute( master.getGraphDb() );
    }
    
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
    protected Job<Void> getMasterShutdownDispatcher()
    {
        return new Job<Void>()
        {
            public Void execute( GraphDatabaseService db )
            {
                master.getGraphDb().shutdown();
                return null;
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
