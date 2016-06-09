package org.neo4j.kernel.ha.cluster;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.test.Race;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.fail;

public class SlavesCanReadCorruptedData
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() )
            .withSharedSetting( HaSettings.pull_interval, "0" )
            .withSharedSetting( HaSettings.tx_push_factor, "0" );

    @Test
    public void slavesCanApparentlyReadCorruptedData() throws Throwable
    {
        // given
        final ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        int stringLength = 1000;

        final String stringA = buildString( stringLength, 'a' );
        final String stringB = buildString( stringLength, 'b' );

        final String key = "key";
        final long nodeId = createNodeAndSetProperty( master, key, stringA );
        cluster.sync();
        // Slaves and master now has node with id = nodeId that has long string property
        removeProperty( master, nodeId, key );
        forceMaintenance( master );
        setProperty( master, nodeId, key, stringB );

        final HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        Race race = new Race();
        int nbrOfReaders = 100;
        final AtomicBoolean end = new AtomicBoolean( false );
        for ( int i = 0; i < nbrOfReaders; i++ )
        {
            race.addContestant( new Runnable()
            {
                @Override
                public void run()
                {
                    while ( !end.get() )
                    {
                        try ( Transaction tx = slave.beginTx() )
                        {
                            Node node = slave.getNodeById( nodeId );
                            Object property = node.getProperty( key, null );

                            assertPropertyValue( property, stringA, stringB );

                            tx.success();
                        }
                    }
                }
            } );
        }

        race.addContestant( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    slave.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
                finally
                {
                    end.set( true );
                }
            }
        } );

        race.go();
    }

    String buildString( int stringLength, char c )
    {
        StringBuilder sba = new StringBuilder();
        for ( int i = 0; i < stringLength; i++ )
        {
            sba.append( c );
        }
        return sba.toString();
    }

    private long createNodeAndSetProperty( HighlyAvailableGraphDatabase master, String propertyKey,
            String propertyValue )
    {
        long ida;
        try ( Transaction tx = master.beginTx() )
        {
            Node node = master.createNode();
            ida = node.getId();
            node.setProperty( propertyKey, propertyValue );

            tx.success();
        }
        return ida;
    }

    private void setProperty( HighlyAvailableGraphDatabase master, long nodeId, String propertyKey,
            String propertyValue )
    {
        try ( Transaction tx = master.beginTx() )
        {
            Node node = master.getNodeById( nodeId );
            node.setProperty( propertyKey, propertyValue );

            tx.success();
        }
    }

    private void forceMaintenance( HighlyAvailableGraphDatabase master )
    {
        NeoStoreDataSource dataSource =
                master.getDependencyResolver().resolveDependency( DataSourceManager.class ).getDataSource();
        dataSource.maintenance();
    }

    private void removeProperty( HighlyAvailableGraphDatabase master, long nodeId, String propertyKey )
    {
        try ( Transaction tx = master.beginTx() )
        {
            Node node = master.getNodeById( nodeId );
            node.removeProperty( propertyKey );

            tx.success();
        }
    }

    private void assertPropertyValue( Object property, String... candidates )
    {
        if ( property == null )
        {
            return;
        }
        for ( String candidate : candidates )
        {
            if ( property.equals( candidate ) )
            {
                return;
            }
        }
        fail( "property value was " + property );
    }
}
