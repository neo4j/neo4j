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
package org.neo4j.kernel.api;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.cluster.ClusterSettings.default_timeout;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.asUniqueSet;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.ha.HaSettings.tx_push_factor;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceSchemaIndexProviderFactory;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;
import static org.neo4j.test.ha.ClusterManager.masterAvailable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.impl.api.index.InMemoryIndexProvider;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterManager.ManagedCluster;

public class SchemaIndexHaIT
{
    @Test
    public void creatingIndexOnMasterShouldHaveSlavesBuildItAsWell() throws Throwable
    {
        // GIVEN
        ManagedCluster cluster = startCluster( clusterOfSize( 3 ) );
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        Map<Object, Node> data = createSomeData( master );

        // WHEN
        IndexDefinition index = createIndex( master );
        cluster.sync();

        // THEN
        awaitIndexOnline( index, cluster, data );
    }
    
    @Test
    public void creatingIndexOnSlaveShouldHaveOtherSlavesAndMasterBuiltItAsWell() throws Throwable
    {
        // GIVEN
        ManagedCluster cluster = startCluster( clusterOfSize( 3 ) );
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        Map<Object, Node> data = createSomeData( master );
        cluster.sync();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        // WHEN
        IndexDefinition index = createIndex( slave );
        cluster.sync();

        // THEN
        awaitIndexOnline( index, cluster, data );
    }
    
    @Test
    public void indexPopulationJobsShouldContinueThroughRoleSwitch() throws Throwable
    {
        // GIVEN a cluster of 3
        ControlledGraphDatabaseFactory dbFactory = new ControlledGraphDatabaseFactory();
        ManagedCluster cluster = startCluster( clusterOfSize( 3 ), dbFactory );
        HighlyAvailableGraphDatabase firstMaster = cluster.getMaster();

        // where the master gets some data created as well as an index
        Map<Object, Node> data = createSomeData( firstMaster );
        createIndex( firstMaster );
        //dbFactory.awaitPopulationStarted( firstMaster );
        dbFactory.triggerFinish( firstMaster );

        // Pick a slave, pull the data and the index
        HighlyAvailableGraphDatabase aSlave = cluster.getAnySlave();
        aSlave.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();

        // and await the index population to start. It will actually block as long as we want it to
        dbFactory.awaitPopulationStarted( aSlave );

        // WHEN we shut down the master
        cluster.shutdown( firstMaster );
        
        dbFactory.triggerFinish( aSlave );
        cluster.await( masterAvailable( firstMaster ) );
        // get the new master, which should be the slave we pulled from above
        HighlyAvailableGraphDatabase newMaster = cluster.getMaster();
        
        // THEN
        assertEquals( "Unexpected new master", aSlave, newMaster );
        awaitIndexOnline( single( newMaster.schema().getIndexes() ), newMaster, data );
    }

    private final File storeDir = TargetDirectory.forTest( getClass() ).graphDbDir( true );
    private ClusterManager clusterManager;
    private final String key = "key";
    private final Label label = label( "label" );
    
    private ManagedCluster startCluster( ClusterManager.Provider provider ) throws Throwable
    {
        return startCluster( provider, new HighlyAvailableGraphDatabaseFactory() );
    }

    private ManagedCluster startCluster( ClusterManager.Provider provider,
            HighlyAvailableGraphDatabaseFactory dbFactory ) throws Throwable
    {
        clusterManager = new ClusterManager( provider, storeDir, stringMap(
                default_timeout.name(), "1s", tx_push_factor.name(), "0" ),
                new HashMap<Integer, Map<String,String>>(), dbFactory );
        clusterManager.start();
        ManagedCluster cluster = clusterManager.getDefaultCluster();
        cluster.await( masterAvailable() );
        return cluster;
    }
    
    @After
    public void after() throws Throwable
    {
        if ( clusterManager != null )
            clusterManager.stop();
    }

    private Map<Object, Node> createSomeData( GraphDatabaseService db )
    {
        Map<Object, Node> result = new HashMap<Object, Node>();
        Transaction tx = db.beginTx();
        try
        {
            for ( int i = 0; i < 10; i++ )
            {
                Node node = db.createNode( label );
                Object propertyValue = i;
                node.setProperty( key, propertyValue );
                result.put( propertyValue, node );
            }
            tx.success();
            return result;
        }
        finally
        {
            tx.finish();
        }
    }

    private IndexDefinition createIndex( GraphDatabaseService db )
    {
        Transaction tx = db.beginTx();
        IndexDefinition index = db.schema().indexCreator( label ).on( key ).create();
        tx.success();
        tx.finish();
        return index;
    }

    private static void awaitIndexOnline( IndexDefinition index, ManagedCluster cluster,
            Map<Object, Node> expectedDdata ) throws InterruptedException
    {
        for ( GraphDatabaseService db : cluster.getAllMembers() )
            awaitIndexOnline( index, db, expectedDdata );
    }
    
    private static void awaitIndexOnline( IndexDefinition index, GraphDatabaseService db,
            Map<Object, Node> expectedDdata ) throws InterruptedException
    {
        long timeout = System.currentTimeMillis() + SECONDS.toMillis( 60 );
        while( !indexOnline( index, db ) )
        {
            Thread.sleep( 1 );
            if ( System.currentTimeMillis() > timeout )
            {
                fail( "Expected index to come online within a reasonable time." );
            }
        }
        
        assertIndexContents( index, db, expectedDdata );
    }

    private static void assertIndexContents( IndexDefinition index, GraphDatabaseService db,
            Map<Object, Node> expectedDdata )
    {
        for ( Map.Entry<Object, Node> entry : expectedDdata.entrySet() )
        {
            assertEquals( asSet( entry.getValue() ),
                    asUniqueSet( db.findNodesByLabelAndProperty( index.getLabel(),
                            single( index.getPropertyKeys() ), entry.getKey() ) ) );
        }
    }

    private static boolean indexOnline( IndexDefinition index, GraphDatabaseService db )
    {
        try
        {
            return db.schema().getIndexState( index ) == IndexState.ONLINE;
        }
        catch ( NotFoundException e )
        {
            return false;
        }
    }
    
    private static class ControlledIndexPopulator extends IndexPopulator.Adapter
    {
        private final DoubleLatch latch;
        private final IndexPopulator inMemoryDelegate;

        public ControlledIndexPopulator( IndexPopulator inMemoryDelegate, DoubleLatch latch )
        {
            this.inMemoryDelegate = inMemoryDelegate;
            this.latch = latch;
        }

        @Override
        public void add( long nodeId, Object propertyValue ) throws IndexEntryConflictException, IOException
        {
            inMemoryDelegate.add( nodeId, propertyValue );
            latch.startAndAwaitFinish();
        }
        
        @Override
        public void close( boolean populationCompletedSuccessfully ) throws IOException
        {
            inMemoryDelegate.close( populationCompletedSuccessfully );
            assertTrue( populationCompletedSuccessfully );
            latch.finish();
        }
    }

    public static final SchemaIndexProvider.Descriptor CONTROLLED_PROVIDER_DESCRIPTOR =
            new SchemaIndexProvider.Descriptor( "controlled", "1.0" );


    private static class ControlledSchemaIndexProvider extends SchemaIndexProvider
    {
        private final SchemaIndexProvider inMemoryDelegate = new InMemoryIndexProvider();
        private final DoubleLatch latch = new DoubleLatch();
        
        public ControlledSchemaIndexProvider()
        {
            super( CONTROLLED_PROVIDER_DESCRIPTOR, 10 );
        }
        
        @Override
        public IndexPopulator getPopulator( long indexId, IndexConfiguration config )
        {
            return new ControlledIndexPopulator( inMemoryDelegate.getPopulator( indexId, config ), latch );
        }

        @Override
        public IndexAccessor getOnlineAccessor( long indexId, IndexConfiguration config )
        {
            return inMemoryDelegate.getOnlineAccessor( indexId, config );
        }

        @Override
        public InternalIndexState getInitialState( long indexId )
        {
            return inMemoryDelegate.getInitialState( indexId );
        }
    }

    private static class ControlledGraphDatabaseFactory extends HighlyAvailableGraphDatabaseFactory
    {
        final Map<GraphDatabaseService,ControlledSchemaIndexProvider> perDbIndexProvider =
                new ConcurrentHashMap<GraphDatabaseService,ControlledSchemaIndexProvider>();
        
        @Override
        public GraphDatabaseBuilder newHighlyAvailableDatabaseBuilder( String path )
        {
            ControlledSchemaIndexProvider provider = new ControlledSchemaIndexProvider();
            KernelExtensionFactory<?> factory = singleInstanceSchemaIndexProviderFactory( "controlled", provider );
            getCurrentState().setKernelExtensions( Arrays.<KernelExtensionFactory<?>>asList( factory ) );
            return dbReferenceCapturingBuilder( perDbIndexProvider, provider,
                    super.newHighlyAvailableDatabaseBuilder( path ) );
        }
        
        void awaitPopulationStarted( GraphDatabaseService db )
        {
            DoubleLatch latch = perDbIndexProvider.get( db ).latch;
            latch.awaitStart();
        }

        void triggerFinish( GraphDatabaseService db )
        {
            ControlledSchemaIndexProvider provider = perDbIndexProvider.get( db );
            provider.latch.finish();
        }
    }
    
    protected static GraphDatabaseBuilder dbReferenceCapturingBuilder(
            final Map<GraphDatabaseService, ControlledSchemaIndexProvider> perDbIndexProvider,
            final ControlledSchemaIndexProvider provider, GraphDatabaseBuilder actual )
    {
        return new GraphDatabaseBuilder.Delegator( actual )
        {
            @Override
            public GraphDatabaseService newGraphDatabase()
            {
                GraphDatabaseService db = super.newGraphDatabase();
                perDbIndexProvider.put( db, provider );
                return db;
            }
        };
    }
}
