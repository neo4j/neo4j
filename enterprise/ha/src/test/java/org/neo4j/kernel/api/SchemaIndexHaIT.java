/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.function.Predicate;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.impl.index.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.register.Register.DoubleLong;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.ha.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.asUniqueSet;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterAvailable;

public class SchemaIndexHaIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() );

    @Test
    public void creatingIndexOnMasterShouldHaveSlavesBuildItAsWell() throws Throwable
    {
        // GIVEN
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        Map<Object, Node> data = createSomeData( master );

        // WHEN
        IndexDefinition index = createIndex( master );
        cluster.sync();

        // THEN
        awaitIndexOnline( index, cluster, data );
    }

    @Test
    public void creatingIndexOnSlaveIsNotAllowed() throws Throwable
    {
        // GIVEN
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        // WHEN
        try
        {
            createIndex( slave );
            fail( "should have thrown exception" );
        }
        catch ( ConstraintViolationException e )
        {
            // expected
        }
    }

    @Test
    public void indexPopulationJobsShouldContinueThroughRoleSwitch() throws Throwable
    {
        // GIVEN a cluster of 3
        ControlledGraphDatabaseFactory dbFactory = new ControlledGraphDatabaseFactory();
        ManagedCluster cluster = clusterRule.withDbFactory( dbFactory ).startCluster();
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
        try ( Transaction tx = newMaster.beginTx() )
        {
            IndexDefinition index = single( newMaster.schema().getIndexes() );
            awaitIndexOnline( index, newMaster, data );
            tx.success();
        }
        // FINALLY: let all db's finish
        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            dbFactory.triggerFinish( db );
        }
    }

    @Test
    public void populatingSchemaIndicesOnMasterShouldBeBroughtOnlineOnSlavesAfterStoreCopy() throws Throwable
    {
        /*
        The master has an index that is currently populating.
        Then a slave comes online and contacts the master to get copies of the store files.
        Because the index is still populating, it won't be copied. Instead the slave will build its own.
        We want to observe that the slave builds an index that eventually comes online.
         */

        // GIVEN
        ControlledGraphDatabaseFactory dbFactory = new ControlledGraphDatabaseFactory( IS_MASTER );

        ManagedCluster cluster = clusterRule.withDbFactory( dbFactory ).startCluster( );

        try
        {
            cluster.await( allSeesAllAsAvailable() );

            HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

            // A slave is offline, and has no store files
            ClusterManager.RepairKit slaveDown = bringSlaveOfflineAndRemoveStoreFiles( cluster, slave );

            // And I create an index on the master, and wait for population to start
            HighlyAvailableGraphDatabase master = cluster.getMaster();
            Map<Object, Node> data = createSomeData(master);
            createIndex( master );
            dbFactory.awaitPopulationStarted( master );


            // WHEN the slave comes online before population has finished on the master
            slave = slaveDown.repair();
            cluster.await( allSeesAllAsAvailable(), 180 );
            cluster.sync();


            // THEN, population should finish successfully on both master and slave
            dbFactory.triggerFinish( master );

            // Check master
            IndexDefinition index;
            try ( Transaction tx = master.beginTx())
            {
                index = single( master.schema().getIndexes() );
                awaitIndexOnline( index, master, data );
                tx.success();
            }

            // Check slave
            try ( Transaction tx = slave.beginTx() )
            {
                awaitIndexOnline( index, slave, data );
                tx.success();
            }
        }
        finally
        {
            for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
            {
                dbFactory.triggerFinish( db );
            }
        }
    }

    @Test
    public void onlineSchemaIndicesOnMasterShouldBeBroughtOnlineOnSlavesAfterStoreCopy() throws Throwable
    {
        /*
        The master has an index that is online.
        Then a slave comes online and contacts the master to get copies of the store files.
        Because the index is online, it should be copied, and the slave should successfully bring the index online.
         */

        // GIVEN
        ControlledGraphDatabaseFactory dbFactory = new ControlledGraphDatabaseFactory();

        ManagedCluster cluster = clusterRule.withDbFactory( dbFactory ).startCluster();
        cluster.await( allSeesAllAsAvailable(), 120 );

        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        // All slaves in the cluster, except the one I care about, proceed as normal
        proceedAsNormalWithIndexPopulationOnAllSlavesExcept( dbFactory, cluster, slave );

        // A slave is offline, and has no store files
        ClusterManager.RepairKit slaveDown = bringSlaveOfflineAndRemoveStoreFiles( cluster, slave );

        // And I create an index on the master, and wait for population to start
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        Map<Object, Node> data = createSomeData(master);
        createIndex( master );
        dbFactory.awaitPopulationStarted( master );

        // And the population finishes
        dbFactory.triggerFinish( master );
        IndexDefinition index;
        try ( Transaction tx = master.beginTx())
        {
            index = single( master.schema().getIndexes() );
            awaitIndexOnline( index, master, data );
            tx.success();
        }


        // WHEN the slave comes online after population has finished on the master
        slave = slaveDown.repair();
        cluster.await( allSeesAllAsAvailable() );
        cluster.sync();


        // THEN the index should work on the slave
        dbFactory.triggerFinish( slave );
        try ( Transaction tx = slave.beginTx() )
        {
            awaitIndexOnline( index, slave, data );
            tx.success();
        }
    }

    private void proceedAsNormalWithIndexPopulationOnAllSlavesExcept( ControlledGraphDatabaseFactory dbFactory,
                                                                      ManagedCluster cluster,
                                                                      HighlyAvailableGraphDatabase slaveToIgnore )
    {
        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            if( db != slaveToIgnore && db.getInstanceState() == HighAvailabilityMemberState.SLAVE )
            {
                dbFactory.triggerFinish( db );
            }
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private ClusterManager.RepairKit bringSlaveOfflineAndRemoveStoreFiles( ManagedCluster cluster, HighlyAvailableGraphDatabase slave ) throws IOException
    {
        ClusterManager.RepairKit slaveDown = cluster.shutdown(slave);

        File storeDir = new File( slave.getStoreDir() );
        deleteRecursively( storeDir );
        storeDir.mkdir();
        return slaveDown;
    }

    public static final Predicate<GraphDatabaseService> IS_MASTER = new Predicate<GraphDatabaseService>()
    {
        @Override
        public boolean test( GraphDatabaseService item )
        {
            return item instanceof HighlyAvailableGraphDatabase && ((HighlyAvailableGraphDatabase) item).isMaster();
        }
    };

    private final String key = "key";
    private final Label label = label( "label" );

    private Map<Object, Node> createSomeData( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Map<Object, Node> result = new HashMap<>();
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
    }

    private IndexDefinition createIndex( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = db.schema().indexFor( label ).on( key ).create();
            tx.success();
            return index;
        }
    }

    private static void awaitIndexOnline( IndexDefinition index, ManagedCluster cluster,
            Map<Object, Node> expectedDdata ) throws InterruptedException
    {
        for ( GraphDatabaseService db : cluster.getAllMembers() )
        {
            awaitIndexOnline( index, db, expectedDdata );
        }
    }

    private static IndexDefinition reHomedIndexDefinition( GraphDatabaseService db, IndexDefinition definition )
    {
        for ( IndexDefinition candidate : db.schema().getIndexes() )
        {
            if ( candidate.equals( definition ) )
            {
                return candidate;
            }
        }
        throw new NoSuchElementException( "New database doesn't have requested index" );
    }

    private static void awaitIndexOnline( IndexDefinition requestedIndex, GraphDatabaseService db,
            Map<Object, Node> expectedData ) throws InterruptedException
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = reHomedIndexDefinition( db, requestedIndex );

            long timeout = System.currentTimeMillis() + SECONDS.toMillis( 120 );
            while( !indexOnline( index, db ) )
            {
                Thread.sleep( 1 );
                if ( System.currentTimeMillis() > timeout )
                {
                    fail( "Expected index to come online within a reasonable time." );
                }
            }

            assertIndexContents( index, db, expectedData );
            tx.success();
        }
    }

    private static void assertIndexContents( IndexDefinition index, GraphDatabaseService db,
            Map<Object, Node> expectedData )
    {
        for ( Map.Entry<Object, Node> entry : expectedData.entrySet() )
        {
            assertEquals( asSet( entry.getValue() ),
                    asUniqueSet( db.findNodes( index.getLabel(), single( index.getPropertyKeys() ), entry.getKey() ) ) );
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

    private static class ControlledIndexPopulator implements IndexPopulator
    {
        private final DoubleLatch latch;
        private final IndexPopulator delegate;

        public ControlledIndexPopulator( IndexPopulator delegate, DoubleLatch latch )
        {
            this.delegate = delegate;
            this.latch = latch;
        }

        @Override
        public void create() throws IOException
        {
            delegate.create();
        }

        @Override
        public void drop() throws IOException
        {
            delegate.drop();
        }

        @Override
        public void add( long nodeId, Object propertyValue )
                throws IndexEntryConflictException, IOException, IndexCapacityExceededException
        {
            delegate.add(nodeId, propertyValue);
            latch.startAndAwaitFinish();
        }

        @Override
        public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
                throws IndexEntryConflictException, IOException
        {
            delegate.verifyDeferredConstraints( propertyAccessor );
        }

        @Override
        public IndexUpdater newPopulatingUpdater( PropertyAccessor propertyAccessor ) throws IOException
        {
            return delegate.newPopulatingUpdater( propertyAccessor );
        }

        @Override
        public void close( boolean populationCompletedSuccessfully ) throws IOException, IndexCapacityExceededException
        {
            delegate.close(populationCompletedSuccessfully);
            assertTrue( "Expected population to succeed :(", populationCompletedSuccessfully );
            latch.finish();
        }

        @Override
        public void markAsFailed( String failure ) throws IOException
        {
            delegate.markAsFailed( failure );
        }

        @Override
        public long sampleResult( DoubleLong.Out result )
        {
            return delegate.sampleResult( result );
        }
    }

    public static final SchemaIndexProvider.Descriptor CONTROLLED_PROVIDER_DESCRIPTOR =
            new SchemaIndexProvider.Descriptor( "controlled", "1.0" );


    private static class ControlledSchemaIndexProvider extends SchemaIndexProvider
    {
        private final SchemaIndexProvider delegate;
        private final DoubleLatch latch = new DoubleLatch();

        public ControlledSchemaIndexProvider(SchemaIndexProvider delegate)
        {
            super( CONTROLLED_PROVIDER_DESCRIPTOR, 100 /*we want it to always win*/ );
            this.delegate = delegate;
        }

        @Override
        public IndexPopulator getPopulator( long indexId, IndexDescriptor descriptor, IndexConfiguration config,
                                            IndexSamplingConfig samplingConfig )
        {
            IndexPopulator populator = delegate.getPopulator( indexId, descriptor, config, samplingConfig );
            return new ControlledIndexPopulator( populator, latch );
        }

        @Override
        public IndexAccessor getOnlineAccessor( long indexId, IndexConfiguration config,
                                                IndexSamplingConfig samplingConfig  ) throws IOException
        {
            return delegate.getOnlineAccessor(indexId, config, samplingConfig );
        }

        @Override
        public InternalIndexState getInitialState( long indexId )
        {
            return delegate.getInitialState(indexId);
        }

        @Override
        public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
        {
            return delegate.storeMigrationParticipant( fs, pageCache );
        }

        @Override
        public String getPopulationFailure( long indexId ) throws IllegalStateException
        {
            return delegate.getPopulationFailure( indexId );
        }
    }

    interface IndexProviderDependencies
    {
        GraphDatabaseService db();
        Config config();
    }

    public static class ControllingIndexProviderFactory extends KernelExtensionFactory<IndexProviderDependencies>
    {
        private final Map<GraphDatabaseService, SchemaIndexProvider> perDbIndexProvider;
        private final Predicate<GraphDatabaseService> injectLatchPredicate;

        public ControllingIndexProviderFactory( Map<GraphDatabaseService, SchemaIndexProvider> perDbIndexProvider,
                                                Predicate<GraphDatabaseService> injectLatchPredicate)
        {
            super( CONTROLLED_PROVIDER_DESCRIPTOR.getKey() );
            this.perDbIndexProvider = perDbIndexProvider;
            this.injectLatchPredicate = injectLatchPredicate;
        }

        @Override
        public Lifecycle newInstance( KernelContext context, SchemaIndexHaIT.IndexProviderDependencies deps ) throws Throwable
        {
            if(injectLatchPredicate.test( deps.db() ))
            {
                ControlledSchemaIndexProvider provider = new ControlledSchemaIndexProvider(
                        new LuceneSchemaIndexProvider( new DefaultFileSystemAbstraction(),
                                DirectoryFactory.PERSISTENT, context.storeDir(), NullLogProvider.getInstance(),
                                deps.config(), context.operationalMode() ) );
                perDbIndexProvider.put( deps.db(), provider );
                return provider;
            }
            else
            {
                return new LuceneSchemaIndexProvider( new DefaultFileSystemAbstraction(),
                        DirectoryFactory.PERSISTENT, context.storeDir(), NullLogProvider.getInstance(),
                        deps.config(), context.operationalMode() );
            }
        }
    }

    private static class ControlledGraphDatabaseFactory extends TestHighlyAvailableGraphDatabaseFactory
    {
        final Map<GraphDatabaseService,SchemaIndexProvider> perDbIndexProvider = new ConcurrentHashMap<>();
        private final KernelExtensionFactory<?> factory;

        public ControlledGraphDatabaseFactory()
        {
            factory = new ControllingIndexProviderFactory(perDbIndexProvider, Predicates.<GraphDatabaseService>alwaysTrue());
        }

        private ControlledGraphDatabaseFactory( Predicate<GraphDatabaseService> dbsToControlIndexingOn )
        {
            factory = new ControllingIndexProviderFactory(perDbIndexProvider, dbsToControlIndexingOn);
        }

        @Override
        public GraphDatabaseBuilder newHighlyAvailableDatabaseBuilder(String path)
        {
            getCurrentState().addKernelExtensions( Arrays.<KernelExtensionFactory<?>>asList( factory ) );
            return super.newHighlyAvailableDatabaseBuilder( path );
        }

        void awaitPopulationStarted( GraphDatabaseService db )
        {
            ControlledSchemaIndexProvider provider = (ControlledSchemaIndexProvider) perDbIndexProvider.get( db );
            if(provider != null ) provider.latch.awaitStart();
        }

        void triggerFinish( GraphDatabaseService db )
        {
            ControlledSchemaIndexProvider provider = (ControlledSchemaIndexProvider) perDbIndexProvider.get( db );
            if(provider != null ) provider.latch.finish();
        }
    }
 }
