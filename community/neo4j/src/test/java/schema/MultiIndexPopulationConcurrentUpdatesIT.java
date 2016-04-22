/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package schema;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.MultipleIndexPopulator;
import org.neo4j.kernel.impl.api.index.NodePropertyUpdates;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.state.DefaultSchemaIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.storeview.AdaptableIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.LabelScanViewNodeStoreScan;
import org.neo4j.kernel.impl.transaction.state.storeview.NodeStoreScan;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

//[NodePropertyUpdate[0, prop:0 add:Sweden, labelsBefore:[], labelsAfter:[0]]]
//[NodePropertyUpdate[1, prop:0 add:USA, labelsBefore:[], labelsAfter:[0]]]
//[NodePropertyUpdate[2, prop:0 add:red, labelsBefore:[], labelsAfter:[1]]]
//[NodePropertyUpdate[3, prop:0 add:green, labelsBefore:[], labelsAfter:[0]]]
//[NodePropertyUpdate[4, prop:0 add:Volvo, labelsBefore:[], labelsAfter:[2]]]
//[NodePropertyUpdate[5, prop:0 add:Ford, labelsBefore:[], labelsAfter:[2]]]
//TODO: check count store counts
public class MultiIndexPopulationConcurrentUpdatesIT
{
    private static final String NAME_PROPERTY = "name";
    private static final String COUNTRY_LABEL = "country";
    private static final String COLOR_LABEL = "color";
    private static final String CAR_LABEL = "car";

    @Rule
    public EmbeddedDatabaseRule embeddedDatabase = new EmbeddedDatabaseRule();

    private IndexingService indexService;
    private int propertyId;
    private Map<String,Integer> labelsNameIdMap;

    @After
    public void tearDown() throws Throwable
    {
        if (indexService != null)
        {
            indexService.shutdown();
        }
    }

    @Before
    public void setUp()
    {
        prepareDb();

        labelsNameIdMap = getLabelsNameIdMap();
        propertyId = getPropertyKeyId();
    }

    @Test
    public void applyConcurrentDeletesToPopulatedIndex() throws Throwable
    {
        Map<Long,NodePropertyUpdate> updateMap = new HashMap<>();
        updateMap.put( 2L, NodePropertyUpdate.remove( 0, propertyId, "Sweden", new long[]{labelsNameIdMap.get( COUNTRY_LABEL )} ) );
        updateMap.put( 4L, NodePropertyUpdate.remove( 3, propertyId, "green", new long[]{labelsNameIdMap.get( COLOR_LABEL )} ) );

        launchCustomIndexPopulation( labelsNameIdMap, propertyId, updateMap );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );

        try ( Transaction ignored = embeddedDatabase.beginTx() )
        {
            Integer countryLabelId = labelsNameIdMap.get( COUNTRY_LABEL );
            Integer colorLabelId = labelsNameIdMap.get( COLOR_LABEL );
            try ( IndexReader indexReader = getIndexReader( propertyId, countryLabelId ) )
            {
                assertEquals("Should be removed by concurrent remove.",
                        0, indexReader.countIndexedNodes( 0, "Sweden" ) );
            }

            try ( IndexReader indexReader = getIndexReader( propertyId, colorLabelId ) )
            {
                assertEquals("Should be removed by concurrent remove.",
                        0, indexReader.countIndexedNodes( 3, "green" ) );
            }
        }
    }

    @Test
    public void applyConcurrentAddsToPopulatedIndex() throws Throwable
    {
        Map<Long,NodePropertyUpdate> updateMap = new HashMap<>();
        updateMap.put( 3L, NodePropertyUpdate.add( 6, propertyId, "Denmark", new long[]{labelsNameIdMap.get( COUNTRY_LABEL )} ) );
        updateMap.put( 4L, NodePropertyUpdate.add( 7, propertyId, "BMW", new long[]{labelsNameIdMap.get( CAR_LABEL )} ) );

        launchCustomIndexPopulation( labelsNameIdMap, propertyId, updateMap );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );

        try ( Transaction ignored = embeddedDatabase.beginTx() )
        {
            Integer countryLabelId = labelsNameIdMap.get( COUNTRY_LABEL );
            Integer carLabelId = labelsNameIdMap.get( CAR_LABEL );
            try ( IndexReader indexReader = getIndexReader( propertyId, countryLabelId ) )
            {
                assertEquals("Should be added by concurrent add.", 1, indexReader.countIndexedNodes( 6, "Denmark" ) );
            }

            try ( IndexReader indexReader = getIndexReader( propertyId, carLabelId ) )
            {
                assertEquals("Should be added by concurrent add.", 1, indexReader.countIndexedNodes( 7, "BMW" ) );
            }
        }
    }

    @Test
    public void applyConcurrentChangesToPopulatedIndex() throws Exception
    {
        Map<Long,NodePropertyUpdate> updateMap = new HashMap<>();
        updateMap.put( 2L, NodePropertyUpdate.change( 3, propertyId, "green", new long[]{labelsNameIdMap.get( COLOR_LABEL )},
                "pink", new long[]{labelsNameIdMap.get( COLOR_LABEL )} ) );
        updateMap.put( 3L, NodePropertyUpdate.change( 5, propertyId, "Ford", new long[]{labelsNameIdMap.get( CAR_LABEL )},
                "SAAB", new long[]{labelsNameIdMap.get( CAR_LABEL )} ) );

        launchCustomIndexPopulation( labelsNameIdMap, propertyId, updateMap );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );

        try ( Transaction ignored = embeddedDatabase.beginTx() )
        {
            Integer colorLabelId = labelsNameIdMap.get( COLOR_LABEL );
            Integer carLabelId = labelsNameIdMap.get( CAR_LABEL );
            try ( IndexReader indexReader = getIndexReader( propertyId, colorLabelId ) )
            {
                assertEquals("Should be deleted by concurrent change.", 0, indexReader.countIndexedNodes( 3, "green" ) );
            }
            try ( IndexReader indexReader = getIndexReader( propertyId, colorLabelId ) )
            {
                assertEquals("Should be updated by concurrent change.", 1, indexReader.countIndexedNodes( 3, "pink" ) );
            }

            try ( IndexReader indexReader = getIndexReader( propertyId, carLabelId ) )
            {
                assertEquals("Should be added by concurrent change.", 1, indexReader.countIndexedNodes( 5, "SAAB" ) );
            }
        }
    }

    private IndexReader getIndexReader( int propertyId, Integer countryLabelId ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( new IndexDescriptor( countryLabelId, propertyId ) ).newReader();
    }

    private void launchCustomIndexPopulation( Map<String,Integer> labelNameIdMap, int propertyId, Map<Long,NodePropertyUpdate> updateMap ) throws Exception
    {
        NeoStores neoStores = getNeoStores();
        LabelScanStore labelScanStore = getLabelScanStore();
        ThreadToStatementContextBridge transactionStatementContextBridge = getTransactionStatementContextBridge();

        try ( Transaction transaction = embeddedDatabase.beginTx() )
        {
            Statement statement = transactionStatementContextBridge.get();


            AdaptableIndexStoreView storeView =
                    new AdaptableIndexStoreViewWrapper( labelScanStore, LockService.NO_LOCK_SERVICE, neoStores,
                            updateMap, propertyId );

            SchemaIndexProviderMap providerMap = new DefaultSchemaIndexProviderMap( getSchemaIndexProvider() );
            JobScheduler scheduler = getJobScheduler();
            StatementTokenNameLookup tokenNameLookup = new StatementTokenNameLookup( statement.readOperations() );

            indexService = IndexingServiceFactory.createIndexingService( Config.empty(), scheduler,
                    providerMap, storeView, tokenNameLookup, getIndexRules( neoStores ),
                    NullLogProvider.getInstance(), IndexingService.NO_MONITOR, () -> {} );
            indexService.start();


            IndexRule[] rules = createIndexRules( labelNameIdMap, propertyId );

            indexService.createIndexes( rules );
            transaction.success();
        }
    }

    private void waitAndActivateIndexes( Map<String,Integer> labelsIds, int propertyId )
            throws IndexNotFoundKernelException, IndexPopulationFailedKernelException, InterruptedException,
            IndexActivationFailedKernelException
    {
        try ( Transaction ignored = embeddedDatabase.beginTx() )
        {
            for ( int labelId : labelsIds.values() )
            {
                waitIndexOnline( indexService, propertyId, labelId );
            }
        }
    }

    private int getPropertyKeyId()
    {
        try ( Transaction ignored = embeddedDatabase.beginTx() )
        {
            return getPropertyIdByName( NAME_PROPERTY );
        }
    }

    private Map<String,Integer> getLabelsNameIdMap()
    {
        try ( Transaction ignored = embeddedDatabase.beginTx() )
        {
            return getLabelIdsByName( COUNTRY_LABEL, COLOR_LABEL, CAR_LABEL );
        }
    }

    private void waitIndexOnline( IndexingService indexService, int propertyId, int labelId )
            throws IndexNotFoundKernelException, IndexPopulationFailedKernelException, InterruptedException,
            IndexActivationFailedKernelException
    {
        IndexProxy indexProxy = indexService.getIndexProxy( new IndexDescriptor( labelId, propertyId ) );
        indexProxy.awaitStoreScanCompleted();
        indexProxy.activate();
    }

    private IndexRule[] createIndexRules( Map<String,Integer> labelNameIdMap, int propertyId )
    {
        return labelNameIdMap.values().stream()
                .map( index -> new IndexRule( index, index, propertyId,
                        new SchemaIndexProvider.Descriptor( "lucene", "version" ), null ) )
                .toArray( IndexRule[]::new );
    }

    private List<IndexRule> getIndexRules( NeoStores neoStores )
    {
        return Iterators.asList( new SchemaStorage( neoStores.getSchemaStore() ).allIndexRules() );
    }

    private Map<String, Integer> getLabelIdsByName( String... names )
    {
        ThreadToStatementContextBridge transactionStatementContextBridge = getTransactionStatementContextBridge();
        ReadOperations readOperations = transactionStatementContextBridge.get().readOperations();
        Map<String, Integer> labelNameIdMap = new HashMap<>();
        for ( String name : names )
        {
            labelNameIdMap.put( name, readOperations.labelGetForName( name ) );
        }
        return labelNameIdMap;
    }

    private int getPropertyIdByName( String name )
    {
        ThreadToStatementContextBridge transactionStatementContextBridge = getTransactionStatementContextBridge();
        ReadOperations readOperations = transactionStatementContextBridge.get().readOperations();
        return readOperations.propertyKeyGetForName( name );
    }

    private void prepareDb()
    {
        Label countryLabel = Label.label( COUNTRY_LABEL );
        Label color = Label.label( COLOR_LABEL );
        Label car = Label.label( CAR_LABEL );
        try ( Transaction transaction = embeddedDatabase.beginTx() )
        {
            createNamedLabeledNode( countryLabel, "Sweden" );
            createNamedLabeledNode( countryLabel, "USA" );

            createNamedLabeledNode( color, "red" );
            createNamedLabeledNode( color, "green" );

            createNamedLabeledNode( car, "Volvo" );
            createNamedLabeledNode( car, "Ford" );

            for ( int i = 0; i < 25; i++ )
            {
                embeddedDatabase.createNode();
            }

            transaction.success();
        }
    }

    private void createNamedLabeledNode( Label label, String name )
    {
        Node node = embeddedDatabase.createNode( label );
        node.setProperty( NAME_PROPERTY, name );
    }

    private LabelScanStore getLabelScanStore()
    {
        return embeddedDatabase.resolveDependency( LabelScanStoreProvider.class ).getLabelScanStore();
    }

    private NeoStores getNeoStores()
    {
        RecordStorageEngine recordStorageEngine = embeddedDatabase.resolveDependency( RecordStorageEngine.class );
        return recordStorageEngine.testAccessNeoStores();
    }

    private ThreadToStatementContextBridge getTransactionStatementContextBridge()
    {
        return embeddedDatabase.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
    }

    private SchemaIndexProvider getSchemaIndexProvider()
    {
        return embeddedDatabase.getDependencyResolver().resolveDependency( SchemaIndexProvider.class );
    }

    private JobScheduler getJobScheduler()
    {
        return embeddedDatabase.getDependencyResolver().resolveDependency( JobScheduler.class );
    }

    private class AdaptableIndexStoreViewWrapper extends AdaptableIndexStoreView
    {
        private int propertyId;
        private Map<Long,NodePropertyUpdate> updateMap;

        AdaptableIndexStoreViewWrapper( LabelScanStore labelScanStore, LockService locks, NeoStores neoStores,
                Map<Long,NodePropertyUpdate> updateMap, int propertyId )
        {
            super( labelScanStore, locks, neoStores );
            this.updateMap = updateMap;
            this.propertyId = propertyId;
        }

        @Override
        public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds,
                IntPredicate propertyKeyIdFilter,
                Visitor<NodePropertyUpdates,FAILURE> propertyUpdatesVisitor,
                Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor )
        {
            StoreScan<FAILURE> failureStoreScan =
                    super.visitNodes( labelIds, propertyKeyIdFilter, propertyUpdatesVisitor, labelUpdateVisitor );
            return new LabelScanViewNodeStoreWrapper( (LabelScanViewNodeStoreScan) failureStoreScan, nodeStore,
                    updateMap, labelIds, propertyId );
        }

        @Override
        public void acceptUpdate( MultipleIndexPopulator.MultipleIndexUpdater updater, NodePropertyUpdate update,
                long currentlyIndexedNodeId )
        {
            super.acceptUpdate( updater, update, currentlyIndexedNodeId );
        }
    }

    private class LabelScanViewNodeStoreWrapper extends NodeStoreScan
    {
        private LabelScanViewNodeStoreScan delegate;
        private int[] labelIds;
        private int propertyId;
        private Map<Long,NodePropertyUpdate> updateMap;

        public LabelScanViewNodeStoreWrapper( LabelScanViewNodeStoreScan delegate, NodeStore nodeStore,
                Map<Long,NodePropertyUpdate> updateMap, int[] labelIds, int propertyId )
        {
            super( nodeStore, LockService.NO_LOCK_SERVICE, 0 );
            this.delegate = delegate;
            this.labelIds = labelIds;
            this.propertyId = propertyId;
            this.updateMap = updateMap;
        }

        @Override
        public void process( NodeRecord loaded ) throws Exception
        {
            delegate.process( loaded );
        }

        @Override
        protected PrimitiveLongResourceIterator getNodeIdIterator()
        {
            PrimitiveLongResourceIterator originalIterator = delegate.getNodeIdIterator();
            return new DelegatingPrimitiveLongResourceIterator( originalIterator, updateMap, labelIds, propertyId );
        }
    }

    private class DelegatingPrimitiveLongResourceIterator implements PrimitiveLongResourceIterator
    {

        private final int[] labelIds;
        private final int propertyId;
        private Map<Long,NodePropertyUpdate> updateMap;
        private PrimitiveLongResourceIterator delegate;

        DelegatingPrimitiveLongResourceIterator( PrimitiveLongResourceIterator delegate,
                Map<Long,NodePropertyUpdate> updateMap, int[] labelIds,
                int propertyId )
        {
            this.delegate = delegate;
            this.updateMap = updateMap;
            this.labelIds = labelIds;
            this.propertyId = propertyId;
        }

        @Override
        public boolean hasNext()
        {
            return delegate.hasNext();
        }

        @Override
        public long next()
        {
            long value = delegate.next();
            NodePropertyUpdate nodePropertyUpdate = updateMap.get( value );
            if ( nodePropertyUpdate != null )
            {
                postUpdate( nodePropertyUpdate );
            }
            return value;
        }

        private void postUpdate( NodePropertyUpdate nodePropertyUpdate )
        {
            try
            {
                IndexProxy indexProxy = indexService.getIndexProxy( new IndexDescriptor( labelIds[0], propertyId ) );
                try ( IndexUpdater indexUpdater = indexProxy.newUpdater( IndexUpdateMode.ONLINE ) )
                {
                    indexUpdater.process( nodePropertyUpdate );
                }
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }
}
