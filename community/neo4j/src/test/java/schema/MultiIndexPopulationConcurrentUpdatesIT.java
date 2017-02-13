/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

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
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.IndexDescriptorFactory;
import org.neo4j.kernel.api.index.NodeUpdates;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.MultipleIndexPopulator;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.transaction.state.DefaultSchemaIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.DirectIndexUpdates;
import org.neo4j.kernel.impl.transaction.state.storeview.DynamicIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.LabelScanViewNodeStoreScan;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;

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
    private Map<Integer,String> labelsIdNameMap;

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
        labelsIdNameMap = labelsNameIdMap.entrySet()
                .stream()
                .collect( Collectors.toMap( Map.Entry::getValue, Map.Entry::getKey ) );
        propertyId = getPropertyKeyId();
    }

    @Test
    public void applyConcurrentDeletesToPopulatedIndex() throws Throwable
    {
        List<NodeUpdates> updates = new ArrayList<>( 2 );
        updates.add( NodeUpdates.forNode( 0, new long[]{labelsNameIdMap.get( COUNTRY_LABEL )} )
                .removed( propertyId, "Sweden" ).build() );
        updates.add( NodeUpdates.forNode( 3, new long[]{labelsNameIdMap.get( COLOR_LABEL )} )
                .removed( propertyId, "green" ).build() );

        launchCustomIndexPopulation( labelsNameIdMap, propertyId, updates );
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
        List<NodeUpdates> updates = new ArrayList<>( 2 );
        updates.add( NodeUpdates.forNode( 6, new long[]{labelsNameIdMap.get( COUNTRY_LABEL )} )
                .added( propertyId, "Denmark" ).build() );
        updates.add( NodeUpdates.forNode( 7, new long[]{labelsNameIdMap.get( CAR_LABEL )} )
                .added( propertyId, "BMW" ).build() );

        launchCustomIndexPopulation( labelsNameIdMap, propertyId, updates );
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
        List<NodeUpdates> updates = new ArrayList<>( 2 );
        updates.add( NodeUpdates.forNode( 3, new long[]{labelsNameIdMap.get( COLOR_LABEL )} )
                .changed( propertyId, "green", "pink" ).build() );
        updates.add( NodeUpdates.forNode( 5, new long[]{labelsNameIdMap.get( CAR_LABEL )} )
                .changed( propertyId, "Ford", "SAAB" ).build() );

        launchCustomIndexPopulation( labelsNameIdMap, propertyId, updates );
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
        return indexService.getIndexProxy( IndexDescriptorFactory.of( countryLabelId, propertyId ) ).newReader();
    }

    private void launchCustomIndexPopulation( Map<String,Integer> labelNameIdMap, int propertyId,
            List<NodeUpdates> updates ) throws Exception
    {
        NeoStores neoStores = getNeoStores();
        LabelScanStore labelScanStore = getLabelScanStore();
        ThreadToStatementContextBridge transactionStatementContextBridge = getTransactionStatementContextBridge();

        try ( Transaction transaction = embeddedDatabase.beginTx() )
        {
            Statement statement = transactionStatementContextBridge.get();

            DynamicIndexStoreView storeView =
                    new DynamicIndexStoreViewWrapper( labelScanStore, LockService.NO_LOCK_SERVICE, neoStores,
                            updates );

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
        IndexProxy indexProxy = indexService.getIndexProxy( IndexDescriptorFactory.of( labelId, propertyId ) );
        indexProxy.awaitStoreScanCompleted();
        indexProxy.activate();
    }

    private IndexRule[] createIndexRules( Map<String,Integer> labelNameIdMap, int propertyId )
    {
        return labelNameIdMap.values().stream()
                .map( index -> IndexRule.indexRule( index, NewIndexDescriptorFactory.forLabel( index, propertyId ),
                        new SchemaIndexProvider.Descriptor( "lucene", "version" ) ) )
                .toArray( IndexRule[]::new );
    }

    private List<IndexRule> getIndexRules( NeoStores neoStores )
    {
        return Iterators.asList( new SchemaStorage( neoStores.getSchemaStore() ).indexesGetAll() );
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

            for ( int i = 0; i < 250; i++ )
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
        return embeddedDatabase.resolveDependency( LabelScanStore.class );
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

    private class DynamicIndexStoreViewWrapper extends DynamicIndexStoreView
    {
        private final List<NodeUpdates> updates;

        DynamicIndexStoreViewWrapper( LabelScanStore labelScanStore, LockService locks, NeoStores neoStores,
                List<NodeUpdates> updates )
        {
            super( labelScanStore, locks, neoStores, NullLogProvider.getInstance() );
            this.updates = updates;
        }

        @Override
        public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds,
                IntPredicate propertyKeyIdFilter,
                Visitor<NodeUpdates,FAILURE> propertyUpdatesVisitor,
                Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor )
        {
            StoreScan<FAILURE> storeScan =
                    super.visitNodes( labelIds, propertyKeyIdFilter, propertyUpdatesVisitor, labelUpdateVisitor );
            return new LabelScanViewNodeStoreWrapper( nodeStore, locks, propertyStore, getLabelScanStore(),
                    element -> false, propertyUpdatesVisitor, labelIds, propertyKeyIdFilter,
                    (LabelScanViewNodeStoreScan) storeScan, updates);
        }
    }

    private class LabelScanViewNodeStoreWrapper extends LabelScanViewNodeStoreScan
    {
        private final LabelScanViewNodeStoreScan delegate;
        private final List<NodeUpdates> updates;

        LabelScanViewNodeStoreWrapper( NodeStore nodeStore, LockService locks,
                PropertyStore propertyStore,
                LabelScanStore labelScanStore, Visitor labelUpdateVisitor,
                Visitor propertyUpdatesVisitor, int[] labelIds, IntPredicate propertyKeyIdFilter,
                LabelScanViewNodeStoreScan delegate,
                List<NodeUpdates> updates )
        {
            super( nodeStore, locks, propertyStore, labelScanStore, labelUpdateVisitor,
                    propertyUpdatesVisitor, labelIds, propertyKeyIdFilter );
            this.delegate = delegate;
            this.updates = updates;
        }

        @Override
        public void acceptUpdate( MultipleIndexPopulator.MultipleIndexUpdater updater, IndexEntryUpdate update,
                long currentlyIndexedNodeId )
        {
            delegate.acceptUpdate( updater, update, currentlyIndexedNodeId );
        }

        @Override
        public PrimitiveLongResourceIterator getNodeIdIterator()
        {
            PrimitiveLongResourceIterator originalIterator = delegate.getNodeIdIterator();
            return new DelegatingPrimitiveLongResourceIterator( originalIterator,
                    updates );
        }
    }

    private class DelegatingPrimitiveLongResourceIterator implements PrimitiveLongResourceIterator
    {

        private final List<NodeUpdates> updates;
        private final PrimitiveLongResourceIterator delegate;

        DelegatingPrimitiveLongResourceIterator(
                PrimitiveLongResourceIterator delegate,
                List<NodeUpdates> updates )
        {
            this.delegate = delegate;
            this.updates = updates;
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
            if ( !hasNext() )
            {
                for ( NodeUpdates update : updates )
                {
                    try ( Transaction transaction = embeddedDatabase.beginTx() )
                    {
                        Node node = embeddedDatabase.getNodeById( update.getNodeId() );
                        for ( int labelId : labelsNameIdMap.values() )
                        {
                            NewIndexDescriptor descriptor = NewIndexDescriptorFactory.forLabel( labelId, propertyId );
                            Optional<IndexEntryUpdate> indexEntryUpdateOptional = update.forIndex( descriptor );
                            if ( indexEntryUpdateOptional.isPresent() )
                            {
                                IndexEntryUpdate indexUpdate = indexEntryUpdateOptional.get();
                                switch ( indexUpdate.updateMode() )
                                {
                                case CHANGED:
                                case ADDED:
                                    node.addLabel(
                                            Label.label( labelsIdNameMap.get( descriptor.schema().getLabelId() ) ) );
                                    node.setProperty( NAME_PROPERTY, indexUpdate.value() );
                                    break;
                                case REMOVED:
                                    node.addLabel(
                                            Label.label( labelsIdNameMap.get( descriptor.schema().getLabelId() ) ) );
                                    node.delete();
                                    break;
                                default:
                                    throw new IllegalArgumentException( indexUpdate.updateMode().name() );
                                }
                            }
                        }
                        transaction.success();
                    }
                }
                DirectIndexUpdates nodePropertyUpdates = new DirectIndexUpdates( updates );
                try
                {
                    indexService.apply( nodePropertyUpdates );
                }
                catch ( IOException | IndexEntryConflictException e )
                {
                    throw new RuntimeException( e );
                }
            }
            return value;
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }

}
