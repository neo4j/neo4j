/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.impl.schema.LuceneIndexProviderFactory;
import org.neo4j.kernel.api.impl.schema.NativeLuceneFusionIndexProviderFactory10;
import org.neo4j.kernel.api.impl.schema.NativeLuceneFusionIndexProviderFactory20;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.MultipleIndexPopulator;
import org.neo4j.kernel.impl.api.index.NodeUpdates;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.transaction.state.DirectIndexUpdates;
import org.neo4j.kernel.impl.transaction.state.storeview.DynamicIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.LabelScanViewNodeStoreScan;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

//[NodePropertyUpdate[0, prop:0 add:Sweden, labelsBefore:[], labelsAfter:[0]]]
//[NodePropertyUpdate[1, prop:0 add:USA, labelsBefore:[], labelsAfter:[0]]]
//[NodePropertyUpdate[2, prop:0 add:red, labelsBefore:[], labelsAfter:[1]]]
//[NodePropertyUpdate[3, prop:0 add:green, labelsBefore:[], labelsAfter:[0]]]
//[NodePropertyUpdate[4, prop:0 add:Volvo, labelsBefore:[], labelsAfter:[2]]]
//[NodePropertyUpdate[5, prop:0 add:Ford, labelsBefore:[], labelsAfter:[2]]]
//TODO: check count store counts
@RunWith( Parameterized.class )
public class MultiIndexPopulationConcurrentUpdatesIT
{
    private static final String NAME_PROPERTY = "name";
    private static final String COUNTRY_LABEL = "country";
    private static final String COLOR_LABEL = "color";
    private static final String CAR_LABEL = "car";

    @Rule
    public EmbeddedDatabaseRule embeddedDatabase = new EmbeddedDatabaseRule();
    private IndexRule[] rules;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<IndexProvider.Descriptor> parameters()
    {
        return asList( LuceneIndexProviderFactory.PROVIDER_DESCRIPTOR,
                NativeLuceneFusionIndexProviderFactory10.DESCRIPTOR,
                NativeLuceneFusionIndexProviderFactory20.DESCRIPTOR,
                InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR );
    }

    @Parameterized.Parameter( 0 )
    public IndexProvider.Descriptor indexDescriptor;

    private IndexingService indexService;
    private int propertyId;
    private Map<String,Integer> labelsNameIdMap;
    private Map<Integer,String> labelsIdNameMap;
    private Node country1;
    private Node country2;
    private Node color1;
    private Node color2;
    private Node car1;
    private Node car2;
    private Node[] otherNodes;

    @After
    public void tearDown() throws Throwable
    {
        if ( indexService != null )
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
        propertyId = getPropertyId();
    }

    @Test
    public void applyConcurrentDeletesToPopulatedIndex() throws Throwable
    {
        List<NodeUpdates> updates = new ArrayList<>( 2 );
        updates.add( NodeUpdates.forNode( country1.getId(), id( COUNTRY_LABEL ) )
                .removed( propertyId, Values.of( "Sweden" ) ).build() );
        updates.add( NodeUpdates.forNode( color2.getId(), id( COLOR_LABEL ) )
                .removed( propertyId, Values.of( "green" ) ).build() );

        launchCustomIndexPopulation( labelsNameIdMap, propertyId, new UpdateGenerator( updates ) );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );

        try ( Transaction ignored = embeddedDatabase.beginTx() )
        {
            Integer countryLabelId = labelsNameIdMap.get( COUNTRY_LABEL );
            Integer colorLabelId = labelsNameIdMap.get( COLOR_LABEL );
            try ( IndexReader indexReader = getIndexReader( propertyId, countryLabelId ) )
            {
                assertEquals("Should be removed by concurrent remove.",
                        0, indexReader.countIndexedNodes( 0, Values.of( "Sweden"  )) );
            }

            try ( IndexReader indexReader = getIndexReader( propertyId, colorLabelId ) )
            {
                assertEquals("Should be removed by concurrent remove.",
                        0, indexReader.countIndexedNodes( 3, Values.of( "green"  )) );
            }
        }
    }

    @Test
    public void applyConcurrentAddsToPopulatedIndex() throws Throwable
    {
        List<NodeUpdates> updates = new ArrayList<>( 2 );
        updates.add( NodeUpdates.forNode( otherNodes[0].getId(), id( COUNTRY_LABEL ) )
                .added( propertyId, Values.of( "Denmark" ) ).build() );
        updates.add( NodeUpdates.forNode( otherNodes[1].getId(), id( CAR_LABEL ) )
                .added( propertyId, Values.of( "BMW" ) ).build() );

        launchCustomIndexPopulation( labelsNameIdMap, propertyId, new UpdateGenerator( updates ) );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );

        try ( Transaction ignored = embeddedDatabase.beginTx() )
        {
            Integer countryLabelId = labelsNameIdMap.get( COUNTRY_LABEL );
            Integer carLabelId = labelsNameIdMap.get( CAR_LABEL );
            try ( IndexReader indexReader = getIndexReader( propertyId, countryLabelId ) )
            {
                assertEquals("Should be added by concurrent add.", 1,
                        indexReader.countIndexedNodes( otherNodes[0].getId(), Values.of( "Denmark" ) ) );
            }

            try ( IndexReader indexReader = getIndexReader( propertyId, carLabelId ) )
            {
                assertEquals("Should be added by concurrent add.", 1,
                        indexReader.countIndexedNodes( otherNodes[1].getId(), Values.of( "BMW" ) ) );
            }
        }
    }

    @Test
    public void applyConcurrentChangesToPopulatedIndex() throws Exception
    {
        List<NodeUpdates> updates = new ArrayList<>( 2 );
        updates.add( NodeUpdates.forNode( color2.getId(), id( COLOR_LABEL ) )
                .changed( propertyId, Values.of( "green" ), Values.of( "pink" ) ).build() );
        updates.add( NodeUpdates.forNode( car2.getId(), id( CAR_LABEL ) )
                .changed( propertyId, Values.of( "Ford" ), Values.of( "SAAB" ) ).build() );

        launchCustomIndexPopulation( labelsNameIdMap, propertyId, new UpdateGenerator( updates ) );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );

        try ( Transaction ignored = embeddedDatabase.beginTx() )
        {
            Integer colorLabelId = labelsNameIdMap.get( COLOR_LABEL );
            Integer carLabelId = labelsNameIdMap.get( CAR_LABEL );
            try ( IndexReader indexReader = getIndexReader( propertyId, colorLabelId ) )
            {
                assertEquals( format( "Should be deleted by concurrent change. Reader is: %s, ", indexReader ), 0,
                        indexReader.countIndexedNodes( color2.getId(), Values.of( "green" ) ) );
            }
            try ( IndexReader indexReader = getIndexReader( propertyId, colorLabelId ) )
            {
                assertEquals("Should be updated by concurrent change.", 1,
                        indexReader.countIndexedNodes( color2.getId(), Values.of( "pink"  ) ) );
            }

            try ( IndexReader indexReader = getIndexReader( propertyId, carLabelId ) )
            {
                assertEquals("Should be added by concurrent change.", 1,
                        indexReader.countIndexedNodes( car2.getId(), Values.of( "SAAB"  ) ) );
            }
        }
    }

    @Test
    public void dropOneOfTheIndexesWhilePopulationIsOngoingDoesInfluenceOtherPopulators() throws Exception
    {
        launchCustomIndexPopulation( labelsNameIdMap, propertyId,
                new IndexDropAction( labelsNameIdMap.get( COLOR_LABEL ) ) );
        labelsNameIdMap.remove( COLOR_LABEL );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );

        checkIndexIsOnline( labelsNameIdMap.get( CAR_LABEL ) );
        checkIndexIsOnline( labelsNameIdMap.get( COUNTRY_LABEL ));
    }

    @Test
    public void indexDroppedDuringPopulationDoesNotExist() throws Exception
    {
        Integer labelToDropId = labelsNameIdMap.get( COLOR_LABEL );
        launchCustomIndexPopulation( labelsNameIdMap, propertyId,
                new IndexDropAction( labelToDropId ) );
        labelsNameIdMap.remove( COLOR_LABEL );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );
        try
        {
            indexService.getIndexProxy( SchemaDescriptorFactory.forLabel( labelToDropId, propertyId ) );
            fail( "Index does not exist, we should fail to find it." );
        }
        catch ( IndexNotFoundKernelException infe )
        {
            // expected
        }
    }

    private void checkIndexIsOnline( int labelId ) throws IndexNotFoundKernelException
    {
        IndexProxy indexProxy = indexService.getIndexProxy( SchemaDescriptorFactory.forLabel( labelId, propertyId ) );
        assertSame( indexProxy.getState(), InternalIndexState.ONLINE );
    }

    private long[] id( String label )
    {
        return new long[]{labelsNameIdMap.get( label )};
    }

    private IndexReader getIndexReader( int propertyId, Integer countryLabelId ) throws IndexNotFoundKernelException
    {
        return indexService.getIndexProxy( SchemaDescriptorFactory.forLabel( countryLabelId, propertyId ) )
                .newReader();
    }

    private void launchCustomIndexPopulation( Map<String,Integer> labelNameIdMap, int propertyId,
            Runnable customAction ) throws Exception
    {
        NeoStores neoStores = getNeoStores();
        LabelScanStore labelScanStore = getLabelScanStore();
        ThreadToStatementContextBridge transactionStatementContextBridge = getTransactionStatementContextBridge();

        try ( Transaction transaction = embeddedDatabase.beginTx();
              KernelTransaction ktx = transactionStatementContextBridge.getKernelTransactionBoundToThisThread( true ) )
        {
            DynamicIndexStoreView storeView = dynamicIndexStoreViewWrapper( customAction, neoStores, labelScanStore );

            IndexProviderMap providerMap = getIndexProviderMap();
            JobScheduler scheduler = getJobScheduler();
            TokenNameLookup tokenNameLookup = new SilentTokenNameLookup( ktx.tokenRead() );

            indexService = IndexingServiceFactory.createIndexingService( Config.defaults(), scheduler,
                    providerMap, storeView, tokenNameLookup, getIndexRules( neoStores ),
                    NullLogProvider.getInstance(), IndexingService.NO_MONITOR, getSchemaState() );
            indexService.start();

            rules = createIndexRules( labelNameIdMap, propertyId );

            indexService.createIndexes( rules );
            transaction.success();
        }
    }

    private DynamicIndexStoreView dynamicIndexStoreViewWrapper( Runnable customAction, NeoStores neoStores,
            LabelScanStore labelScanStore )
    {
        LockService locks = LockService.NO_LOCK_SERVICE;
        NeoStoreIndexStoreView neoStoreIndexStoreView = new NeoStoreIndexStoreView( locks, neoStores );
        return new DynamicIndexStoreViewWrapper( neoStoreIndexStoreView, labelScanStore, locks, neoStores, customAction );
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

    private int getPropertyId()
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
        IndexProxy indexProxy = indexService.getIndexProxy( SchemaDescriptorFactory.forLabel( labelId, propertyId ) );
        indexProxy.awaitStoreScanCompleted();
        while ( indexProxy.getState() != InternalIndexState.ONLINE )
        {
            Thread.sleep( 10 );
        }
        indexProxy.activate();
    }

    private IndexRule[] createIndexRules( Map<String,Integer> labelNameIdMap, int propertyId )
    {
        return labelNameIdMap.values().stream()
                .map( index -> IndexRule.indexRule( index, SchemaIndexDescriptorFactory.forLabel( index, propertyId ), indexDescriptor ) )
                .toArray( IndexRule[]::new );
    }

    private List<IndexRule> getIndexRules( NeoStores neoStores )
    {
        return Iterators.asList( new SchemaStorage( neoStores.getSchemaStore() ).indexesGetAll() );
    }

    private Map<String, Integer> getLabelIdsByName( String... names )
    {
        ThreadToStatementContextBridge transactionStatementContextBridge = getTransactionStatementContextBridge();
        Map<String,Integer> labelNameIdMap = new HashMap<>();
        KernelTransaction ktx =
                transactionStatementContextBridge.getKernelTransactionBoundToThisThread( true );
        try ( Statement ignore = ktx.acquireStatement() )
        {
            TokenRead tokenRead = ktx.tokenRead();
            for ( String name : names )
            {
                labelNameIdMap.put( name, tokenRead.nodeLabel( name ) );
            }
        }
        return labelNameIdMap;
    }

    private int getPropertyIdByName( String name )
    {
        ThreadToStatementContextBridge transactionStatementContextBridge = getTransactionStatementContextBridge();
        KernelTransaction ktx =
                transactionStatementContextBridge.getKernelTransactionBoundToThisThread( true );
        try ( Statement ignore = ktx.acquireStatement() )
        {
            return ktx.tokenRead().propertyKey( name );
        }
    }

    private void prepareDb()
    {
        Label countryLabel = Label.label( COUNTRY_LABEL );
        Label color = Label.label( COLOR_LABEL );
        Label car = Label.label( CAR_LABEL );
        try ( Transaction transaction = embeddedDatabase.beginTx() )
        {
            country1 = createNamedLabeledNode( countryLabel, "Sweden" );
            country2 = createNamedLabeledNode( countryLabel, "USA" );

            color1 = createNamedLabeledNode( color, "red" );
            color2 = createNamedLabeledNode( color, "green" );

            car1 = createNamedLabeledNode( car, "Volvo" );
            car2 = createNamedLabeledNode( car, "Ford" );

            otherNodes = new Node[250];
            for ( int i = 0; i < 250; i++ )
            {
                otherNodes[i] = embeddedDatabase.createNode();
            }

            transaction.success();
        }
    }

    private Node createNamedLabeledNode( Label label, String name )
    {
        Node node = embeddedDatabase.createNode( label );
        node.setProperty( NAME_PROPERTY, name );
        return node;
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

    private SchemaState getSchemaState()
    {
        return embeddedDatabase.resolveDependency( SchemaState.class );
    }

    private ThreadToStatementContextBridge getTransactionStatementContextBridge()
    {
        return embeddedDatabase.resolveDependency( ThreadToStatementContextBridge.class );
    }

    private IndexProviderMap getIndexProviderMap()
    {
        return embeddedDatabase.resolveDependency( IndexProviderMap.class );
    }

    private JobScheduler getJobScheduler()
    {
        return embeddedDatabase.resolveDependency( JobScheduler.class );
    }

    private class DynamicIndexStoreViewWrapper extends DynamicIndexStoreView
    {
        private final Runnable customAction;

        DynamicIndexStoreViewWrapper( NeoStoreIndexStoreView neoStoreIndexStoreView, LabelScanStore labelScanStore, LockService locks,
                NeoStores neoStores, Runnable customAction )
        {
            super( neoStoreIndexStoreView, labelScanStore, locks, neoStores, NullLogProvider.getInstance() );
            this.customAction = customAction;
        }

        @Override
        public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds,
                IntPredicate propertyKeyIdFilter,
                Visitor<NodeUpdates,FAILURE> propertyUpdatesVisitor,
                Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
                boolean forceStoreScan )
        {
            StoreScan<FAILURE> storeScan = super.visitNodes( labelIds, propertyKeyIdFilter, propertyUpdatesVisitor,
                    labelUpdateVisitor, forceStoreScan );
            return new LabelScanViewNodeStoreWrapper<>( nodeStore, locks, propertyStore, getLabelScanStore(),
                    element -> false, propertyUpdatesVisitor, labelIds, propertyKeyIdFilter,
                    (LabelScanViewNodeStoreScan<FAILURE>) storeScan, customAction );
        }
    }

    private class LabelScanViewNodeStoreWrapper<FAILURE extends Exception> extends LabelScanViewNodeStoreScan<FAILURE>
    {
        private final LabelScanViewNodeStoreScan<FAILURE> delegate;
        private final Runnable customAction;

        LabelScanViewNodeStoreWrapper( NodeStore nodeStore, LockService locks,
                PropertyStore propertyStore,
                LabelScanStore labelScanStore, Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
                Visitor<NodeUpdates,FAILURE> propertyUpdatesVisitor, int[] labelIds, IntPredicate propertyKeyIdFilter,
                LabelScanViewNodeStoreScan<FAILURE> delegate,
                Runnable customAction )
        {
            super( nodeStore, locks, propertyStore, labelScanStore, labelUpdateVisitor,
                    propertyUpdatesVisitor, labelIds, propertyKeyIdFilter );
            this.delegate = delegate;
            this.customAction = customAction;
        }

        @Override
        public void acceptUpdate( MultipleIndexPopulator.MultipleIndexUpdater updater, IndexEntryUpdate<?> update,
                long currentlyIndexedNodeId )
        {
            delegate.acceptUpdate( updater, update, currentlyIndexedNodeId );
        }

        @Override
        public PrimitiveLongResourceIterator getNodeIdIterator()
        {
            PrimitiveLongResourceIterator originalIterator = delegate.getNodeIdIterator();
            return new DelegatingPrimitiveLongResourceIterator( originalIterator, customAction );
        }
    }

    private class DelegatingPrimitiveLongResourceIterator implements PrimitiveLongResourceIterator
    {
        private final Runnable customAction;
        private final PrimitiveLongResourceIterator delegate;

        DelegatingPrimitiveLongResourceIterator(
                PrimitiveLongResourceIterator delegate,
                Runnable customAction )
        {
            this.delegate = delegate;
            this.customAction = customAction;
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
                customAction.run();
            }
            return value;
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }

    private class UpdateGenerator implements Runnable
    {

        private Iterable<NodeUpdates> updates;

        UpdateGenerator( Iterable<NodeUpdates> updates )
        {
            this.updates = updates;
        }

        @Override
        public void run()
        {
            for ( NodeUpdates update : updates )
                {
                    try ( Transaction transaction = embeddedDatabase.beginTx() )
                    {
                        Node node = embeddedDatabase.getNodeById( update.getNodeId() );
                        for ( int labelId : labelsNameIdMap.values() )
                        {
                            LabelSchemaDescriptor schema = SchemaDescriptorFactory.forLabel( labelId, propertyId );
                            for ( IndexEntryUpdate<?> indexUpdate :
                                    update.forIndexKeys( Collections.singleton( schema ) ) )
                            {
                                switch ( indexUpdate.updateMode() )
                                {
                                case CHANGED:
                                case ADDED:
                                    node.addLabel(
                                            Label.label( labelsIdNameMap.get( schema.getLabelId() ) ) );
                                    node.setProperty( NAME_PROPERTY, indexUpdate.values()[0].asObject() );
                                    break;
                                case REMOVED:
                                    node.addLabel(
                                            Label.label( labelsIdNameMap.get( schema.getLabelId() ) ) );
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
                try
                {
                    for ( NodeUpdates update : updates )
                    {
                        Iterable<IndexEntryUpdate<SchemaDescriptor>> entryUpdates =
                                indexService.convertToIndexUpdates( update );
                        DirectIndexUpdates directIndexUpdates = new DirectIndexUpdates( entryUpdates );
                        indexService.apply( directIndexUpdates );
                    }
                }
                catch ( IOException | IndexEntryConflictException e )
                {
                    throw new RuntimeException( e );
                }
        }
    }

    private class IndexDropAction implements Runnable
    {
        private int labelIdToDropIndexFor;

        private IndexDropAction( int labelIdToDropIndexFor )
        {
            this.labelIdToDropIndexFor = labelIdToDropIndexFor;
        }

        @Override
        public void run()
        {
            org.neo4j.kernel.api.schema.LabelSchemaDescriptor descriptor =
                    SchemaDescriptorFactory.forLabel( labelIdToDropIndexFor, propertyId );
            IndexRule rule = findRuleForLabel( descriptor );
            try
            {
                indexService.dropIndex( rule );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        private IndexRule findRuleForLabel( LabelSchemaDescriptor schemaDescriptor )
        {
            for ( IndexRule rule : rules )
            {
                if ( rule.schema().equals( schemaDescriptor ) )
                {
                    return rule;
                }
            }
            return null;
        }
    }
}
