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
package org.neo4j.schema;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.SchemaCache;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.transaction.state.storeview.DynamicIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.EntityIdIterator;
import org.neo4j.kernel.impl.transaction.state.storeview.LabelScanViewNodeStoreScan;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.lock.LockService;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodeLabelUpdate;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.test.rule.EmbeddedDbmsRule;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.internal.helpers.collection.Iterables.iterable;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.kernel.database.Database.initialSchemaRulesLoader;

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
    public EmbeddedDbmsRule embeddedDatabase = new EmbeddedDbmsRule();
    private IndexDescriptor[] rules;
    private StorageEngine storageEngine;
    private SchemaCache schemaCache;

    @Parameterized.Parameters( name = "{0}" )
    public static GraphDatabaseSettings.SchemaIndex[] parameters()
    {
        return GraphDatabaseSettings.SchemaIndex.values();
    }

    @Parameterized.Parameter
    public GraphDatabaseSettings.SchemaIndex schemaIndex;

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
        storageEngine = embeddedDatabase.getDependencyResolver().resolveDependency( StorageEngine.class );
    }

    @Test
    public void applyConcurrentDeletesToPopulatedIndex() throws Throwable
    {
        List<EntityUpdates> updates = new ArrayList<>( 2 );
        updates.add( EntityUpdates.forEntity( country1.getId(), false ).withTokens( id( COUNTRY_LABEL ) )
                .removed( propertyId, Values.of( "Sweden" ) ).build() );
        updates.add( EntityUpdates.forEntity( color2.getId(), false ).withTokens( id( COLOR_LABEL ) )
                .removed( propertyId, Values.of( "green" ) ).build() );

        launchCustomIndexPopulation( labelsNameIdMap, propertyId, new UpdateGenerator( updates ) );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );

        try ( Transaction tx = embeddedDatabase.beginTx() )
        {
            Integer countryLabelId = labelsNameIdMap.get( COUNTRY_LABEL );
            Integer colorLabelId = labelsNameIdMap.get( COLOR_LABEL );
            try ( IndexReader indexReader = getIndexReader( propertyId, countryLabelId ) )
            {
                assertEquals("Should be removed by concurrent remove.",
                        0, indexReader.countIndexedNodes( 0, new int[] {propertyId}, Values.of( "Sweden"  )) );
            }

            try ( IndexReader indexReader = getIndexReader( propertyId, colorLabelId ) )
            {
                assertEquals("Should be removed by concurrent remove.",
                        0, indexReader.countIndexedNodes( 3, new int[] {propertyId}, Values.of( "green"  )) );
            }
        }
    }

    @Test
    public void applyConcurrentAddsToPopulatedIndex() throws Throwable
    {
        List<EntityUpdates> updates = new ArrayList<>( 2 );
        updates.add( EntityUpdates.forEntity( otherNodes[0].getId(), false ).withTokens( id( COUNTRY_LABEL ) )
                .added( propertyId, Values.of( "Denmark" ) ).build() );
        updates.add( EntityUpdates.forEntity( otherNodes[1].getId(), false ).withTokens( id( CAR_LABEL ) )
                .added( propertyId, Values.of( "BMW" ) ).build() );

        launchCustomIndexPopulation( labelsNameIdMap, propertyId, new UpdateGenerator( updates ) );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );

        try ( Transaction tx = embeddedDatabase.beginTx() )
        {
            Integer countryLabelId = labelsNameIdMap.get( COUNTRY_LABEL );
            Integer carLabelId = labelsNameIdMap.get( CAR_LABEL );
            try ( IndexReader indexReader = getIndexReader( propertyId, countryLabelId ) )
            {
                assertEquals("Should be added by concurrent add.", 1,
                        indexReader.countIndexedNodes( otherNodes[0].getId(), new int[] {propertyId}, Values.of( "Denmark" ) ) );
            }

            try ( IndexReader indexReader = getIndexReader( propertyId, carLabelId ) )
            {
                assertEquals("Should be added by concurrent add.", 1,
                        indexReader.countIndexedNodes( otherNodes[1].getId(), new int[] {propertyId}, Values.of( "BMW" ) ) );
            }
        }
    }

    @Test
    public void applyConcurrentChangesToPopulatedIndex() throws Throwable
    {
        List<EntityUpdates> updates = new ArrayList<>( 2 );
        updates.add( EntityUpdates.forEntity( color2.getId(), false ).withTokens( id( COLOR_LABEL ) )
                .changed( propertyId, Values.of( "green" ), Values.of( "pink" ) ).build() );
        updates.add( EntityUpdates.forEntity( car2.getId(), false ).withTokens( id( CAR_LABEL ) )
                .changed( propertyId, Values.of( "Ford" ), Values.of( "SAAB" ) ).build() );

        launchCustomIndexPopulation( labelsNameIdMap, propertyId, new UpdateGenerator( updates ) );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );

        try ( Transaction tx = embeddedDatabase.beginTx() )
        {
            Integer colorLabelId = labelsNameIdMap.get( COLOR_LABEL );
            Integer carLabelId = labelsNameIdMap.get( CAR_LABEL );
            try ( IndexReader indexReader = getIndexReader( propertyId, colorLabelId ) )
            {
                assertEquals( format( "Should be deleted by concurrent change. Reader is: %s, ", indexReader ), 0,
                        indexReader.countIndexedNodes( color2.getId(), new int[] {propertyId}, Values.of( "green" ) ) );
            }
            try ( IndexReader indexReader = getIndexReader( propertyId, colorLabelId ) )
            {
                assertEquals("Should be updated by concurrent change.", 1,
                        indexReader.countIndexedNodes( color2.getId(), new int[] {propertyId}, Values.of( "pink"  ) ) );
            }

            try ( IndexReader indexReader = getIndexReader( propertyId, carLabelId ) )
            {
                assertEquals("Should be added by concurrent change.", 1,
                        indexReader.countIndexedNodes( car2.getId(), new int[] {propertyId}, Values.of( "SAAB"  ) ) );
            }
        }
    }

    @Test
    public void dropOneOfTheIndexesWhilePopulationIsOngoingDoesInfluenceOtherPopulators() throws Throwable
    {
        launchCustomIndexPopulation( labelsNameIdMap, propertyId,
                new IndexDropAction( labelsNameIdMap.get( COLOR_LABEL ) ) );
        labelsNameIdMap.remove( COLOR_LABEL );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );

        checkIndexIsOnline( labelsNameIdMap.get( CAR_LABEL ) );
        checkIndexIsOnline( labelsNameIdMap.get( COUNTRY_LABEL ));
    }

    @Test
    public void indexDroppedDuringPopulationDoesNotExist() throws Throwable
    {
        Integer labelToDropId = labelsNameIdMap.get( COLOR_LABEL );
        launchCustomIndexPopulation( labelsNameIdMap, propertyId, new IndexDropAction( labelToDropId ) );
        labelsNameIdMap.remove( COLOR_LABEL );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );
        try
        {
            Iterator<IndexDescriptor> iterator = schemaCache.indexesForSchema( SchemaDescriptor.forLabel( labelToDropId, propertyId ) );
            while ( iterator.hasNext() )
            {
                IndexDescriptor index = iterator.next();
                indexService.getIndexProxy( index );
            }
            fail( "Index does not exist, we should fail to find it." );
        }
        catch ( IndexNotFoundKernelException ignore )
        {
            // expected
        }
    }

    private void checkIndexIsOnline( int labelId ) throws IndexNotFoundKernelException
    {
        LabelSchemaDescriptor schema = SchemaDescriptor.forLabel( labelId, propertyId );
        IndexDescriptor index = single( schemaCache.indexesForSchema( schema ) );
        IndexProxy indexProxy = indexService.getIndexProxy( index );
        assertSame( indexProxy.getState(), InternalIndexState.ONLINE );
    }

    private long[] id( String label )
    {
        return new long[]{labelsNameIdMap.get( label )};
    }

    private IndexReader getIndexReader( int propertyId, Integer countryLabelId ) throws IndexNotFoundKernelException
    {
        LabelSchemaDescriptor schema = SchemaDescriptor.forLabel( countryLabelId, propertyId );
        IndexDescriptor index = single( schemaCache.indexesForSchema( schema ) );
        return indexService.getIndexProxy( index ).newReader();
    }

    private void launchCustomIndexPopulation( Map<String,Integer> labelNameIdMap, int propertyId, Runnable customAction ) throws Throwable
    {
        RecordStorageEngine storageEngine = getStorageEngine();
        LabelScanStore labelScanStore = getLabelScanStore();

        try ( Transaction transaction = embeddedDatabase.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) transaction).kernelTransaction();
            DynamicIndexStoreView storeView = dynamicIndexStoreViewWrapper( customAction, storageEngine::newReader, labelScanStore );

            IndexProviderMap providerMap = getIndexProviderMap();
            JobScheduler scheduler = getJobScheduler();
            TokenNameLookup tokenNameLookup = new SilentTokenNameLookup( ktx.tokenRead() );

            NullLogProvider nullLogProvider = NullLogProvider.getInstance();
            indexService = IndexingServiceFactory.createIndexingService( Config.defaults(), scheduler,
                    providerMap, storeView, tokenNameLookup, initialSchemaRulesLoader( storageEngine ),
                    nullLogProvider, nullLogProvider, IndexingService.NO_MONITOR, getSchemaState(),
                    mock( IndexStatisticsStore.class ), false );
            indexService.start();

            rules = createIndexRules( labelNameIdMap, propertyId );
            schemaCache = new SchemaCache( new StandardConstraintSemantics(), providerMap );
            schemaCache.load( iterable( rules ) );

            indexService.createIndexes( rules );
            transaction.commit();
        }
    }

    private DynamicIndexStoreView dynamicIndexStoreViewWrapper( Runnable customAction,
            Supplier<StorageReader> readerSupplier, LabelScanStore labelScanStore )
    {
        LockService locks = LockService.NO_LOCK_SERVICE;
        NeoStoreIndexStoreView neoStoreIndexStoreView = new NeoStoreIndexStoreView( locks, readerSupplier );
        return new DynamicIndexStoreViewWrapper( neoStoreIndexStoreView, labelScanStore, locks, readerSupplier, customAction );
    }

    private void waitAndActivateIndexes( Map<String,Integer> labelsIds, int propertyId )
            throws IndexNotFoundKernelException, IndexPopulationFailedKernelException, InterruptedException,
            IndexActivationFailedKernelException
    {
        try ( Transaction tx = embeddedDatabase.beginTx() )
        {
            for ( int labelId : labelsIds.values() )
            {
                waitIndexOnline( indexService, propertyId, labelId );
            }
        }
    }

    private int getPropertyId()
    {
        try ( Transaction tx = embeddedDatabase.beginTx() )
        {
            return getPropertyIdByName( tx, NAME_PROPERTY );
        }
    }

    private Map<String,Integer> getLabelsNameIdMap()
    {
        try ( Transaction tx = embeddedDatabase.beginTx() )
        {
            return getLabelIdsByName( tx, COUNTRY_LABEL, COLOR_LABEL, CAR_LABEL );
        }
    }

    private void waitIndexOnline( IndexingService indexService, int propertyId, int labelId )
            throws IndexNotFoundKernelException, IndexPopulationFailedKernelException, InterruptedException,
            IndexActivationFailedKernelException
    {
        LabelSchemaDescriptor schema = SchemaDescriptor.forLabel( labelId, propertyId );
        IndexDescriptor index = single( schemaCache.indexesForSchema( schema ) );
        IndexProxy indexProxy = indexService.getIndexProxy( index );
        indexProxy.awaitStoreScanCompleted( 0, TimeUnit.MILLISECONDS );
        while ( indexProxy.getState() != InternalIndexState.ONLINE )
        {
            Thread.sleep( 10 );
        }
        indexProxy.activate();
    }

    private IndexDescriptor[] createIndexRules( Map<String,Integer> labelNameIdMap, int propertyId )
    {
        final IndexProviderMap indexProviderMap = getIndexProviderMap();
        IndexProvider indexProvider = indexProviderMap.lookup( schemaIndex.providerName() );
        IndexProviderDescriptor providerDescriptor = indexProvider.getProviderDescriptor();
        List<IndexDescriptor> list = new ArrayList<>();
        for ( Integer labelId : labelNameIdMap.values() )
        {
            final LabelSchemaDescriptor schema = SchemaDescriptor.forLabel( labelId, propertyId );
            IndexDescriptor index = IndexPrototype.forSchema( schema, providerDescriptor )
                    .withName( "index_" + labelId ).materialise( labelId );
            index = indexProvider.completeConfiguration( index );
            list.add( index );
        }
        return list.toArray( new IndexDescriptor[0] );
    }

    private Map<String, Integer> getLabelIdsByName( Transaction tx, String... names )
    {
        Map<String,Integer> labelNameIdMap = new HashMap<>();
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        TokenRead tokenRead = ktx.tokenRead();
        for ( String name : names )
        {
            labelNameIdMap.put( name, tokenRead.nodeLabel( name ) );
        }
        return labelNameIdMap;
    }

    private int getPropertyIdByName( Transaction tx, String name )
    {
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        return ktx.tokenRead().propertyKey( name );
    }

    private void prepareDb()
    {
        Label countryLabel = Label.label( COUNTRY_LABEL );
        Label color = Label.label( COLOR_LABEL );
        Label car = Label.label( CAR_LABEL );
        try ( Transaction transaction = embeddedDatabase.beginTx() )
        {
            country1 = createNamedLabeledNode( transaction, countryLabel, "Sweden" );
            country2 = createNamedLabeledNode( transaction, countryLabel, "USA" );

            color1 = createNamedLabeledNode( transaction, color, "red" );
            color2 = createNamedLabeledNode( transaction, color, "green" );

            car1 = createNamedLabeledNode( transaction, car, "Volvo" );
            car2 = createNamedLabeledNode( transaction, car, "Ford" );

            otherNodes = new Node[250];
            for ( int i = 0; i < 250; i++ )
            {
                otherNodes[i] = transaction.createNode();
            }

            transaction.commit();
        }
    }

    private Node createNamedLabeledNode( Transaction tx, Label label, String name )
    {
        Node node = tx.createNode( label );
        node.setProperty( NAME_PROPERTY, name );
        return node;
    }

    private LabelScanStore getLabelScanStore()
    {
        return embeddedDatabase.resolveDependency( LabelScanStore.class );
    }

    private RecordStorageEngine getStorageEngine()
    {
        return embeddedDatabase.resolveDependency( RecordStorageEngine.class );
    }

    private SchemaState getSchemaState()
    {
        return embeddedDatabase.resolveDependency( SchemaState.class );
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
                Supplier<StorageReader> storageEngine, Runnable customAction )
        {
            super( neoStoreIndexStoreView, labelScanStore, locks, storageEngine, NullLogProvider.getInstance() );
            this.customAction = customAction;
        }

        @Override
        public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds,
                IntPredicate propertyKeyIdFilter,
                Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor,
                Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
                boolean forceStoreScan )
        {
            StoreScan<FAILURE> storeScan = super.visitNodes( labelIds, propertyKeyIdFilter, propertyUpdatesVisitor,
                    labelUpdateVisitor, forceStoreScan );
            return new LabelScanViewNodeStoreWrapper<>( storageEngine.get(), locks, getLabelScanStore(),
                    element -> false, propertyUpdatesVisitor, labelIds, propertyKeyIdFilter,
                    (LabelScanViewNodeStoreScan<FAILURE>) storeScan, customAction );
        }
    }

    private static class LabelScanViewNodeStoreWrapper<FAILURE extends Exception> extends LabelScanViewNodeStoreScan<FAILURE>
    {
        private final LabelScanViewNodeStoreScan<FAILURE> delegate;
        private final Runnable customAction;

        LabelScanViewNodeStoreWrapper( StorageReader storageReader, LockService locks,
                LabelScanStore labelScanStore, Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
                Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor, int[] labelIds, IntPredicate propertyKeyIdFilter,
                LabelScanViewNodeStoreScan<FAILURE> delegate,
                Runnable customAction )
        {
            super( storageReader, locks, labelScanStore, labelUpdateVisitor,
                    propertyUpdatesVisitor, labelIds, propertyKeyIdFilter );
            this.delegate = delegate;
            this.customAction = customAction;
        }

        @Override
        public EntityIdIterator getEntityIdIterator()
        {
            EntityIdIterator originalIterator = delegate.getEntityIdIterator();
            return new DelegatingEntityIdIterator( originalIterator, customAction );
        }
    }

    private static class DelegatingEntityIdIterator implements EntityIdIterator
    {
        private final Runnable customAction;
        private final EntityIdIterator delegate;

        DelegatingEntityIdIterator(
                EntityIdIterator delegate,
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

        @Override
        public void invalidateCache()
        {
            delegate.invalidateCache();
        }
    }

    private class UpdateGenerator implements Runnable
    {
        private Iterable<EntityUpdates> updates;

        UpdateGenerator( Iterable<EntityUpdates> updates )
        {
            this.updates = updates;
        }

        @Override
        public void run()
        {
            for ( EntityUpdates update : updates )
                {
                    try ( Transaction transaction = embeddedDatabase.beginTx() )
                    {
                        Node node = transaction.getNodeById( update.getEntityId() );
                        for ( int labelId : labelsNameIdMap.values() )
                        {
                            LabelSchemaDescriptor schema = SchemaDescriptor.forLabel( labelId, propertyId );
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
                        transaction.commit();
                    }
                }
                try ( StorageReader reader = storageEngine.newReader() )
                {
                    for ( EntityUpdates update : updates )
                    {
                        Iterable<IndexDescriptor> relatedIndexes = schemaCache.getIndexesRelatedTo(
                                update.entityTokensChanged(),
                                update.entityTokensUnchanged(),
                                update.propertiesChanged(), false, EntityType.NODE );
                        Iterable<IndexEntryUpdate<IndexDescriptor>> entryUpdates = update.forIndexKeys( relatedIndexes, reader, EntityType.NODE );
                        indexService.applyUpdates( entryUpdates );
                    }
                }
                catch ( UncheckedIOException | KernelException e )
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
            LabelSchemaDescriptor descriptor = SchemaDescriptor.forLabel( labelIdToDropIndexFor, propertyId );
            IndexDescriptor rule = findRuleForLabel( descriptor );
            indexService.dropIndex( rule );
        }

        private IndexDescriptor findRuleForLabel( LabelSchemaDescriptor schemaDescriptor )
        {
            for ( IndexDescriptor rule : rules )
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
