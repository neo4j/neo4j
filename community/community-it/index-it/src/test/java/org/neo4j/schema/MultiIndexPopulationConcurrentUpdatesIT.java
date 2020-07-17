/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.index.label.RelationshipTypeScanStore;
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
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
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
import org.neo4j.kernel.impl.transaction.state.storeview.LabelViewNodeStoreScan;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.lock.LockService;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.common.Subject.AUTH_DISABLED;
import static org.neo4j.internal.helpers.collection.Iterables.iterable;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.database.Database.initialSchemaRulesLoader;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

//[NodePropertyUpdate[0, prop:0 add:Sweden, labelsBefore:[], labelsAfter:[0]]]
//[NodePropertyUpdate[1, prop:0 add:USA, labelsBefore:[], labelsAfter:[0]]]
//[NodePropertyUpdate[2, prop:0 add:red, labelsBefore:[], labelsAfter:[1]]]
//[NodePropertyUpdate[3, prop:0 add:green, labelsBefore:[], labelsAfter:[0]]]
//[NodePropertyUpdate[4, prop:0 add:Volvo, labelsBefore:[], labelsAfter:[2]]]
//[NodePropertyUpdate[5, prop:0 add:Ford, labelsBefore:[], labelsAfter:[2]]]
//TODO: check count store counts
@ImpermanentDbmsExtension
public class MultiIndexPopulationConcurrentUpdatesIT
{
    private static final String NAME_PROPERTY = "name";
    private static final String COUNTRY_LABEL = "country";
    private static final String COLOR_LABEL = "color";
    private static final String CAR_LABEL = "car";

    @Inject
    private GraphDatabaseAPI db;

    private IndexDescriptor[] rules;
    private StorageEngine storageEngine;
    private SchemaCache schemaCache;
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

    @AfterEach
    void tearDown() throws Throwable
    {
        if ( indexService != null )
        {
            indexService.shutdown();
        }
    }

    @BeforeEach
    void setUp()
    {
        prepareDb();

        labelsNameIdMap = getLabelsNameIdMap();
        labelsIdNameMap = labelsNameIdMap.entrySet()
                .stream()
                .collect( Collectors.toMap( Map.Entry::getValue, Map.Entry::getKey ) );
        propertyId = getPropertyId();
        storageEngine = db.getDependencyResolver().resolveDependency( StorageEngine.class );
    }

    private static Stream<GraphDatabaseSettings.SchemaIndex> parameters()
    {
        return Arrays.stream( GraphDatabaseSettings.SchemaIndex.values() );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void applyConcurrentDeletesToPopulatedIndex( GraphDatabaseSettings.SchemaIndex schemaIndex ) throws Throwable
    {
        List<EntityUpdates> updates = new ArrayList<>( 2 );
        updates.add( EntityUpdates.forEntity( country1.getId(), false ).withTokens( id( COUNTRY_LABEL ) )
                .removed( propertyId, Values.of( "Sweden" ) ).build() );
        updates.add( EntityUpdates.forEntity( color2.getId(), false ).withTokens( id( COLOR_LABEL ) )
                .removed( propertyId, Values.of( "green" ) ).build() );

        launchCustomIndexPopulation( schemaIndex, labelsNameIdMap, propertyId, new UpdateGenerator( updates ) );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );

        try ( Transaction tx = db.beginTx() )
        {
            Integer countryLabelId = labelsNameIdMap.get( COUNTRY_LABEL );
            Integer colorLabelId = labelsNameIdMap.get( COLOR_LABEL );
            try ( IndexReader indexReader = getIndexReader( propertyId, countryLabelId ) )
            {
                assertThat( indexReader.countIndexedNodes( 0, NULL, new int[]{propertyId}, Values.of( "Sweden" ) ) )
                        .as( "Should be removed by concurrent remove." ).isEqualTo( 0 );
            }

            try ( IndexReader indexReader = getIndexReader( propertyId, colorLabelId ) )
            {
                assertThat( indexReader.countIndexedNodes( 3, NULL, new int[]{propertyId}, Values.of( "green" ) ) )
                        .as( "Should be removed by concurrent remove." ).isEqualTo( 0 );
            }
        }
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void applyConcurrentAddsToPopulatedIndex( GraphDatabaseSettings.SchemaIndex schemaIndex ) throws Throwable
    {
        List<EntityUpdates> updates = new ArrayList<>( 2 );
        updates.add( EntityUpdates.forEntity( otherNodes[0].getId(), false ).withTokens( id( COUNTRY_LABEL ) )
                .added( propertyId, Values.of( "Denmark" ) ).build() );
        updates.add( EntityUpdates.forEntity( otherNodes[1].getId(), false ).withTokens( id( CAR_LABEL ) )
                .added( propertyId, Values.of( "BMW" ) ).build() );

        launchCustomIndexPopulation( schemaIndex, labelsNameIdMap, propertyId, new UpdateGenerator( updates ) );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );

        try ( Transaction tx = db.beginTx() )
        {
            Integer countryLabelId = labelsNameIdMap.get( COUNTRY_LABEL );
            Integer carLabelId = labelsNameIdMap.get( CAR_LABEL );
            try ( IndexReader indexReader = getIndexReader( propertyId, countryLabelId ) )
            {
                assertThat( indexReader.countIndexedNodes( otherNodes[0].getId(), NULL, new int[]{propertyId}, Values.of( "Denmark" ) ) )
                        .as( "Should be added by concurrent add." ).isEqualTo( 1 );
            }

            try ( IndexReader indexReader = getIndexReader( propertyId, carLabelId ) )
            {
                assertThat( indexReader.countIndexedNodes( otherNodes[1].getId(), NULL, new int[]{propertyId}, Values.of( "BMW" ) ) )
                        .as( "Should be added by concurrent add." ).isEqualTo( 1 );
            }
        }
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void applyConcurrentChangesToPopulatedIndex( GraphDatabaseSettings.SchemaIndex schemaIndex ) throws Throwable
    {
        List<EntityUpdates> updates = new ArrayList<>( 2 );
        updates.add( EntityUpdates.forEntity( color2.getId(), false ).withTokens( id( COLOR_LABEL ) )
                .changed( propertyId, Values.of( "green" ), Values.of( "pink" ) ).build() );
        updates.add( EntityUpdates.forEntity( car2.getId(), false ).withTokens( id( CAR_LABEL ) )
                .changed( propertyId, Values.of( "Ford" ), Values.of( "SAAB" ) ).build() );

        launchCustomIndexPopulation( schemaIndex, labelsNameIdMap, propertyId, new UpdateGenerator( updates ) );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );

        try ( Transaction tx = db.beginTx() )
        {
            Integer colorLabelId = labelsNameIdMap.get( COLOR_LABEL );
            Integer carLabelId = labelsNameIdMap.get( CAR_LABEL );
            try ( IndexReader indexReader = getIndexReader( propertyId, colorLabelId ) )
            {
                assertThat( indexReader.countIndexedNodes( color2.getId(), NULL, new int[]{propertyId}, Values.of( "green" ) ) )
                        .as( format( "Should be deleted by concurrent change. Reader is: %s, ", indexReader ) ).isEqualTo( 0 );
            }
            try ( IndexReader indexReader = getIndexReader( propertyId, colorLabelId ) )
            {
                assertThat( indexReader.countIndexedNodes( color2.getId(), NULL, new int[]{propertyId}, Values.of( "pink" ) ) )
                        .as( "Should be updated by concurrent change." ).isEqualTo( 1 );
            }

            try ( IndexReader indexReader = getIndexReader( propertyId, carLabelId ) )
            {
                assertThat( indexReader.countIndexedNodes( car2.getId(), NULL, new int[]{propertyId}, Values.of( "SAAB" ) ) )
                        .as( "Should be added by concurrent change." ).isEqualTo( 1 );
            }
        }
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void dropOneOfTheIndexesWhilePopulationIsOngoingDoesInfluenceOtherPopulators( GraphDatabaseSettings.SchemaIndex schemaIndex ) throws Throwable
    {
        launchCustomIndexPopulation( schemaIndex, labelsNameIdMap, propertyId,
                new IndexDropAction( labelsNameIdMap.get( COLOR_LABEL ) ) );
        labelsNameIdMap.remove( COLOR_LABEL );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );

        checkIndexIsOnline( labelsNameIdMap.get( CAR_LABEL ) );
        checkIndexIsOnline( labelsNameIdMap.get( COUNTRY_LABEL ));
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void indexDroppedDuringPopulationDoesNotExist( GraphDatabaseSettings.SchemaIndex schemaIndex ) throws Throwable
    {
        Integer labelToDropId = labelsNameIdMap.get( COLOR_LABEL );
        launchCustomIndexPopulation( schemaIndex, labelsNameIdMap, propertyId, new IndexDropAction( labelToDropId ) );
        labelsNameIdMap.remove( COLOR_LABEL );
        waitAndActivateIndexes( labelsNameIdMap, propertyId );

        assertThrows( IndexNotFoundKernelException.class, () ->
                {
                    Iterator<IndexDescriptor> iterator = schemaCache.indexesForSchema( SchemaDescriptor.forLabel( labelToDropId, propertyId ) );
                    while ( iterator.hasNext() )
                    {
                        IndexDescriptor index = iterator.next();
                        indexService.getIndexProxy( index );
                    }
                });
    }

    private void checkIndexIsOnline( int labelId ) throws IndexNotFoundKernelException
    {
        LabelSchemaDescriptor schema = SchemaDescriptor.forLabel( labelId, propertyId );
        IndexDescriptor index = single( schemaCache.indexesForSchema( schema ) );
        IndexProxy indexProxy = indexService.getIndexProxy( index );
        assertSame( InternalIndexState.ONLINE, indexProxy.getState() );
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

    private void launchCustomIndexPopulation( GraphDatabaseSettings.SchemaIndex schemaIndex, Map<String,Integer> labelNameIdMap, int propertyId,
            Runnable customAction ) throws Throwable
    {
        RecordStorageEngine storageEngine = getStorageEngine();
        LabelScanStore labelScanStore = getLabelScanStore();
        RelationshipTypeScanStore relationshipTypeScanStore = getRelationshipTypeScanStore();

        try ( Transaction transaction = db.beginTx() )
        {
            Config config = Config.defaults();
            KernelTransaction ktx = ((InternalTransaction) transaction).kernelTransaction();
            DynamicIndexStoreView storeView =
                    dynamicIndexStoreViewWrapper( customAction, storageEngine::newReader, labelScanStore, relationshipTypeScanStore, config );

            IndexProviderMap providerMap = getIndexProviderMap();
            JobScheduler scheduler = getJobScheduler();

            NullLogProvider nullLogProvider = NullLogProvider.getInstance();
            indexService = IndexingServiceFactory.createIndexingService( config, scheduler,
                    providerMap, storeView, ktx.tokenRead(), initialSchemaRulesLoader( storageEngine ),
                    nullLogProvider, nullLogProvider, IndexingService.NO_MONITOR, getSchemaState(),
                    mock( IndexStatisticsStore.class ), PageCacheTracer.NULL, INSTANCE, "", false );
            indexService.start();

            rules = createIndexRules( schemaIndex, labelNameIdMap, propertyId );
            schemaCache = new SchemaCache( new StandardConstraintSemantics(), providerMap );
            schemaCache.load( iterable( rules ) );

            indexService.createIndexes( AUTH_DISABLED, rules );
            transaction.commit();
        }
    }

    private DynamicIndexStoreView dynamicIndexStoreViewWrapper( Runnable customAction,
            Supplier<StorageReader> readerSupplier, LabelScanStore labelScanStore,
            RelationshipTypeScanStore relationshipTypeScanStore, Config config )
    {
        LockService locks = LockService.NO_LOCK_SERVICE;
        NeoStoreIndexStoreView neoStoreIndexStoreView = new NeoStoreIndexStoreView( locks, readerSupplier );
        return new DynamicIndexStoreViewWrapper( neoStoreIndexStoreView, labelScanStore, relationshipTypeScanStore, locks, readerSupplier, customAction,
                config );
    }

    private void waitAndActivateIndexes( Map<String,Integer> labelsIds, int propertyId )
            throws IndexNotFoundKernelException, IndexPopulationFailedKernelException, InterruptedException,
            IndexActivationFailedKernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int labelId : labelsIds.values() )
            {
                waitIndexOnline( indexService, propertyId, labelId );
            }
        }
    }

    private int getPropertyId()
    {
        try ( Transaction tx = db.beginTx() )
        {
            return getPropertyIdByName( tx, NAME_PROPERTY );
        }
    }

    private Map<String,Integer> getLabelsNameIdMap()
    {
        try ( Transaction tx = db.beginTx() )
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

    private IndexDescriptor[] createIndexRules( GraphDatabaseSettings.SchemaIndex schemaIndex, Map<String,Integer> labelNameIdMap, int propertyId )
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
        try ( Transaction transaction = db.beginTx() )
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
        return db.getDependencyResolver().resolveDependency( LabelScanStore.class );
    }

    private RelationshipTypeScanStore getRelationshipTypeScanStore()
    {
        return db.getDependencyResolver().resolveDependency( RelationshipTypeScanStore.class );
    }

    private RecordStorageEngine getStorageEngine()
    {
        return db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
    }

    private SchemaState getSchemaState()
    {
        return db.getDependencyResolver().resolveDependency( SchemaState.class );
    }

    private IndexProviderMap getIndexProviderMap()
    {
        return db.getDependencyResolver().resolveDependency( IndexProviderMap.class );
    }

    private JobScheduler getJobScheduler()
    {
        return db.getDependencyResolver().resolveDependency( JobScheduler.class );
    }

    private class DynamicIndexStoreViewWrapper extends DynamicIndexStoreView
    {
        private final Runnable customAction;

        DynamicIndexStoreViewWrapper( NeoStoreIndexStoreView neoStoreIndexStoreView, LabelScanStore labelScanStore,
                RelationshipTypeScanStore relationshipTypeScanStore, LockService locks,
                Supplier<StorageReader> storageEngine, Runnable customAction, Config config )
        {
            super( neoStoreIndexStoreView, labelScanStore, relationshipTypeScanStore, locks, storageEngine, NullLogProvider.getInstance(), config );
            this.customAction = customAction;
        }

        @Override
        public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds,
                IntPredicate propertyKeyIdFilter,
                Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor,
                Visitor<EntityTokenUpdate,FAILURE> labelUpdateVisitor,
                boolean forceStoreScan, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
        {
            StoreScan<FAILURE> storeScan = super.visitNodes( labelIds, propertyKeyIdFilter, propertyUpdatesVisitor,
                    labelUpdateVisitor, forceStoreScan, cursorTracer, memoryTracker );
            return new LabelViewNodeStoreWrapper<>( storageEngine.get(), locks, getLabelScanStore(),
                    element -> false, propertyUpdatesVisitor, labelIds, propertyKeyIdFilter,
                    (LabelViewNodeStoreScan<FAILURE>) storeScan, customAction );
        }
    }

    private static class LabelViewNodeStoreWrapper<FAILURE extends Exception> extends LabelViewNodeStoreScan<FAILURE>
    {
        private final LabelViewNodeStoreScan<FAILURE> delegate;
        private final Runnable customAction;

        LabelViewNodeStoreWrapper( StorageReader storageReader, LockService locks,
                LabelScanStore labelScanStore, Visitor<EntityTokenUpdate,FAILURE> labelUpdateVisitor,
                Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor, int[] labelIds, IntPredicate propertyKeyIdFilter,
                LabelViewNodeStoreScan<FAILURE> delegate,
                Runnable customAction )
        {
            super( storageReader, locks, labelScanStore, labelUpdateVisitor, propertyUpdatesVisitor, labelIds, propertyKeyIdFilter, NULL, INSTANCE );
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
        private final Iterable<EntityUpdates> updates;

        UpdateGenerator( Iterable<EntityUpdates> updates )
        {
            this.updates = updates;
        }

        @Override
        public void run()
        {
            for ( EntityUpdates update : updates )
                {
                    try ( Transaction transaction = db.beginTx() )
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
                        Iterable<IndexEntryUpdate<IndexDescriptor>> entryUpdates = update.forIndexKeys( relatedIndexes, reader, EntityType.NODE, NULL,
                                INSTANCE );
                        indexService.applyUpdates( entryUpdates, NULL );
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
        private final int labelIdToDropIndexFor;

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
