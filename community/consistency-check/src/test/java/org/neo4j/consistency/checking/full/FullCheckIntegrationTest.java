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
package org.neo4j.consistency.checking.full;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.GraphStoreFixture;
import org.neo4j.consistency.checking.GraphStoreFixture.Applier;
import org.neo4j.consistency.checking.GraphStoreFixture.IdGenerator;
import org.neo4j.consistency.checking.GraphStoreFixture.TransactionDataBuilder;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.direct.DirectStoreAccess;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.NodeUpdates;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.allocator.ReusableRecordsAllocator;
import org.neo4j.kernel.impl.store.allocator.ReusableRecordsCompositeAllocator;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLog;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.string.UTF8;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.consistency.ConsistencyCheckService.defaultConsistencyCheckThreadsNumber;
import static org.neo4j.consistency.checking.RecordCheckTestBase.inUse;
import static org.neo4j.consistency.checking.RecordCheckTestBase.notInUse;
import static org.neo4j.consistency.checking.SchemaRuleUtil.constraintIndexRule;
import static org.neo4j.consistency.checking.SchemaRuleUtil.indexRule;
import static org.neo4j.consistency.checking.SchemaRuleUtil.nodePropertyExistenceConstraintRule;
import static org.neo4j.consistency.checking.SchemaRuleUtil.relPropertyExistenceConstraintRule;
import static org.neo4j.consistency.checking.SchemaRuleUtil.uniquenessConstraintRule;
import static org.neo4j.consistency.checking.full.FullCheckIntegrationTest.ConsistencySummaryVerifier.on;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.helpers.collection.Iterables.asIterable;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.ReadOperations.ANY_LABEL;
import static org.neo4j.kernel.api.ReadOperations.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;
import static org.neo4j.kernel.api.schema.index.IndexDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.schema.index.IndexDescriptorFactory.uniqueForLabel;
import static org.neo4j.kernel.impl.store.AbstractDynamicStore.readFullByteArrayFromHeavyRecords;
import static org.neo4j.kernel.impl.store.DynamicArrayStore.allocateFromNumbers;
import static org.neo4j.kernel.impl.store.DynamicArrayStore.getRightArray;
import static org.neo4j.kernel.impl.store.DynamicNodeLabels.dynamicPointer;
import static org.neo4j.kernel.impl.store.LabelIdArray.prependNodeId;
import static org.neo4j.kernel.impl.store.PropertyType.ARRAY;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.record.Record.NO_PREV_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.kernel.impl.util.Bits.bits;
import static org.neo4j.test.Property.property;
import static org.neo4j.test.Property.set;

public class FullCheckIntegrationTest
{
    private static final SchemaIndexProvider.Descriptor DESCRIPTOR = new SchemaIndexProvider.Descriptor( "lucene", "1.0" );
    private static final String PROP1 = "key1";
    private static final String PROP2 = "key2";
    private static final Object VALUE1 = "value1";
    private static final Object VALUE2 = "value2";

    private int label1;
    private int label2;
    private int label3;
    private int label4;
    private int  draconian;
    private int key1;
    private int key2;
    private int mandatory;
    private int C;
    private int T;
    private int M;

    private final List<Long> indexedNodes = new ArrayList<>();

    private static final Map<Class<?>,Set<String>> allReports = new HashMap<>();

    @BeforeClass
    public static void collectAllDifferentInconsistencyTypes()
    {
        Class<?> reportClass = ConsistencyReport.class;
        for ( Class<?> cls : reportClass.getDeclaredClasses() )
        {
            for ( Method method : cls.getDeclaredMethods() )
            {
                if ( method.getAnnotation( Documented.class ) != null )
                {
                    Set<String> types = allReports.computeIfAbsent( cls, k -> new HashSet<>() );
                    types.add( method.getName() );
                }
            }
        }
    }

    @AfterClass
    public static void verifyThatWeHaveExercisedAllTypesOfInconsistenciesThatWeHave()
    {
        if ( !allReports.isEmpty() )
        {
            StringBuilder builder = new StringBuilder( "There are types of inconsistencies not covered by "
                    + "this integration test, please add tests that tests for:" );
            for ( Map.Entry<Class<?>,Set<String>> reporter : allReports.entrySet() )
            {
                builder.append( format( "%n%s:", reporter.getKey().getSimpleName() ) );
                for ( String type : reporter.getValue() )
                {
                    builder.append( format( "%n  %s", type ) );
                }
            }
            System.err.println( builder.toString() );
        }
    }

    private final GraphStoreFixture fixture = new GraphStoreFixture( getRecordFormatName() )
    {
        @Override
        protected void generateInitialData( GraphDatabaseService db )
        {
            try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
            {
                db.schema().indexFor( label( "label3" ) ).on( PROP1 ).create();

                try ( KernelStatement statement = statementOn( db ) )
                {
                    // the Core API for composite index creation is not quite merged yet
                    TokenWriteOperations tokenWriteOperations = statement.tokenWriteOperations();
                    key1 = tokenWriteOperations.propertyKeyGetOrCreateForName( PROP1 );
                    key2 = tokenWriteOperations.propertyKeyGetOrCreateForName( PROP2 );
                    label3 = statement.readOperations().labelGetForName( "label3" );
                    statement.schemaWriteOperations()
                            .indexCreate( SchemaDescriptorFactory.forLabel( label3, key1, key2 ) );
                }

                db.schema().constraintFor( label( "label4" ) ).assertPropertyIsUnique( PROP1 ).create();
                tx.success();
            }
            catch ( KernelException e )
            {
                throw new RuntimeException( e );
            }

            try ( org.neo4j.graphdb.Transaction ignored = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            }

            try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
            {
                Node node1 = set( db.createNode( label( "label1" ) ) );
                Node node2 = set( db.createNode( label( "label2" ) ), property( PROP1, VALUE1 ) );
                node1.createRelationshipTo( node2, withName( "C" ) );
                // Just to create one more rel type
                db.createNode().createRelationshipTo( db.createNode(), withName( "T" ) );
                indexedNodes.add( set( db.createNode( label( "label3" ) ), property( PROP1, VALUE1 ) ).getId() );
                indexedNodes.add( set( db.createNode( label( "label3" ) ),
                        property( PROP1, VALUE1 ), property( PROP2, VALUE2 ) ).getId() );

                set( db.createNode( label( "label4" ) ), property( PROP1, VALUE1 ) );
                tx.success();

                try ( KernelStatement statement = statementOn( db ) )
                {
                    ReadOperations readOperations = statement.readOperations();
                    TokenWriteOperations tokenWriteOperations = statement.tokenWriteOperations();
                    label1 = readOperations.labelGetForName( "label1" );
                    label2 = readOperations.labelGetForName( "label2" );
                    label3 = readOperations.labelGetForName( "label3" );
                    label4 = readOperations.labelGetForName( "label4" );
                    draconian = tokenWriteOperations.labelGetOrCreateForName( "draconian" );
                    key1 = readOperations.propertyKeyGetForName( PROP1 );
                    mandatory = tokenWriteOperations.propertyKeyGetOrCreateForName( "mandatory" );
                    C = readOperations.relationshipTypeGetForName( "C" );
                    T = readOperations.relationshipTypeGetForName( "T" );
                    M = tokenWriteOperations.relationshipTypeGetOrCreateForName( "M" );
                }
            }
            catch ( KernelException e )
            {
                throw new RuntimeException( e );
            }
        }
    };

    private final SuppressOutput suppressOutput = SuppressOutput.suppress( SuppressOutput.System.out );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( suppressOutput ).around( fixture );

    @Test
    public void shouldCheckConsistencyOfAConsistentStore() throws Exception
    {
        // when
        ConsistencySummaryStatistics result = check();

        // then
        assertEquals( result.toString(), 0, result.getTotalInconsistencyCount() );
    }

    @Test
    public void shouldReportNodeInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                tx.create( new NodeRecord( next.node(), false, next.relationship(), -1 ) );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.NODE, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportInlineNodeLabelInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                NodeRecord nodeRecord = new NodeRecord( next.node(), false, -1, -1 );
                NodeLabelsField.parseLabelsField( nodeRecord ).add( 10, null, null );
                tx.create( nodeRecord );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.NODE, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportNodeDynamicLabelContainingUnknownLabelAsNodeInconsistency() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                NodeRecord nodeRecord = new NodeRecord( next.node(), false, -1, -1 );
                DynamicRecord record = inUse( new DynamicRecord( next.nodeLabel() ) );
                Collection<DynamicRecord> newRecords = new ArrayList<>();
                allocateFromNumbers( newRecords, prependNodeId( nodeRecord.getId(), new long[]{42L} ),
                        new ReusableRecordsAllocator( 60, record ) );
                nodeRecord.setLabelField( dynamicPointer( newRecords ), newRecords );

                tx.create( nodeRecord );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.NODE, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldNotReportAnythingForNodeWithConsistentChainOfDynamicRecordsWithLabels() throws Exception
    {
        // given
        assertEquals( 3, chainOfDynamicRecordsWithLabelsForANode( 130 ).first().size() );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        assertTrue( "should be consistent", stats.isConsistent() );
    }

    @Test
    public void shouldReportLabelScanStoreInconsistencies() throws Exception
    {
        // given
        GraphStoreFixture.IdGenerator idGenerator = fixture.idGenerator();
        long nodeId1 = idGenerator.node();
        long labelId = idGenerator.label() - 1;

        LabelScanStore labelScanStore = fixture.directStoreAccess().labelScanStore();
        Iterable<NodeLabelUpdate> nodeLabelUpdates = asIterable(
                labelChanges( nodeId1, new long[]{}, new long[]{labelId} )
        );
        write( labelScanStore, nodeLabelUpdates );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.LABEL_SCAN_DOCUMENT, 1 )
                   .andThatsAllFolks();
    }

    private void write( LabelScanStore labelScanStore, Iterable<NodeLabelUpdate> nodeLabelUpdates )
            throws IOException
    {
        try ( LabelScanWriter writer = labelScanStore.newWriter() )
        {
            for ( NodeLabelUpdate update : nodeLabelUpdates )
            {
                writer.write( update );
            }
        }
    }

    @Test
    public void shouldReportIndexInconsistencies() throws Exception
    {
        // given
        for ( Long indexedNodeId : indexedNodes )
        {
            fixture.directStoreAccess().nativeStores().getNodeStore().updateRecord(
                    notInUse( new NodeRecord( indexedNodeId, false, -1, -1 ) ) );
        }

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.INDEX, 3 ) // 3 index entries are pointing to nodes not in use
                   .verify( RecordType.LABEL_SCAN_DOCUMENT, 2 ) // the label scan is pointing to 2 nodes not in use
                   .verify( RecordType.COUNTS, 3 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldNotReportIndexInconsistenciesIfIndexIsFailed() throws Exception
    {
        // this test fails all indexes, and then destroys a record and makes sure we only get a failure for
        // the label scan store but not for any index

        // given
        DirectStoreAccess storeAccess = fixture.directStoreAccess();

        // fail all indexes
        Iterator<IndexRule> rules = new SchemaStorage( storeAccess.nativeStores().getSchemaStore() ).indexesGetAll();
        while ( rules.hasNext() )
        {
            IndexRule rule = rules.next();
            IndexSamplingConfig samplingConfig = new IndexSamplingConfig( Config.defaults() );
            IndexPopulator populator = storeAccess.indexes().apply( rule.getProviderDescriptor() )
                .getPopulator( rule.getId(), rule.getIndexDescriptor(), samplingConfig );
            populator.markAsFailed( "Oh noes! I was a shiny index and then I was failed" );
            populator.close( false );

        }

        for ( Long indexedNodeId : indexedNodes )
        {
            storeAccess.nativeStores().getNodeStore().updateRecord(
                    notInUse( new NodeRecord( indexedNodeId, false, -1, -1 ) ) );
        }

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.LABEL_SCAN_DOCUMENT, 2 ) // the label scan is pointing to 2 nodes not in use
                   .verify( RecordType.COUNTS, 3 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportMismatchedLabels() throws Exception
    {
        final List<Integer> labels = new ArrayList<>();

        // given
        final Pair<List<DynamicRecord>, List<Integer>> pair = chainOfDynamicRecordsWithLabelsForANode( 3 );
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                NodeRecord node = new NodeRecord( 42, false, -1, -1 );
                node.setInUse( true );
                List<DynamicRecord> dynamicRecords;
                dynamicRecords = pair.first();
                labels.addAll( pair.other() );
                node.setLabelField( dynamicPointer( dynamicRecords ), dynamicRecords );
                tx.create( node );

            }
        } );

        long[] before = asArray( labels );
        labels.remove( 1 );
        long[] after = asArray( labels );

        write( fixture.directStoreAccess().labelScanStore(), asList( labelChanges( 42, before, after ) ) );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.LABEL_SCAN_DOCUMENT, 1 )
                   .andThatsAllFolks();
    }

    private long[] asArray( List<? extends Number> in )
    {
        long[] longs = new long[in.size()];
        for ( int i = 0; i < in.size(); i++ )
        {
            longs[i] = in.get( i ).longValue();
        }
        return longs;
    }

    private PrimitiveLongSet asPrimitiveLongSet( List<? extends Number> in )
    {
        return PrimitiveLongCollections.setOf( asArray( in ) );
    }

    @Test
    public void shouldReportMismatchedInlinedLabels() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                NodeRecord node = new NodeRecord( 42, false, -1, -1 );
                node.setInUse( true );
                node.setLabelField( inlinedLabelsLongRepresentation( label1, label2 ), Collections.emptySet() );
                tx.create( node );
            }
        } );

        write( fixture.directStoreAccess().labelScanStore(), asList( labelChanges( 42, new long[]{label1, label2}, new long[]{label1} ) ) );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.LABEL_SCAN_DOCUMENT, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportNodesThatAreNotIndexed() throws Exception
    {
        // given
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( Config.defaults() );
        Iterator<IndexRule> indexRuleIterator =
                new SchemaStorage( fixture.directStoreAccess().nativeStores().getSchemaStore() ).indexesGetAll();
        NeoStoreIndexStoreView storeView = new NeoStoreIndexStoreView( LockService.NO_LOCK_SERVICE,
                fixture.directStoreAccess().nativeStores().getRawNeoStores() );
        while ( indexRuleIterator.hasNext() )
        {
            IndexRule indexRule = indexRuleIterator.next();
            IndexDescriptor descriptor = indexRule.getIndexDescriptor();
            IndexAccessor accessor = fixture.directStoreAccess().indexes().
                    apply( indexRule.getProviderDescriptor() ).getOnlineAccessor(
                            indexRule.getId(), descriptor, samplingConfig );
            try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
            {
                for ( long nodeId : indexedNodes )
                {
                    NodeUpdates updates = storeView.nodeAsUpdates( nodeId );
                    for ( IndexEntryUpdate<?> update : updates.forIndexKeys( asList( descriptor ) ) )
                    {
                        updater.process( IndexEntryUpdate.remove( nodeId, descriptor, update.values() ) );
                    }
                }
            }
            accessor.force();
            accessor.close();
        }

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.NODE, 3 ) // 1 node missing from 1 index + 1 node missing from 2 indexes
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportNodesWithDuplicatePropertyValueInUniqueIndex() throws Exception
    {
        // given
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( Config.defaults() );
        Iterator<IndexRule> indexRuleIterator =
                new SchemaStorage( fixture.directStoreAccess().nativeStores().getSchemaStore() ).indexesGetAll();
        while ( indexRuleIterator.hasNext() )
        {
            IndexRule indexRule = indexRuleIterator.next();
            IndexAccessor accessor = fixture.directStoreAccess().indexes().apply( indexRule.getProviderDescriptor() )
                    .getOnlineAccessor( indexRule.getId(), indexRule.getIndexDescriptor(), samplingConfig );
            IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE );
            updater.process( IndexEntryUpdate.add( 42, indexRule.getIndexDescriptor().schema(), values( indexRule ) ) );
            updater.close();
            accessor.force();
            accessor.close();
        }

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.NODE, 1 ) // the duplicate in unique index
                   .verify( RecordType.INDEX, 3 ) // the index entries pointing to non-existent node 42
                   .andThatsAllFolks();
    }

    private Value[] values( IndexRule indexRule )
    {
        switch ( indexRule.schema().getPropertyIds().length )
        {
        case 1: return Iterators.array( Values.of( VALUE1 ) );
        case 2: return Iterators.array( Values.of( VALUE1 ), Values.of( VALUE2 ) );
        default: throw new UnsupportedOperationException();
        }
    }

    @Test
    public void shouldReportMissingMandatoryNodeProperty() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                // structurally correct, but does not have the 'mandatory' property with the 'draconian' label
                NodeRecord node = new NodeRecord( next.node(), false, -1, next.property() );
                node.setInUse( true );
                node.setLabelField( inlinedLabelsLongRepresentation( draconian ),
                        Collections.emptySet() );
                PropertyRecord property = new PropertyRecord( node.getNextProp(), node );
                property.setInUse( true );
                PropertyBlock block = new PropertyBlock();
                block.setSingleBlock( key1 | (((long) PropertyType.INT.intValue()) << 24) | (1337L << 28) );
                property.addPropertyBlock( block );
                tx.create( node );
                tx.create( property );
            }
        } );

        createNodePropertyExistenceConstraint( draconian, mandatory );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.NODE, 1 )
                .andThatsAllFolks();
    }

    @Test
    public void shouldReportMissingMandatoryRelationshipProperty() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                long nodeId1 = next.node();
                long nodeId2 = next.node();
                long relId = next.relationship();
                long propId = next.property();

                NodeRecord node1 = new NodeRecord( nodeId1, true, false, relId, NO_NEXT_PROPERTY.intValue(),
                        NO_LABELS_FIELD.intValue() );
                NodeRecord node2 = new NodeRecord( nodeId2, true, false, relId, NO_NEXT_PROPERTY.intValue(),
                        NO_LABELS_FIELD.intValue() );

                // structurally correct, but does not have the 'mandatory' property with the 'M' rel type
                RelationshipRecord relationship = new RelationshipRecord( relId, true, nodeId1, nodeId2, M,
                        NO_PREV_RELATIONSHIP.intValue(), NO_NEXT_RELATIONSHIP.intValue(),
                        NO_PREV_RELATIONSHIP.intValue(), NO_NEXT_RELATIONSHIP.intValue(), true, true );
                relationship.setNextProp( propId );

                PropertyRecord property = new PropertyRecord( propId, relationship );
                property.setInUse( true );
                PropertyBlock block = new PropertyBlock();
                block.setSingleBlock( key1 | (((long) PropertyType.INT.intValue()) << 24) | (1337L << 28) );
                property.addPropertyBlock( block );

                tx.create( node1 );
                tx.create( node2 );
                tx.create( relationship );
                tx.create( property );
                tx.incrementRelationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL, 1 );
                tx.incrementRelationshipCount( ANY_LABEL, M, ANY_LABEL, 1 );
            }
        } );

        createRelationshipPropertyExistenceConstraint( M, mandatory );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP, 1 )
                .andThatsAllFolks();
    }

    private long inlinedLabelsLongRepresentation( long... labelIds )
    {
        long header = (long) labelIds.length << 36;
        byte bitsPerLabel = (byte) (36 / labelIds.length);
        Bits bits = bits( 5 );
        for ( long labelId : labelIds )
        {
            bits.put( labelId, bitsPerLabel );
        }
        return header | bits.getLongs()[0];
    }

    @Test
    public void shouldReportCyclesInDynamicRecordsWithLabels() throws Exception
    {
        // given
        final List<DynamicRecord> chain = chainOfDynamicRecordsWithLabelsForANode( 176/*3 full records*/ ).first();
        assertEquals( "number of records in chain", 3, chain.size() );
        assertEquals( "all records full", chain.get( 0 ).getLength(), chain.get( 2 ).getLength() );
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                long nodeId = ((long[]) getRightArray( readFullByteArrayFromHeavyRecords( chain, ARRAY ) ).asObject())[0];
                NodeRecord before = inUse( new NodeRecord( nodeId, false, -1, -1 ) );
                NodeRecord after = inUse( new NodeRecord( nodeId, false, -1, -1 ) );
                DynamicRecord record1 = chain.get( 0 ).clone();
                DynamicRecord record2 = chain.get( 1 ).clone();
                DynamicRecord record3 = chain.get( 2 ).clone();

                record3.setNextBlock( record2.getId() );
                before.setLabelField( dynamicPointer( chain ), chain );
                after.setLabelField( dynamicPointer( chain ), asList( record1, record2, record3 ) );
                tx.update( before, after );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.NODE, 1 )
                   .verify( RecordType.COUNTS, 177 )
                   .andThatsAllFolks();
    }

    private Pair<List<DynamicRecord>,List<Integer>> chainOfDynamicRecordsWithLabelsForANode( int labelCount )
            throws TransactionFailureException
    {
        final long[] labels = new long[labelCount + 1]; // allocate enough labels to need three records
        final List<Integer> createdLabels = new ArrayList<>();
        try ( Applier applier = fixture.createApplier() )
        {
            for ( int i = 1/*leave space for the node id*/; i < labels.length; i++ )
            {
                final int offset = i;
                applier.apply( new GraphStoreFixture.Transaction()
                { // Neo4j can create no more than one label per transaction...
                    @Override
                    protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                            GraphStoreFixture.IdGenerator next )
                    {
                        Integer label = next.label();
                        tx.nodeLabel( (int) (labels[offset] = label), "label:" + offset );
                        createdLabels.add( label );
                    }
                } );
            }
        }
        final List<DynamicRecord> chain = new ArrayList<>();
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                NodeRecord nodeRecord = new NodeRecord( next.node(), false, -1, -1 );
                DynamicRecord record1 = inUse( new DynamicRecord( next.nodeLabel() ) );
                DynamicRecord record2 = inUse( new DynamicRecord( next.nodeLabel() ) );
                DynamicRecord record3 = inUse( new DynamicRecord( next.nodeLabel() ) );
                labels[0] = nodeRecord.getId(); // the first id should not be a label id, but the id of the node
                ReusableRecordsAllocator allocator = new ReusableRecordsAllocator( 60, record1, record2, record3 );
                allocateFromNumbers( chain, labels, allocator );

                nodeRecord.setLabelField( dynamicPointer( chain ), chain );

                tx.create( nodeRecord );
            }
        } );
        return Pair.of( chain, createdLabels );
    }

    @Test
    public void shouldReportNodeDynamicLabelContainingDuplicateLabelAsNodeInconsistency() throws Exception
    {
        int nodeId = 1000;
        Collection<DynamicRecord> duplicatedLabel = new ArrayList<>();
        final Pair<List<DynamicRecord>, List<Integer>> labels = chainOfDynamicRecordsWithLabelsForANode( 1 );

        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                NodeRecord node = new NodeRecord( nodeId, false, -1, -1 );
                node.setInUse( true );
                List<DynamicRecord> labelRecords = labels.first();
                node.setLabelField( dynamicPointer( labelRecords ), labelRecords );
                tx.create( node );

                Integer labelId = labels.other().get( 0 );
                DynamicRecord record = inUse( new DynamicRecord( labelId ) );
                allocateFromNumbers( duplicatedLabel, new long[]{nodeId, labelId, labelId}, new ReusableRecordsAllocator( 60, record ) );
            }
        } );

        StoreAccess storeAccess = fixture.directStoreAccess().nativeStores();
        NodeRecord nodeRecord = new NodeRecord( nodeId );
        storeAccess.getNodeStore().getRecord( nodeId, nodeRecord, FORCE );
        nodeRecord.setLabelField( dynamicPointer( duplicatedLabel ), duplicatedLabel );
        nodeRecord.setInUse( true );
        storeAccess.getNodeStore().updateRecord( nodeRecord );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.NODE, 1 ) // the duplicated label
                   .verify( RecordType.COUNTS, 0 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportOrphanedNodeDynamicLabelAsNodeInconsistency() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                tx.nodeLabel( 42, "Label" );

                NodeRecord nodeRecord = new NodeRecord( next.node(), false, -1, -1 );
                DynamicRecord record = inUse( new DynamicRecord( next.nodeLabel() ) );
                Collection<DynamicRecord> newRecords = new ArrayList<>();
                allocateFromNumbers( newRecords, prependNodeId( next.node(), new long[]{42L} ),
                        new ReusableRecordsAllocator( 60, record ) );
                nodeRecord.setLabelField( dynamicPointer( newRecords ), newRecords );

                tx.create( nodeRecord );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.NODE_DYNAMIC_LABEL, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportRelationshipInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                tx.create( new RelationshipRecord( next.relationship(), 1, 2, C ) );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP, 2 )
                   .verify( RecordType.COUNTS, 3 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportRelationshipOtherNodeInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                long node1 = next.node();
                long node2 = next.node();
                long rel = next.relationship();
                tx.create( inUse( new RelationshipRecord( rel, node1, node2, 0 ) ) );
                tx.create( inUse( new NodeRecord( node1, false, rel + 1, -1 ) ) );
                tx.create( inUse( new NodeRecord( node2, false, rel + 2, -1 ) ) );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP, 2 )
                   .verify( RecordType.NODE, 2 )
                   .verify( RecordType.COUNTS, 2 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportPropertyInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                NodeRecord node = new NodeRecord( next.node() );
                PropertyRecord property = new PropertyRecord( next.property() );
                node.setNextProp( property.getId() );

                // Mess up the prev/next pointers a bit
                property.setNextProp( 1_000 );

                PropertyBlock block = new PropertyBlock();
                block.setSingleBlock( next.propertyKey() | (((long) PropertyType.INT.intValue()) << 24) | (666L << 28) );
                property.addPropertyBlock( block );
                tx.create( node );
                tx.create( property );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.PROPERTY, 2 )
                   .verify( RecordType.NODE, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportStringPropertyInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                DynamicRecord string = new DynamicRecord( next.stringProperty() );
                string.setInUse( true );
                string.setCreated();
                string.setType( PropertyType.STRING.intValue() );
                string.setNextBlock( next.stringProperty() );
                string.setData( UTF8.encode( "hello world" ) );

                PropertyBlock block = new PropertyBlock();
                block.setSingleBlock( (((long) PropertyType.STRING.intValue()) << 24) | (string.getId() << 28) );
                block.addValueRecord( string );

                PropertyRecord property = new PropertyRecord( next.property() );
                property.addPropertyBlock( block );

                tx.create( property );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.STRING_PROPERTY, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportBrokenSchemaRecordChain() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                DynamicRecord schema = new DynamicRecord( next.schema() );
                DynamicRecord schemaBefore = schema.clone();

                schema.setNextBlock( next.schema() ); // Point to a record that isn't in use.
                IndexRule rule = indexRule( schema.getId(), label1, key1, DESCRIPTOR );
                schema.setData( rule.serialize() );

                tx.createSchema( asList( schemaBefore ), asList( schema ), rule );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.SCHEMA, 3 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportDuplicateConstraintReferences() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                int ruleId1 = (int) next.schema();
                int ruleId2 = (int) next.schema();
                int labelId = next.label();
                int propertyKeyId = next.propertyKey();

                DynamicRecord record1 = new DynamicRecord( ruleId1 );
                DynamicRecord record2 = new DynamicRecord( ruleId2 );
                DynamicRecord record1Before = record1.clone();
                DynamicRecord record2Before = record2.clone();

                IndexRule rule1 = constraintIndexRule( ruleId1, labelId, propertyKeyId, DESCRIPTOR, ruleId1 );
                IndexRule rule2 = constraintIndexRule( ruleId2, labelId, propertyKeyId, DESCRIPTOR, ruleId1 );

                Collection<DynamicRecord> records1 = serializeRule( rule1, record1 );
                Collection<DynamicRecord> records2 = serializeRule( rule2, record2 );

                assertEquals( asList( record1 ), records1 );
                assertEquals( asList( record2 ), records2 );

                tx.nodeLabel( labelId, "label" );
                tx.propertyKey( propertyKeyId, "property" );

                tx.createSchema( asList(record1Before), records1, rule1 );
                tx.createSchema( asList(record2Before), records2, rule2 );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.SCHEMA, 4 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportInvalidConstraintBackReferences() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                int ruleId1 = (int) next.schema();
                int ruleId2 = (int) next.schema();
                int labelId = next.label();
                int propertyKeyId = next.propertyKey();

                DynamicRecord record1 = new DynamicRecord( ruleId1 );
                DynamicRecord record2 = new DynamicRecord( ruleId2 );
                DynamicRecord record1Before = record1.clone();
                DynamicRecord record2Before = record2.clone();

                IndexRule rule1 = constraintIndexRule( ruleId1, labelId, propertyKeyId, DESCRIPTOR, ruleId2 );
                ConstraintRule rule2 = uniquenessConstraintRule( ruleId2, labelId, propertyKeyId, ruleId2 );

                Collection<DynamicRecord> records1 = serializeRule( rule1, record1 );
                Collection<DynamicRecord> records2 = serializeRule( rule2, record2 );

                assertEquals( asList( record1 ), records1 );
                assertEquals( asList( record2 ), records2 );

                tx.nodeLabel( labelId, "label" );
                tx.propertyKey( propertyKeyId, "property" );

                tx.createSchema( asList(record1Before), records1, rule1 );
                tx.createSchema( asList(record2Before), records2, rule2 );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.SCHEMA, 2 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportArrayPropertyInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                DynamicRecord array = new DynamicRecord( next.arrayProperty() );
                array.setInUse( true );
                array.setCreated();
                array.setType( ARRAY.intValue() );
                array.setNextBlock( next.arrayProperty() );
                array.setData( UTF8.encode( "hello world" ) );

                PropertyBlock block = new PropertyBlock();
                block.setSingleBlock( (((long) ARRAY.intValue()) << 24) | (array.getId() << 28) );
                block.addValueRecord( array );

                PropertyRecord property = new PropertyRecord( next.property() );
                property.addPropertyBlock( block );

                tx.create( property );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.ARRAY_PROPERTY, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportRelationshipLabelNameInconsistencies() throws Exception
    {
        // given
        final Reference<Integer> inconsistentName = new Reference<>();
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                inconsistentName.set( next.relationshipType() );
                tx.relationshipType( inconsistentName.get(), "FOO" );
            }
        } );
        StoreAccess access = fixture.directStoreAccess().nativeStores();
        DynamicRecord record = access.getRelationshipTypeNameStore().getRecord( inconsistentName.get(),
                access.getRelationshipTypeNameStore().newRecord(), FORCE );
        record.setNextBlock( record.getId() );
        access.getRelationshipTypeNameStore().updateRecord( record );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP_TYPE_NAME, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportPropertyKeyNameInconsistencies() throws Exception
    {
        // given
        final Reference<Integer> inconsistentName = new Reference<>();
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                inconsistentName.set( next.propertyKey() );
                tx.propertyKey( inconsistentName.get(), "FOO" );
            }
        } );
        StoreAccess access = fixture.directStoreAccess().nativeStores();
        DynamicRecord record = access.getPropertyKeyNameStore().getRecord( inconsistentName.get() + 1,
                access.getPropertyKeyNameStore().newRecord(), FORCE );
        record.setNextBlock( record.getId() );
        access.getPropertyKeyNameStore().updateRecord( record );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.PROPERTY_KEY_NAME, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportRelationshipTypeInconsistencies() throws Exception
    {
        // given
        StoreAccess access = fixture.directStoreAccess().nativeStores();
        RecordStore<RelationshipTypeTokenRecord> relTypeStore = access.getRelationshipTypeTokenStore();
        RelationshipTypeTokenRecord record = relTypeStore.getRecord( (int) relTypeStore.nextId(),
                relTypeStore.newRecord(), FORCE );
        record.setNameId( 20 );
        record.setInUse( true );
        relTypeStore.updateRecord( record );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        access.close();
        on( stats ).verify( RecordType.RELATIONSHIP_TYPE, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportLabelInconsistencies() throws Exception
    {
        // given
        StoreAccess access = fixture.directStoreAccess().nativeStores();
        LabelTokenRecord record = access.getLabelTokenStore().getRecord( 1,
                access.getLabelTokenStore().newRecord(), FORCE );
        record.setNameId( 20 );
        record.setInUse( true );
        access.getLabelTokenStore().updateRecord( record );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.LABEL, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportPropertyKeyInconsistencies() throws Exception
    {
        // given
        final Reference<Integer> inconsistentKey = new Reference<>();
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                inconsistentKey.set( next.propertyKey() );
                tx.propertyKey( inconsistentKey.get(), "FOO" );
            }
        } );
        StoreAccess access = fixture.directStoreAccess().nativeStores();
        DynamicRecord record = access.getPropertyKeyNameStore().getRecord( inconsistentKey.get() + 1,
                access.getPropertyKeyNameStore().newRecord(), FORCE );
        record.setInUse( false );
        access.getPropertyKeyNameStore().updateRecord( record );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.PROPERTY_KEY, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportRelationshipGroupTypeInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                long node = next.node();
                long group = next.relationshipGroup();
                int nonExistentType = next.relationshipType() + 1;
                tx.create( inUse( new NodeRecord( node, true, group, NO_NEXT_PROPERTY.intValue() ) ) );
                tx.create( withOwner( inUse( new RelationshipGroupRecord( group, nonExistentType ) ), node ) );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP_GROUP, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportRelationshipGroupChainInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                long node = next.node();
                long group = next.relationshipGroup();
                tx.create( inUse( new NodeRecord( node, true, group, NO_NEXT_PROPERTY.intValue() ) ) );
                tx.create( withOwner( withNext( inUse( new RelationshipGroupRecord( group, C ) ),
                        group + 1 /*non-existent group id*/ ), node ) );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP_GROUP, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportRelationshipGroupUnsortedChainInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                long node = next.node();
                long firstGroupId = next.relationshipGroup();
                long otherGroupId = next.relationshipGroup();
                tx.create( inUse( new NodeRecord( node, true, firstGroupId, NO_NEXT_PROPERTY.intValue() ) ) );
                tx.create( withOwner( withNext( inUse( new RelationshipGroupRecord( firstGroupId, T ) ),
                        otherGroupId ), node ) );
                tx.create( withOwner( inUse( new RelationshipGroupRecord( otherGroupId, C ) ), node ) );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP_GROUP, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportRelationshipGroupRelationshipNotInUseInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                long node = next.node();
                long groupId = next.relationshipGroup();
                long rel = next.relationship();
                tx.create( inUse( new NodeRecord( node, true, groupId, NO_NEXT_PROPERTY.intValue() ) ) );
                tx.create( withOwner( withRelationships( inUse( new RelationshipGroupRecord( groupId, C ) ),
                        rel, rel, rel ), node ) );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP_GROUP, 3 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportRelationshipGroupRelationshipNotFirstInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                /*
                 *   node ----------------> group
                 *                             |
                 *                             v
                 *   otherNode <--> relA <--> relB
                 */
                long node = next.node();
                long otherNode = next.node();
                long group = next.relationshipGroup();
                long relA = next.relationship();
                long relB = next.relationship();
                tx.create( inUse( new NodeRecord( node, true, group, NO_NEXT_PROPERTY.intValue() ) ) );
                tx.create( inUse( new NodeRecord( otherNode, false, relA, NO_NEXT_PROPERTY.intValue() ) ) );
                tx.create( withNext( inUse( new RelationshipRecord( relA, otherNode, otherNode, C ) ), relB ) );
                tx.create( withPrev( inUse( new RelationshipRecord( relB, otherNode, otherNode, C ) ), relA ) );
                tx.create( withOwner( withRelationships( inUse( new RelationshipGroupRecord( group, C ) ), relB, relB, relB ), node ) );
                tx.incrementRelationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL, 2 );
                tx.incrementRelationshipCount( ANY_LABEL, C, ANY_LABEL, 2 );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP_GROUP, 3 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportFirstRelationshipGroupOwnerInconsistency() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                // node -[first]-> group -[owner]-> otherNode
                long node = next.node();
                long otherNode = next.node();
                long group = next.relationshipGroup();
                tx.create( inUse( new NodeRecord( node, true, group, NO_NEXT_PROPERTY.intValue() ) ) );
                tx.create( inUse( new NodeRecord( otherNode, false, NO_NEXT_RELATIONSHIP.intValue(),
                        NO_NEXT_PROPERTY.intValue() ) ) );
                tx.create( withOwner( inUse( new RelationshipGroupRecord( group, C ) ), otherNode ) );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        // - next group has other owner that its previous
        // - first group has other owner
        on( stats ).verify( RecordType.NODE, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportChainedRelationshipGroupOwnerInconsistency() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                /* node -[first]-> groupA -[next]-> groupB
                 *    ^               /                |
                 *     \--[owner]----               [owner]
                 *                                     v
                 *                                  otherNode
                 */
                long node = next.node();
                long otherNode = next.node();
                long groupA = next.relationshipGroup();
                long groupB = next.relationshipGroup();
                tx.create( inUse( new NodeRecord( node, true, groupA, NO_NEXT_PROPERTY.intValue() ) ) );
                tx.create( inUse( new NodeRecord( otherNode, false, NO_NEXT_RELATIONSHIP.intValue(),
                        NO_NEXT_PROPERTY.intValue() ) ) );
                tx.create( withNext( withOwner( inUse( new RelationshipGroupRecord( groupA, C ) ),
                        node ), groupB ) );
                tx.create( withOwner( inUse( new RelationshipGroupRecord( groupB, T ) ), otherNode ) );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP_GROUP, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportRelationshipGroupOwnerNotInUse() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                // group -[owner]-> <not-in-use node>
                long node = next.node();
                long group = next.relationshipGroup();
                tx.create( withOwner( inUse( new RelationshipGroupRecord( group, C ) ), node ) );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP_GROUP, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportRelationshipGroupOwnerInvalidValue() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                // node -[first]-> group -[owner]-> -1
                long group = next.relationshipGroup();
                tx.create( withOwner( inUse( new RelationshipGroupRecord( group, C ) ), -1 ) );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP_GROUP, 1 )
                   .andThatsAllFolks();
    }

    private RelationshipRecord withNext( RelationshipRecord relationship, long next )
    {
        relationship.setFirstNextRel( next );
        relationship.setSecondNextRel( next );
        return relationship;
    }

    private RelationshipRecord withPrev( RelationshipRecord relationship, long prev )
    {
        relationship.setFirstInFirstChain( false );
        relationship.setFirstInSecondChain( false );
        relationship.setFirstPrevRel( prev );
        relationship.setSecondPrevRel( prev );
        return relationship;
    }

    @Test
    public void shouldReportRelationshipGroupRelationshipOfOtherTypeInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                /*
                 *   node -----> groupA
                 *                   |
                 *                   v
                 *   otherNode <--> relB
                 */
                long node = next.node();
                long otherNode = next.node();
                long group = next.relationshipGroup();
                long rel = next.relationship();
                tx.create( new NodeRecord( node, true, group, NO_NEXT_PROPERTY.intValue() ) );
                tx.create( new NodeRecord( otherNode, false, rel, NO_NEXT_PROPERTY.intValue() ) );
                tx.create( new RelationshipRecord( rel, otherNode, otherNode, T ) );
                tx.create( withOwner( withRelationships( new RelationshipGroupRecord( group, C ),
                        rel, rel, rel ), node ) );
                tx.incrementRelationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL, 1 );
                tx.incrementRelationshipCount( ANY_LABEL, T, ANY_LABEL, 1 );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.RELATIONSHIP_GROUP, 3 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldNotReportRelationshipGroupInconsistenciesForConsistentRecords() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                /* Create a little mini consistent structure:
                 *
                 *    nodeA --> groupA -[next]-> groupB
                 *      ^          |
                 *       \       [out]
                 *        \        v
                 *       [start]- rel -[end]-> nodeB
                 */

                long nodeA = next.node();
                long nodeB = next.node();
                long rel = next.relationship();
                long groupA = next.relationshipGroup();
                long groupB = next.relationshipGroup();

                tx.create( new NodeRecord( nodeA, true, groupA, NO_NEXT_PROPERTY.intValue() ) );
                tx.create( new NodeRecord( nodeB, false, rel, NO_NEXT_PROPERTY.intValue() ) );
                tx.create( firstInChains( new RelationshipRecord( rel, nodeA, nodeB, C ), 1 ) );
                tx.incrementRelationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL, 1 );
                tx.incrementRelationshipCount( ANY_LABEL, C, ANY_LABEL, 1 );

                tx.create( withOwner( withRelationship( withNext( new RelationshipGroupRecord( groupA, C ), groupB ),
                        Direction.OUTGOING, rel ), nodeA ) );
                tx.create( withOwner( new RelationshipGroupRecord( groupB, T ), nodeA ) );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        assertTrue( "should be consistent", stats.isConsistent() );
    }

    @Test
    public void shouldReportWrongNodeCountsEntries() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                tx.incrementNodeCount( label3, 1 );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.COUNTS, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportWrongRelationshipCountsEntries() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                tx.incrementRelationshipCount( label1 , C, ANY_LABEL, 1 );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.COUNTS, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportIfSomeKeysAreMissing() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                tx.incrementNodeCount( label3, -1 );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.COUNTS, 1 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportIfThereAreExtraKeys() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                tx.incrementNodeCount( 1024 /* new label */, 1 );
            }
        } );

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.COUNTS, 2 )
                   .andThatsAllFolks();
    }

    @Test
    public void shouldReportDuplicatedIndexRules() throws Exception
    {
        // Given
        int labelId = createLabel();
        int propertyKeyId = createPropertyKey();
        createIndexRule( labelId, propertyKeyId );
        createIndexRule( labelId, propertyKeyId );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 1 ).andThatsAllFolks();
    }

    @Test
    public void shouldReportDuplicatedCompositeIndexRules() throws Exception
    {
        // Given
        int labelId = createLabel();
        int propertyKeyId1 = createPropertyKey( "p1" );
        int propertyKeyId2 = createPropertyKey( "p2" );
        int propertyKeyId3 = createPropertyKey( "p3" );
        createIndexRule( labelId, propertyKeyId1, propertyKeyId2, propertyKeyId3 );
        createIndexRule( labelId, propertyKeyId1, propertyKeyId2, propertyKeyId3 );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 1 ).andThatsAllFolks();
    }

    @Test
    public void shouldReportDuplicatedUniquenessConstraintRules() throws Exception
    {
        // Given
        int labelId = createLabel();
        int propertyKeyId = createPropertyKey();
        createUniquenessConstraintRule( labelId, propertyKeyId );
        createUniquenessConstraintRule( labelId, propertyKeyId );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 2 ) // pair of duplicated indexes & pair of duplicated constraints
                .andThatsAllFolks();
    }

    @Test
    public void shouldReportDuplicatedCompositeUniquenessConstraintRules() throws Exception
    {
        // Given
        int labelId = createLabel();
        int propertyKeyId1 = createPropertyKey( "p1" );
        int propertyKeyId2 = createPropertyKey( "p2" );
        createUniquenessConstraintRule( labelId, propertyKeyId1, propertyKeyId2 );
        createUniquenessConstraintRule( labelId, propertyKeyId1, propertyKeyId2 );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 2 ) // pair of duplicated indexes & pair of duplicated constraints
                .andThatsAllFolks();
    }

    @Test
    public void shouldReportDuplicatedNodeKeyConstraintRules() throws Exception
    {
        // Given
        int labelId = createLabel();
        int propertyKeyId1 = createPropertyKey( "p1" );
        int propertyKeyId2 = createPropertyKey( "p2" );
        createNodeKeyConstraintRule( labelId, propertyKeyId1, propertyKeyId2 );
        createNodeKeyConstraintRule( labelId, propertyKeyId1, propertyKeyId2 );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 2 ) // pair of duplicated indexes & pair of duplicated constraints
                .andThatsAllFolks();
    }

    @Test
    public void shouldReportDuplicatedNodePropertyExistenceConstraintRules() throws Exception
    {
        // Given
        int labelId = createLabel();
        int propertyKeyId = createPropertyKey();
        createNodePropertyExistenceConstraint( labelId, propertyKeyId );
        createNodePropertyExistenceConstraint( labelId, propertyKeyId );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 1 ).andThatsAllFolks();
    }

    @Test
    public void shouldReportDuplicatedRelationshipPropertyExistenceConstraintRules() throws Exception
    {
        // Given
        int relTypeId = createRelType();
        int propertyKeyId = createPropertyKey();
        createRelationshipPropertyExistenceConstraint( relTypeId, propertyKeyId );
        createRelationshipPropertyExistenceConstraint( relTypeId, propertyKeyId );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 1 ).andThatsAllFolks();
    }

    @Test
    public void shouldReportInvalidLabelIdInIndexRule() throws Exception
    {
        // Given
        int labelId = fixture.idGenerator().label();
        int propertyKeyId = createPropertyKey();
        createIndexRule( labelId, propertyKeyId );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 1 ).andThatsAllFolks();
    }

    @Test
    public void shouldReportInvalidLabelIdInUniquenessConstraintRule() throws Exception
    {
        // Given
        int badLabelId = fixture.idGenerator().label();
        int propertyKeyId = createPropertyKey();
        createUniquenessConstraintRule( badLabelId, propertyKeyId );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 2 ) // invalid label in both index & owning constraint
                .andThatsAllFolks();
    }

    @Test
    public void shouldReportInvalidLabelIdInNodeKeyConstraintRule() throws Exception
    {
        // Given
        int badLabelId = fixture.idGenerator().label();
        int propertyKeyId = createPropertyKey();
        createNodeKeyConstraintRule( badLabelId, propertyKeyId );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 2 ) // invalid label in both index & owning constraint
                .andThatsAllFolks();
    }

    @Test
    public void shouldReportInvalidLabelIdInNodePropertyExistenceConstraintRule() throws Exception
    {
        // Given
        int badLabelId = fixture.idGenerator().label();
        int propertyKeyId = createPropertyKey();
        createNodePropertyExistenceConstraint( badLabelId, propertyKeyId );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 1 ).andThatsAllFolks();
    }

    @Test
    public void shouldReportInvalidPropertyKeyIdInIndexRule() throws Exception
    {
        // Given
        int labelId = createLabel();
        int badPropertyKeyId = fixture.idGenerator().propertyKey();
        createIndexRule( labelId, badPropertyKeyId );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 1 ).andThatsAllFolks();
    }

    @Test
    public void shouldReportInvalidSecondPropertyKeyIdInIndexRule() throws Exception
    {
        // Given
        int labelId = createLabel();
        int propertyKeyId = createPropertyKey();
        int badPropertyKeyId = fixture.idGenerator().propertyKey();
        createIndexRule( labelId, propertyKeyId, badPropertyKeyId );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 1 ).andThatsAllFolks();
    }

    @Test
    public void shouldReportInvalidPropertyKeyIdInUniquenessConstraintRule() throws Exception
    {
        // Given
        int labelId = createLabel();
        int badPropertyKeyId = fixture.idGenerator().propertyKey();
        createUniquenessConstraintRule( labelId, badPropertyKeyId );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 2 ) // invalid property key in both index & owning constraint
                .andThatsAllFolks();
    }

    @Test
    public void shouldReportInvalidSecondPropertyKeyIdInUniquenessConstraintRule() throws Exception
    {
        // Given
        int labelId = createLabel();
        int propertyKeyId = createPropertyKey();
        int badPropertyKeyId = fixture.idGenerator().propertyKey();
        createUniquenessConstraintRule( labelId, propertyKeyId, badPropertyKeyId );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 2 ) // invalid property key in both index & owning constraint
                .andThatsAllFolks();
    }

    @Test
    public void shouldReportInvalidSecondPropertyKeyIdInNodeKeyConstraintRule() throws Exception
    {
        // Given
        int labelId = createLabel();
        int propertyKeyId = createPropertyKey();
        int badPropertyKeyId = fixture.idGenerator().propertyKey();
        createNodeKeyConstraintRule( labelId, propertyKeyId, badPropertyKeyId );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 2 ) // invalid property key in both index & owning constraint
                .andThatsAllFolks();
    }

    @Test
    public void shouldReportInvalidPropertyKeyIdInNodePropertyExistenceConstraintRule() throws Exception
    {
        // Given
        int labelId = createLabel();
        int badPropertyKeyId = fixture.idGenerator().propertyKey();
        createNodePropertyExistenceConstraint( labelId, badPropertyKeyId );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 1 ).andThatsAllFolks();
    }

    @Test
    public void shouldReportInvalidRelTypeIdInRelationshipPropertyExistenceConstraintRule() throws Exception
    {
        // Given
        int badRelTypeId = fixture.idGenerator().relationshipType();
        int propertyKeyId = createPropertyKey();
        createRelationshipPropertyExistenceConstraint( badRelTypeId, propertyKeyId );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        on( stats ).verify( RecordType.SCHEMA, 1 ).andThatsAllFolks();
    }

    @Test
    public void shouldReportNothingForUniquenessAndPropertyExistenceConstraintOnSameLabelAndProperty() throws Exception
    {
        // Given
        int labelId = createLabel();
        int propertyKeyId = createPropertyKey();

        createUniquenessConstraintRule( labelId, propertyKeyId );
        createNodePropertyExistenceConstraint( labelId, propertyKeyId );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        assertTrue( stats.isConsistent() );
    }

    @Test
    public void shouldReportNothingForNodeKeyAndPropertyExistenceConstraintOnSameLabelAndProperty() throws Exception
    {
        // Given
        int labelId = createLabel();
        int propertyKeyId = createPropertyKey();

        createNodeKeyConstraintRule( labelId, propertyKeyId );
        createNodePropertyExistenceConstraint( labelId, propertyKeyId );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        assertTrue( stats.isConsistent() );
    }

    @Test
    public void shouldManageUnusedRecordsWithWeirdDataIn() throws Exception
    {
        // Given
        final AtomicLong id = new AtomicLong();
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( TransactionDataBuilder tx, IdGenerator next )
            {
                id.set( next.relationship() );
                RelationshipRecord relationship = new RelationshipRecord( id.get() );
                relationship.setFirstNode( -1 );
                relationship.setSecondNode( -1 );
                relationship.setInUse( true );
                tx.create( relationship );
            }
        } );
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( TransactionDataBuilder tx, IdGenerator next )
            {
                RelationshipRecord relationship = new RelationshipRecord( id.get() );
                tx.delete( relationship );
            }
        } );

        // When
        ConsistencySummaryStatistics stats = check();

        // Then
        assertTrue( stats.isConsistent() );
    }

    private ConsistencySummaryStatistics check() throws ConsistencyCheckIncompleteException
    {
        return check( fixture.directStoreAccess() );
    }

    private ConsistencySummaryStatistics check( DirectStoreAccess stores ) throws ConsistencyCheckIncompleteException
    {
        Config config = config();
        FullCheck checker = new FullCheck( config, ProgressMonitorFactory.NONE, fixture.getAccessStatistics(),
                defaultConsistencyCheckThreadsNumber() );
        return checker.execute( stores, FormattedLog.toOutputStream( System.out ),
                ( report, method, message ) ->
                {
                    Set<String> types = allReports.get( report );
                    assert types != null;
                    types.remove( method );
                } );
    }

    private Config config()
    {
        Map<String,String> params = stringMap(
                // Enable property owners check by default in tests:
                ConsistencyCheckSettings.consistency_check_property_owners.name(), "true",
                GraphDatabaseSettings.record_format.name(), getRecordFormatName());
        return Config.defaults( params );
    }

    protected static RelationshipGroupRecord withRelationships( RelationshipGroupRecord group, long out,
            long in, long loop )
    {
        group.setFirstOut( out );
        group.setFirstIn( in );
        group.setFirstLoop( loop );
        return group;
    }

    protected static RelationshipGroupRecord withRelationship( RelationshipGroupRecord group, Direction direction,
            long rel )
    {
        switch ( direction )
        {
        case OUTGOING:
            group.setFirstOut( rel );
            break;
        case INCOMING:
            group.setFirstIn( rel );
            break;
        case BOTH:
            group.setFirstLoop( rel );
            break;
        default:
            throw new IllegalArgumentException( direction.name() );
        }
        return group;
    }

    protected static RelationshipRecord firstInChains( RelationshipRecord relationship, int count )
    {
        relationship.setFirstInFirstChain( true );
        relationship.setFirstPrevRel( count );
        relationship.setFirstInSecondChain( true );
        relationship.setSecondPrevRel( count );
        return relationship;
    }

    protected static RelationshipGroupRecord withNext( RelationshipGroupRecord group, long next )
    {
        group.setNext( next );
        return group;
    }

    protected static RelationshipGroupRecord withOwner( RelationshipGroupRecord record, long owner )
    {
        record.setOwningNode( owner );
        return record;
    }

    protected String getRecordFormatName()
    {
        return StringUtils.EMPTY;
    }

    private int createLabel() throws Exception
    {
        final MutableInt id = new MutableInt( -1 );

        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                int labelId = next.label();
                tx.nodeLabel( labelId, "label" );
                id.setValue( labelId );
            }
        } );

        return id.intValue();
    }

    private int createPropertyKey() throws Exception
    {
        return createPropertyKey( "property" );
    }

    private int createPropertyKey( String propertyKey ) throws Exception
    {
        final MutableInt id = new MutableInt( -1 );

        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                int propertyKeyId = next.propertyKey();
                tx.propertyKey( propertyKeyId, propertyKey );
                id.setValue( propertyKeyId );
            }
        } );

        return id.intValue();
    }

    private int createRelType() throws Exception
    {
        final MutableInt id = new MutableInt( -1 );

        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                int relTypeId = next.relationshipType();
                tx.relationshipType( relTypeId, "relType" );
                id.setValue( relTypeId );
            }
        } );

        return id.intValue();
    }

    private void createIndexRule( final int labelId, final int... propertyKeyIds ) throws Exception
    {
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                int id = (int) next.schema();

                DynamicRecord recordBefore = new DynamicRecord( id );
                DynamicRecord recordAfter = recordBefore.clone();

                IndexRule rule = IndexRule.indexRule( id, forLabel( labelId, propertyKeyIds ), DESCRIPTOR );
                Collection<DynamicRecord> records = serializeRule( rule, recordAfter );

                tx.createSchema( singleton( recordBefore ), records, rule );
            }
        } );
    }

    private void createUniquenessConstraintRule( final int labelId, final int... propertyKeyIds ) throws Exception
    {
        SchemaStore schemaStore = (SchemaStore) fixture.directStoreAccess().nativeStores().getSchemaStore();

        long ruleId1 = schemaStore.nextId();
        long ruleId2 = schemaStore.nextId();

        IndexRule indexRule = IndexRule.constraintIndexRule( ruleId1,
                uniqueForLabel( labelId, propertyKeyIds ), DESCRIPTOR, ruleId2 );
        ConstraintRule uniqueRule = ConstraintRule.constraintRule( ruleId2,
                ConstraintDescriptorFactory.uniqueForLabel( labelId, propertyKeyIds ), ruleId1 );

        writeToSchemaStore( schemaStore, indexRule );
        writeToSchemaStore( schemaStore, uniqueRule );
    }

    private void createNodeKeyConstraintRule( final int labelId, final int... propertyKeyIds ) throws Exception
    {
        SchemaStore schemaStore = (SchemaStore) fixture.directStoreAccess().nativeStores().getSchemaStore();

        long ruleId1 = schemaStore.nextId();
        long ruleId2 = schemaStore.nextId();

        IndexRule indexRule = IndexRule.constraintIndexRule( ruleId1,
                uniqueForLabel( labelId, propertyKeyIds ), DESCRIPTOR, ruleId2 );
        ConstraintRule nodeKeyRule = ConstraintRule.constraintRule( ruleId2,
                ConstraintDescriptorFactory.nodeKeyForLabel( labelId, propertyKeyIds ), ruleId1 );

        writeToSchemaStore( schemaStore, indexRule );
        writeToSchemaStore( schemaStore, nodeKeyRule );
    }

    private void createNodePropertyExistenceConstraint( int labelId, int propertyKeyId )
    {
        SchemaStore schemaStore = (SchemaStore) fixture.directStoreAccess().nativeStores().getSchemaStore();
        ConstraintRule rule = nodePropertyExistenceConstraintRule( schemaStore.nextId(), labelId, propertyKeyId );
        writeToSchemaStore( schemaStore, rule );
    }

    private void createRelationshipPropertyExistenceConstraint( int relTypeId, int propertyKeyId )
    {
        SchemaStore schemaStore = (SchemaStore) fixture.directStoreAccess().nativeStores().getSchemaStore();
        ConstraintRule rule = relPropertyExistenceConstraintRule( schemaStore.nextId(), relTypeId, propertyKeyId );
        writeToSchemaStore( schemaStore, rule );
    }

    private void writeToSchemaStore( SchemaStore schemaStore, SchemaRule rule )
    {
        Collection<DynamicRecord> records = schemaStore.allocateFrom( rule );
        for ( DynamicRecord record : records )
        {
            schemaStore.updateRecord( record );
        }
    }

    private static KernelStatement statementOn( GraphDatabaseService db )
    {
        DependencyResolver resolver = ((GraphDatabaseAPI) db).getDependencyResolver();
        ThreadToStatementContextBridge bridge = resolver.resolveDependency( ThreadToStatementContextBridge.class );
        return (KernelStatement) bridge.get();
    }

    private static class Reference<T>
    {
        private T value;

        void set( T value )
        {
            this.value = value;
        }

        T get()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return String.valueOf( value );
        }
    }

    public static final class ConsistencySummaryVerifier
    {
        private final ConsistencySummaryStatistics stats;
        private final Set<RecordType> types = new HashSet<>();
        private long total;

        public static ConsistencySummaryVerifier on( ConsistencySummaryStatistics stats )
        {
            return new ConsistencySummaryVerifier( stats );
        }

        private ConsistencySummaryVerifier( ConsistencySummaryStatistics stats )
        {
            this.stats = stats;
        }

        public ConsistencySummaryVerifier verify( RecordType type, int inconsistencies )
        {
            if ( !types.add( type ) )
            {
                throw new IllegalStateException( "Tried to verify the same type twice: " + type );
            }
            assertEquals( "Inconsistencies of type: " + type, inconsistencies,
                    stats.getInconsistencyCountForRecordType( type ) );
            total += inconsistencies;
            return this;
        }

        public void andThatsAllFolks()
        {
            assertEquals( "Total number of inconsistencies: " + stats, total, stats.getTotalInconsistencyCount() );
        }
    }

    private static Collection<DynamicRecord> serializeRule( SchemaRule rule, DynamicRecord... records )
    {
        byte[] data = rule.serialize();
        DynamicRecordAllocator dynamicRecordAllocator =
                new ReusableRecordsCompositeAllocator( asList( records ), schemaAllocator );
        Collection<DynamicRecord> result = new ArrayList<>();
        AbstractDynamicStore.allocateRecordsFromBytes( result, data, dynamicRecordAllocator );
        return result;
    }

    private static DynamicRecordAllocator schemaAllocator = new DynamicRecordAllocator()
    {
        private int next = 10000; // we start high to not conflict with real ids

        @Override
        public int getRecordDataSize()
        {
            return SchemaStore.BLOCK_SIZE;
        }

        @Override
        public DynamicRecord nextRecord()
        {
            return new DynamicRecord( next++ );
        }
    };
}
