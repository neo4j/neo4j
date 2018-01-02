/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.consistency.checking.NodeRecordCheck;
import org.neo4j.consistency.checking.PropertyChain;
import org.neo4j.consistency.checking.RelationshipRecordCheck;
import org.neo4j.consistency.checking.SchemaRecordCheck;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.CacheTask;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.checking.index.IndexEntryProcessor;
import org.neo4j.consistency.checking.index.IndexIterator;
import org.neo4j.consistency.checking.labelscan.LabelScanCheck;
import org.neo4j.consistency.checking.labelscan.LabelScanDocumentProcessor;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.direct.BoundedIterable;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RecordStore.Scanner;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;

import static java.lang.String.format;

import static org.neo4j.consistency.checking.full.MultiPassStore.ARRAYS;
import static org.neo4j.consistency.checking.full.MultiPassStore.LABELS;
import static org.neo4j.consistency.checking.full.MultiPassStore.NODES;
import static org.neo4j.consistency.checking.full.MultiPassStore.PROPERTIES;
import static org.neo4j.consistency.checking.full.MultiPassStore.RELATIONSHIPS;
import static org.neo4j.consistency.checking.full.MultiPassStore.RELATIONSHIP_GROUPS;
import static org.neo4j.consistency.checking.full.MultiPassStore.STRINGS;
import static org.neo4j.consistency.checking.full.QueueDistribution.ROUND_ROBIN;

public class ConsistencyCheckTasks
{
    private final ProgressMonitorFactory.MultiPartBuilder progress;
    private final StoreProcessor defaultProcessor;
    private final StoreAccess nativeStores;
    private final Statistics statistics;
    private final MultiPassStore.Factory multiPass;
    private final ConsistencyReporter reporter;
    private final LabelScanStore labelScanStore;
    private final IndexAccessors indexes;
    private final CacheAccess cacheAccess;
    private final int numberOfThreads;

    ConsistencyCheckTasks( ProgressMonitorFactory.MultiPartBuilder progress,
            StoreProcessor defaultProcessor, StoreAccess nativeStores, Statistics statistics,
            CacheAccess cacheAccess, LabelScanStore labelScanStore,
            IndexAccessors indexes, MultiPassStore.Factory multiPass, ConsistencyReporter reporter, int numberOfThreads )
    {
        this.progress = progress;
        this.defaultProcessor = defaultProcessor;
        this.nativeStores = nativeStores;
        this.statistics = statistics;
        this.cacheAccess = cacheAccess;
        this.multiPass = multiPass;
        this.reporter = reporter;
        this.labelScanStore = labelScanStore;
        this.indexes = indexes;
        this.numberOfThreads = numberOfThreads;
    }

    public List<ConsistencyCheckerTask> createTasksForFullCheck( boolean checkLabelScanStore, boolean checkIndexes,
            boolean checkGraph )
    {
        List<ConsistencyCheckerTask> tasks = new ArrayList<>();
        if ( checkGraph )
        {
            MandatoryProperties mandatoryProperties = new MandatoryProperties( nativeStores );
            StoreProcessor processor =
                    multiPass.processor( CheckStage.Stage1_NS_PropsLabels, PROPERTIES );
            tasks.add( create( CheckStage.Stage1_NS_PropsLabels.name(), nativeStores.getNodeStore(),
                    processor, ROUND_ROBIN ) );
            //ReltionshipStore pass - check label counts using cached labels, check properties, skip nodes and relationships
            processor = multiPass.processor( CheckStage.Stage2_RS_Labels, LABELS );
            multiPass.reDecorateRelationship( processor, RelationshipRecordCheck.relationshipRecordCheckForwardPass() );
            tasks.add( create( CheckStage.Stage2_RS_Labels.name(), nativeStores.getRelationshipStore(),
                    processor, ROUND_ROBIN ) );
            //NodeStore pass - just cache nextRel and inUse
            tasks.add( new CacheTask.CacheNextRel( CheckStage.Stage3_NS_NextRel, cacheAccess,
                    Scanner.scan( nativeStores.getNodeStore() ) ) );
            //RelationshipStore pass - check nodes inUse, FirstInFirst, FirstInSecond using cached info
            processor = multiPass.processor( CheckStage.Stage4_RS_NextRel, NODES );
            multiPass.reDecorateRelationship( processor, RelationshipRecordCheck.relationshipRecordCheckBackwardPass(
                    new PropertyChain<>( mandatoryProperties.forRelationships( reporter ) ) ) );
            tasks.add( create( CheckStage.Stage4_RS_NextRel.name(), nativeStores.getRelationshipStore(),
                    processor, ROUND_ROBIN ) );
            //NodeStore pass - just cache nextRel and inUse
            multiPass.reDecorateNode( processor, NodeRecordCheck.toCheckNextRel(), true );
            multiPass.reDecorateNode( processor, NodeRecordCheck.toCheckNextRelationshipGroup(), false );
            tasks.add( new CacheTask.CheckNextRel( CheckStage.Stage5_Check_NextRel, cacheAccess, nativeStores, processor ) );
            // source chain
            //RelationshipStore pass - forward scan of source chain using the cache.
            processor = multiPass.processor( CheckStage.Stage6_RS_Forward, RELATIONSHIPS );
            multiPass.reDecorateRelationship( processor,
                    RelationshipRecordCheck.relationshipRecordCheckSourceChain() );
            tasks.add( create( CheckStage.Stage6_RS_Forward.name(), nativeStores.getRelationshipStore(),
                    processor, QueueDistribution.RELATIONSHIPS ) );
            //RelationshipStore pass - reverse scan of source chain using the cache.
            processor = multiPass.processor( CheckStage.Stage7_RS_Backward, RELATIONSHIPS );
            multiPass.reDecorateRelationship( processor,
                    RelationshipRecordCheck.relationshipRecordCheckSourceChain() );
            tasks.add( create( CheckStage.Stage7_RS_Backward.name(), nativeStores.getRelationshipStore(),
                    processor, QueueDistribution.RELATIONSHIPS ) );

            //relationshipGroup
            StoreProcessor relGrpProcessor = multiPass.processor( Stage.PARALLEL_FORWARD, RELATIONSHIP_GROUPS );
            tasks.add( create( "RelationshipGroupStore-RelGrp", nativeStores.getRelationshipGroupStore(),
                    relGrpProcessor, ROUND_ROBIN ) );

            PropertyReader propertyReader = new PropertyReader( nativeStores );
            tasks.add( recordScanner( CheckStage.Stage8_PS_Props.name(),
                    new IterableStore<>( nativeStores.getNodeStore(), true ),
                    new PropertyAndNode2LabelIndexProcessor( reporter, (checkIndexes ? indexes : null ),
                            propertyReader, cacheAccess, mandatoryProperties.forNodes( reporter ) ),
                    CheckStage.Stage8_PS_Props, ROUND_ROBIN,
                    new IterableStore<>( nativeStores.getPropertyStore(), true ) ) );

            tasks.add( create( "StringStore-Str", nativeStores.getStringStore(),
                    multiPass.processor( Stage.SEQUENTIAL_FORWARD, STRINGS ), ROUND_ROBIN ) );
            tasks.add( create( "ArrayStore-Arrays", nativeStores.getArrayStore(),
                    multiPass.processor( Stage.SEQUENTIAL_FORWARD, ARRAYS ), ROUND_ROBIN ) );
        }
        // The schema store is verified in multiple passes that share state since it fits into memory
        // and we care about the consistency of back references (cf. SemanticCheck)
        // PASS 1: Dynamic record chains
        tasks.add( create( "SchemaStore", nativeStores.getSchemaStore(), ROUND_ROBIN ) );
        // PASS 2: Rule integrity and obligation build up
        final SchemaRecordCheck schemaCheck =
                new SchemaRecordCheck( new SchemaStorage( nativeStores.getSchemaStore() ) );
        tasks.add( new SchemaStoreProcessorTask<>( "SchemaStoreProcessor-check_rules", statistics, numberOfThreads,
                nativeStores.getSchemaStore(), nativeStores, "check_rules",
                schemaCheck, progress, cacheAccess, defaultProcessor, ROUND_ROBIN ) );
        // PASS 3: Obligation verification and semantic rule uniqueness
        tasks.add( new SchemaStoreProcessorTask<>( "SchemaStoreProcessor-check_obligations", statistics,
                    numberOfThreads, nativeStores.getSchemaStore(), nativeStores,
                "check_obligations", schemaCheck.forObligationChecking(), progress, cacheAccess, defaultProcessor,
                ROUND_ROBIN ) );
        if ( checkGraph )
        {
            tasks.add( create( "RelationshipTypeTokenStore", nativeStores.getRelationshipTypeTokenStore(), ROUND_ROBIN ) );
            tasks.add( create( "PropertyKeyTokenStore", nativeStores.getPropertyKeyTokenStore(), ROUND_ROBIN ) );
            tasks.add( create( "LabelTokenStore", nativeStores.getLabelTokenStore(), ROUND_ROBIN ) );
            tasks.add( create( "RelationshipTypeNameStore", nativeStores.getRelationshipTypeNameStore(), ROUND_ROBIN ) );
            tasks.add( create( "PropertyKeyNameStore", nativeStores.getPropertyKeyNameStore(), ROUND_ROBIN ) );
            tasks.add( create( "LabelNameStore", nativeStores.getLabelNameStore(), ROUND_ROBIN ) );
            tasks.add( create( "NodeDynamicLabelStore", nativeStores.getNodeDynamicLabelStore(), ROUND_ROBIN ) );
        }
        if ( checkLabelScanStore )
        {
            tasks.add( recordScanner( "NodeStoreToLabelScanStore",
                    new IterableStore<>( nativeStores.getNodeStore(), true ),
                    new NodeToLabelScanRecordProcessor( reporter, labelScanStore ),
                    CheckStage.Stage9_NS_LabelCounts, ROUND_ROBIN ) );
        }
        ConsistencyReporter filteredReporter = multiPass.reporter( NODES );
        if ( checkLabelScanStore )
        {
            tasks.add( recordScanner( "LabelScanStore",
                    labelScanStore.newAllEntriesReader(), new LabelScanDocumentProcessor(
                            filteredReporter, new LabelScanCheck() ), Stage.SEQUENTIAL_FORWARD,
                    ROUND_ROBIN ) );
        }
        if ( checkIndexes )
        {
            for ( IndexRule indexRule : indexes.rules() )
            {
                tasks.add( recordScanner( format( "Index_%d", indexRule.getId() ),
                        new IndexIterator( indexes.accessorFor( indexRule ) ),
                        new IndexEntryProcessor( filteredReporter, new IndexCheck( indexRule ) ),
                        Stage.SEQUENTIAL_FORWARD, ROUND_ROBIN ) );
            }
        }
        return tasks;
    }

    private <RECORD> RecordScanner<RECORD> recordScanner( String name,
            BoundedIterable<RECORD> store, RecordProcessor<RECORD> processor, Stage stage,
            QueueDistribution distribution,
            @SuppressWarnings( "rawtypes" ) IterableStore... warmupStores )
    {
        return stage.isParallel()
                ? new ParallelRecordScanner<>( name, statistics, numberOfThreads, store, progress, processor,
                        cacheAccess, distribution, warmupStores )
                : new SequentialRecordScanner<>( name, statistics, numberOfThreads, store, progress, processor,
                        warmupStores );
    }

    private <RECORD extends AbstractBaseRecord> StoreProcessorTask<RECORD> create( String name,
            RecordStore<RECORD> input, QueueDistribution distribution )
    {
        return new StoreProcessorTask<>( name, statistics, numberOfThreads, input, nativeStores, name, progress,
                cacheAccess, defaultProcessor, distribution );
    }

    private <RECORD extends AbstractBaseRecord> StoreProcessorTask<RECORD> create( String name,
            RecordStore<RECORD> input, StoreProcessor processor, QueueDistribution distribution )
    {
        return new StoreProcessorTask<>( name, statistics, numberOfThreads, input, nativeStores, name, progress,
                cacheAccess, processor, distribution );
    }
}
