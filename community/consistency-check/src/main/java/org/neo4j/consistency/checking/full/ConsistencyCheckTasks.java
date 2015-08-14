/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.consistency.checking.RelationshipRecordCheck;
import org.neo4j.consistency.checking.SchemaRecordCheck;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.CacheAction;
import org.neo4j.consistency.checking.cache.CacheProcessor;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.checking.index.IndexEntryProcessor;
import org.neo4j.consistency.checking.index.IndexIterator;
import org.neo4j.consistency.checking.labelscan.LabelScanCheck;
import org.neo4j.consistency.checking.labelscan.LabelScanDocumentProcessor;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
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
            StoreProcessor processor =
                    multiPass.processor( CheckStage.Stage1_NS_PropsLabels, PROPERTIES );
            tasks.add( create( CheckStage.Stage1_NS_PropsLabels.name(), nativeStores.getNodeStore(),
                    processor ) );
            //ReltionshipStore pass - check label counts using cached labels, check properties, skip nodes and relationships
            processor = multiPass.processor( CheckStage.Stage2_RS_Labels, LABELS );
            multiPass.reDecorateRelationship( processor, RelationshipRecordCheck.RelationshipRecordCheckPass1( false ) );
            tasks.add( create( CheckStage.Stage2_RS_Labels.name(), nativeStores.getRelationshipStore(),
                    processor ) );
            //NodeStore pass - just cache nextRel and inUse
            tasks.add( create( CheckStage.Stage3_NS_NextRel.name(), nativeStores.getNodeStore(),
                    new CacheProcessor( CheckStage.Stage3_NS_NextRel, cacheAccess,
                            new CacheAction.CacheNextRel( cacheAccess, Scanner.scan( nativeStores.getNodeStore() ) ) ) ) );
            //RelationshipStore pass - check nodes inUse, FirstInFirst, FirstInSecond using cached info
            processor = multiPass.processor( CheckStage.Stage4_RS_NextRel, NODES );
            multiPass.reDecorateRelationship( processor, RelationshipRecordCheck.RelationshipRecordCheckPass2( true ) );
            tasks.add( create( CheckStage.Stage4_RS_NextRel.name(), nativeStores.getRelationshipStore(),
                    processor ) );
            //NodeStore pass - just cache nextRel and inUse
            multiPass.reDecorateNode( processor, NodeRecordCheck.toCheckNextRel( false ), true );
            multiPass.reDecorateNode( processor, NodeRecordCheck.toCheckNextRelationshipGroup( false ), false );
            tasks.add( create( CheckStage.Stage5_Check_NextRel.name(), nativeStores.getNodeStore(),
                    new CacheProcessor( CheckStage.Stage5_Check_NextRel, cacheAccess,
                            new CacheAction.CheckNextRel( cacheAccess, nativeStores, processor ) ) ) );
            // source chain
            //RelationshipStore pass - forward scan of source chain using the cache.
            processor = multiPass.processor( CheckStage.Stage6_RS_Forward, RELATIONSHIPS );
            multiPass.reDecorateRelationship( processor,
                    RelationshipRecordCheck.RelationshipRecordCheckSourceChain( false ) );
            tasks.add( create( CheckStage.Stage6_RS_Forward.name(), nativeStores.getRelationshipStore(),
                    processor ) );
            //RelationshipStore pass - reverse scan of source chain using the cache.
            processor = multiPass.processor( CheckStage.Stage7_RS_Backward, RELATIONSHIPS );
            multiPass.reDecorateRelationship( processor,
                    RelationshipRecordCheck.RelationshipRecordCheckSourceChain( false ) );
            tasks.add( create( CheckStage.Stage7_RS_Backward.name(), nativeStores.getRelationshipStore(),
                    processor ) );

            //relationshipGroup
            StoreProcessor relGrpProcessor = multiPass.processor( Stage.PARALLEL_FORWARD, RELATIONSHIP_GROUPS );
            tasks.add( create( "RelationshipGroupStore-RelGrp", nativeStores.getRelationshipGroupStore(),
                    relGrpProcessor ) );

            PropertyReader propertyReader = new PropertyReader( nativeStores );
            tasks.add( new RecordScanner<>( CheckStage.Stage8_PS_Props.name(), statistics, numberOfThreads,
                    new IterableStore<>( nativeStores.getNodeStore(), true ), progress,
                    new PropertyAndNode2LabelIndexProcessor( reporter, (checkIndexes ? indexes : null ),
                            propertyReader, cacheAccess ),
                    CheckStage.Stage8_PS_Props, cacheAccess, new IterableStore<>( nativeStores.getPropertyStore(), true ) ) );

            tasks.add( create( "StringStore-Str", nativeStores.getStringStore(),
                    multiPass.processor( Stage.SEQUENTIAL_FORWARD, STRINGS ) ) );
            tasks.add( create( "ArrayStore-Arrays", nativeStores.getArrayStore(),
                    multiPass.processor( Stage.SEQUENTIAL_FORWARD, ARRAYS ) ) );
        }
        // The schema store is verified in multiple passes that share state since it fits into memory
        // and we care about the consistency of back references (cf. SemanticCheck)
        // PASS 1: Dynamic record chains
        tasks.add( create( "SchemaStore", nativeStores.getSchemaStore() ) );
        // PASS 2: Rule integrity and obligation build up
        final SchemaRecordCheck schemaCheck =
                new SchemaRecordCheck( new SchemaStorage( nativeStores.getSchemaStore() ) );
        tasks.add( new SchemaStoreProcessorTask<>( "SchemaStoreProcessor-check_rules", statistics, numberOfThreads,
                nativeStores.getSchemaStore(), nativeStores, "check_rules",
                schemaCheck, progress, cacheAccess, defaultProcessor ) );
        // PASS 3: Obligation verification and semantic rule uniqueness
        tasks.add( new SchemaStoreProcessorTask<>( "SchemaStoreProcessor-check_obligations", statistics,
                    numberOfThreads, nativeStores.getSchemaStore(), nativeStores,
                "check_obligations", schemaCheck.forObligationChecking(), progress, cacheAccess, defaultProcessor ) );
        if ( checkGraph )
        {
            tasks.add( create( "RelationshipTypeTokenStore", nativeStores.getRelationshipTypeTokenStore() ) );
            tasks.add( create( "PropertyKeyTokenStore", nativeStores.getPropertyKeyTokenStore() ) );
            tasks.add( create( "LabelTokenStore", nativeStores.getLabelTokenStore() ) );
            tasks.add( create( "RelationshipTypeNameStore", nativeStores.getRelationshipTypeNameStore() ) );
            tasks.add( create( "PropertyKeyNameStore", nativeStores.getPropertyKeyNameStore() ) );
            tasks.add( create( "LabelNameStore", nativeStores.getLabelNameStore() ) );
            tasks.add( create( "NodeDynamicLabelStore", nativeStores.getNodeDynamicLabelStore() ) );
        }
        if ( checkLabelScanStore )
        {
            tasks.add( new RecordScanner<>( "NodeStoreToLabelScanStore", statistics, numberOfThreads,
                    new IterableStore<>( nativeStores.getNodeStore(), true ),
                    progress,
                    new NodeToLabelScanRecordProcessor( reporter, labelScanStore ),
                    CheckStage.Stage9_NS_LabelCounts, cacheAccess ) );
        }
        ConsistencyReporter filteredReporter = multiPass.reporter( NODES );
        if ( checkLabelScanStore )
        {
            tasks.add( new RecordScanner<>( "LabelScanStore", statistics, numberOfThreads,
                    labelScanStore.newAllEntriesReader(), progress, new LabelScanDocumentProcessor(
                            filteredReporter, new LabelScanCheck() ), Stage.SEQUENTIAL_FORWARD, cacheAccess ) );
        }
        if ( checkIndexes )
        {
            for ( IndexRule indexRule : indexes.rules() )
            {
                tasks.add( new RecordScanner<>( format( "Index_%d", indexRule.getId() ),
                        statistics, numberOfThreads, new IndexIterator( indexes.accessorFor( indexRule ) ),
                        progress, new IndexEntryProcessor( filteredReporter, new IndexCheck( indexRule ) ),
                        Stage.SEQUENTIAL_FORWARD, cacheAccess ) );
            }
        }
        return tasks;
    }

    private <RECORD extends AbstractBaseRecord> StoreProcessorTask<RECORD> create( String name,
            RecordStore<RECORD> input )
    {
        return new StoreProcessorTask<>( name, statistics, numberOfThreads, input, nativeStores, name, progress,
                cacheAccess, defaultProcessor );
    }

    private <RECORD extends AbstractBaseRecord> StoreProcessorTask<RECORD> create( String name,
            RecordStore<RECORD> input, StoreProcessor processor )
    {
        return new StoreProcessorTask<>( name, statistics, numberOfThreads, input, nativeStores, name, progress,
                cacheAccess, processor );
    }
}
