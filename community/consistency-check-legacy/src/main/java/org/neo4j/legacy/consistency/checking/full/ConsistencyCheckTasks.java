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
package org.neo4j.legacy.consistency.checking.full;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.legacy.consistency.checking.SchemaRecordCheck;
import org.neo4j.legacy.consistency.checking.index.IndexAccessors;
import org.neo4j.legacy.consistency.checking.index.IndexEntryProcessor;
import org.neo4j.legacy.consistency.checking.index.IndexIterator;
import org.neo4j.legacy.consistency.checking.labelscan.LabelScanCheck;
import org.neo4j.legacy.consistency.checking.labelscan.LabelScanDocumentProcessor;
import org.neo4j.legacy.consistency.report.ConsistencyReporter;

import static java.lang.String.format;

import static org.neo4j.legacy.consistency.checking.full.MultiPassStore.ARRAYS;
import static org.neo4j.legacy.consistency.checking.full.MultiPassStore.LABELS;
import static org.neo4j.legacy.consistency.checking.full.MultiPassStore.NODES;
import static org.neo4j.legacy.consistency.checking.full.MultiPassStore.PROPERTIES;
import static org.neo4j.legacy.consistency.checking.full.MultiPassStore.PROPERTY_KEYS;
import static org.neo4j.legacy.consistency.checking.full.MultiPassStore.RELATIONSHIPS;
import static org.neo4j.legacy.consistency.checking.full.MultiPassStore.RELATIONSHIP_GROUPS;
import static org.neo4j.legacy.consistency.checking.full.MultiPassStore.STRINGS;

public class ConsistencyCheckTasks
{
    private final ProgressMonitorFactory.MultiPartBuilder progress;
    private final TaskExecutionOrder order;
    private final StoreProcessor processor;

    ConsistencyCheckTasks( ProgressMonitorFactory.MultiPartBuilder progress, TaskExecutionOrder order,
                           StoreProcessor processor )
    {
        this.progress = progress;
        this.order = order;
        this.processor = processor;
    }

    public List<StoppableRunnable> createTasks(
            StoreAccess nativeStores, LabelScanStore labelScanStore, IndexAccessors indexes,
            MultiPassStore.Factory multiPass, ConsistencyReporter reporter,
            boolean checkLabelScanStore, boolean checkIndexes, boolean checkGraph )
    {
        List<StoppableRunnable> tasks = new ArrayList<>();

        if ( checkGraph )
        {
            tasks.add( create( nativeStores.getNodeStore(),
                    multiPass.processors( LABELS, PROPERTIES, RELATIONSHIPS, RELATIONSHIP_GROUPS ) ) );

            tasks.add( create( nativeStores.getRelationshipStore(),
                    multiPass.processors(  NODES, PROPERTIES, RELATIONSHIPS  ) ) );

            tasks.add( create( nativeStores.getPropertyStore(),
                    multiPass.processors(  PROPERTIES, STRINGS, ARRAYS, PROPERTY_KEYS ) ) );

            tasks.add( create( nativeStores.getStringStore(), multiPass.processors( STRINGS ) ) );

            tasks.add( create( nativeStores.getArrayStore(), multiPass.processors( ARRAYS ) ) );

            tasks.add( create( nativeStores.getRelationshipGroupStore(), multiPass.processors( RELATIONSHIP_GROUPS ) ) );
        }

        // The schema store is verified in multiple passes that share state since it fits into memory
        // and we care about the consistency of back references (cf. SemanticCheck)

        // PASS 1: Dynamic record chains
        tasks.add( create( nativeStores.getSchemaStore() ));

        // PASS 2: Rule integrity and obligation build up
        final SchemaRecordCheck schemaCheck = new SchemaRecordCheck( new SchemaStorage( nativeStores.getSchemaStore() ) );
        tasks.add( new SchemaStoreProcessorTask<>(
                nativeStores.getSchemaStore(), "check_rules", schemaCheck, progress, order,
                processor, processor ) );

        // PASS 3: Obligation verification and semantic rule uniqueness
        tasks.add( new SchemaStoreProcessorTask<>(
                nativeStores.getSchemaStore(), "check_obligations", schemaCheck.forObligationChecking(), progress, order,
                processor, processor ) );

        if ( checkGraph )
        {
            tasks.add( create( nativeStores.getRelationshipTypeTokenStore() ) );
            tasks.add( create( nativeStores.getPropertyKeyTokenStore() ) );
            tasks.add( create( nativeStores.getLabelTokenStore() ) );
            tasks.add( create( nativeStores.getRelationshipTypeNameStore() ) );
            tasks.add( create( nativeStores.getPropertyKeyNameStore() ) );
            tasks.add( create( nativeStores.getLabelNameStore() ) );
            tasks.add( create( nativeStores.getNodeDynamicLabelStore() ) );
        }

        if ( checkLabelScanStore )
        {
            tasks.add( new RecordScanner<>( new IterableStore<>( nativeStores.getNodeStore() ),
                    "NodeStoreToLabelScanStore",
                    progress, new NodeToLabelScanRecordProcessor( reporter, labelScanStore ) ) );
        }

        if ( checkIndexes )
        {
            tasks.add( new RecordScanner<>( new IterableStore<>( nativeStores.getNodeStore() ), "NodeStoreToIndexes",
                    progress, new NodeToLabelIndexesProcessor( reporter, indexes, new PropertyReader(
                            (PropertyStore) nativeStores.getPropertyStore(), (NodeStore) nativeStores.getNodeStore() ) ) ) );
        }

        int iPass = 0;
        for ( ConsistencyReporter filteredReporter : multiPass.reporters( order, NODES ) )
        {
            if ( checkLabelScanStore )
            {
                tasks.add( new RecordScanner<>( labelScanStore.newAllEntriesReader(),
                        format( "LabelScanStore_%d", iPass ), progress, new LabelScanDocumentProcessor(
                        filteredReporter,
                        new LabelScanCheck() ) ) );
            }

            if ( checkIndexes )
            {
                for ( IndexRule indexRule : indexes.rules() )
                {
                    tasks.add( new RecordScanner<>( new IndexIterator( indexes.accessorFor( indexRule ) ),
                            format( "Index_%d_%d", indexRule.getId(), iPass ), progress,
                            new IndexEntryProcessor( filteredReporter,
                            new IndexCheck( indexRule ) ) ) );
                }
            }
            iPass++;
        }
        return tasks;
    }

    private <RECORD extends AbstractBaseRecord> StoreProcessorTask<RECORD> create( RecordStore<RECORD> input )
    {
        return new StoreProcessorTask<>(
                input, progress, order, processor, processor );
    }

    private <RECORD extends AbstractBaseRecord> StoreProcessorTask<RECORD> create( RecordStore<RECORD> input,
            StoreProcessor[] processors )
    {
        return new StoreProcessorTask<>(
                input, progress, order, processor, processors );
    }
}
