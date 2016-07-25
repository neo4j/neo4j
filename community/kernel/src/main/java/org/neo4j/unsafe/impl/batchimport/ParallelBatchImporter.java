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
package org.neo4j.unsafe.impl.batchimport;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Format;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.unsafe.impl.batchimport.cache.GatheringMemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.NodeLabelsCache;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputCache;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.PerTypeRelationshipSplitter;
import org.neo4j.unsafe.impl.batchimport.staging.DynamicProcessorAssigner;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;

import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.SourceOrCachedInputIterable.cachedForSure;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.MAIN;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors.superviseExecution;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors.withDynamicProcessorAssignment;

/**
 * {@link BatchImporter} which tries to exercise as much of the available resources to gain performance.
 * Or rather ensure that the slowest resource (usually I/O) is fully saturated and that enough work is
 * being performed to keep that slowest resource saturated all the time.
 * <p>
 * Overall goals: split up processing cost by parallelizing. Keep CPUs busy, keep I/O busy and writing sequentially.
 * I/O is only allowed to be read to and written from sequentially, any random access drastically reduces performance.
 * Goes through multiple stages where each stage has one or more steps executing in parallel, passing
 * batches between these steps through each stage, i.e. passing batches downstream.
 */
public class ParallelBatchImporter implements BatchImporter
{
    private final File storeDir;
    private final FileSystemAbstraction fileSystem;
    private final Configuration config;
    private final LogService logService;
    private final Log log;
    private final ExecutionMonitor executionMonitor;
    private final AdditionalInitialIds additionalInitialIds;
    private final Config dbConfig;

    /**
     * Advanced usage of the parallel batch importer, for special and very specific cases. Please use
     * a constructor with fewer arguments instead.
     */
    public ParallelBatchImporter( File storeDir, FileSystemAbstraction fileSystem, Configuration config,
            LogService logService, ExecutionMonitor executionMonitor,
            AdditionalInitialIds additionalInitialIds,
            Config dbConfig )
    {
        this.storeDir = storeDir;
        this.fileSystem = fileSystem;
        this.config = config;
        this.logService = logService;
        this.dbConfig = dbConfig;
        this.log = logService.getInternalLogProvider().getLog( getClass() );
        this.executionMonitor = executionMonitor;
        this.additionalInitialIds = additionalInitialIds;
    }

    /**
     * Instantiates {@link ParallelBatchImporter} with default services and behaviour.
     * The provided {@link ExecutionMonitor} will be decorated with {@link DynamicProcessorAssigner} for
     * optimal assignment of processors to bottleneck steps over time.
     */
    public ParallelBatchImporter( File storeDir, Configuration config, LogService logService,
            ExecutionMonitor executionMonitor, Config dbConfig )
    {
        this( storeDir, new DefaultFileSystemAbstraction(), config, logService,
                withDynamicProcessorAssignment( executionMonitor, config ), EMPTY, dbConfig );
    }

    @Override
    public void doImport( Input input ) throws IOException
    {
        log.info( "Import starting" );

        // Things that we need to close later. The reason they're not in the try-with-resource statement
        // is that we need to close, and set to null, at specific points preferably. So use good ol' finally block.
        NodeRelationshipCache nodeRelationshipCache = null;
        NodeLabelsCache nodeLabelsCache = null;
        long startTime = currentTimeMillis();
        boolean hasBadEntries = false;
        File badFile = new File( storeDir, Configuration.BAD_FILE_NAME );
        CountingStoreUpdateMonitor storeUpdateMonitor = new CountingStoreUpdateMonitor();
        RecordFormats recordFormats = RecordFormatSelector.selectForConfig( dbConfig, NullLogProvider.getInstance() );
        try ( BatchingNeoStores neoStore = new BatchingNeoStores( fileSystem, storeDir, recordFormats, config, logService,
                additionalInitialIds, dbConfig );
              CountsAccessor.Updater countsUpdater = neoStore.getCountsStore().reset(
                    neoStore.getLastCommittedTransactionId() );
              InputCache inputCache = new InputCache( fileSystem, storeDir, recordFormats, config ) )
        {
            Collector badCollector = input.badCollector();
            // Some temporary caches and indexes in the import
            IoMonitor writeMonitor = new IoMonitor( neoStore.getIoTracer() );
            IdMapper idMapper = input.idMapper();
            IdGenerator idGenerator = input.idGenerator();
            nodeRelationshipCache = new NodeRelationshipCache( AUTO, config.denseNodeThreshold() );
            StatsProvider memoryUsageStats = new MemoryUsageStatsProvider( nodeRelationshipCache, idMapper );
            InputIterable<InputNode> nodes = input.nodes();
            InputIterable<InputRelationship> relationships = input.relationships();
            InputIterable<InputNode> cachedNodes = cachedForSure( nodes, inputCache.nodes( MAIN, true ) );
            InputIterable<InputRelationship> cachedRelationships =
                    cachedForSure( relationships, inputCache.relationships( MAIN, true ) );

            RelationshipStore relationshipStore = neoStore.getRelationshipStore();

            // Stage 1 -- nodes, properties, labels
            NodeStage nodeStage = new NodeStage( config, writeMonitor,
                    nodes, idMapper, idGenerator, neoStore, inputCache, neoStore.getLabelScanStore(),
                    storeUpdateMonitor, memoryUsageStats );

            // Stage 2 -- calculate dense node threshold
            CalculateDenseNodesStage calculateDenseNodesStage = new CalculateDenseNodesStage( config,
                    relationships, nodeRelationshipCache, idMapper, badCollector, inputCache, neoStore );

            // Execute stages 1 and 2 in parallel or sequentially?
            if ( idMapper.needsPreparation() )
            {   // The id mapper of choice needs preparation in order to get ids from it,
                // So we need to execute the node stage first as it fills the id mapper and prepares it in the end,
                // before executing any stage that needs ids from the id mapper, for example calc dense node stage.
                executeStages( nodeStage );
                executeStages( new IdMapperPreparationStage( config, idMapper, cachedNodes,
                        badCollector, memoryUsageStats ) );
                PrimitiveLongIterator duplicateNodeIds = badCollector.leftOverDuplicateNodesIds();
                if ( duplicateNodeIds.hasNext() )
                {
                    executeStages( new DeleteDuplicateNodesStage( config, duplicateNodeIds, neoStore ) );
                }
                executeStages( calculateDenseNodesStage );
            }
            else
            {   // The id mapper of choice doesn't need any preparation, so we can go ahead and execute
                // the node and calc dense node stages in parallel.
                executeStages( nodeStage, calculateDenseNodesStage );
            }
            // At this point we know how many nodes we have, so we tell the cache that instead of having the
            // cache keeping track of that in a the face of concurrent updates.
            nodeRelationshipCache.setHighNodeId( neoStore.getNodeStore().getHighId() );

            importRelationships( nodeRelationshipCache, storeUpdateMonitor, neoStore, writeMonitor,
                    idMapper, cachedRelationships, inputCache,
                    calculateDenseNodesStage.getRelationshipTypes( Long.MAX_VALUE ),
                    // Is batch size a good measure for considering a group of relationships a minority?
                    calculateDenseNodesStage.getRelationshipTypes( config.batchSize() ) );

            // Release this potentially really big piece of cached data
            long memoryWeCanHoldForCertain = totalMemoryUsageOf( idMapper, nodeRelationshipCache );
            long highNodeId = nodeRelationshipCache.getHighNodeId();
            idMapper.close();
            idMapper = null;
            nodeRelationshipCache.close();
            nodeRelationshipCache = null;

            new RelationshipGroupDefragmenter( config, executionMonitor ).run(
                    max( max( memoryWeCanHoldForCertain, highNodeId * 4), mebiBytes( 1 ) ), neoStore, highNodeId );

            // Stage 6 -- count nodes per label and labels per node
            nodeLabelsCache = new NodeLabelsCache( AUTO, neoStore.getLabelRepository().getHighId() );
            memoryUsageStats = new MemoryUsageStatsProvider( nodeLabelsCache );
            executeStages( new NodeCountsStage( config, nodeLabelsCache, neoStore.getNodeStore(),
                    neoStore.getLabelRepository().getHighId(), countsUpdater, memoryUsageStats ) );
            // Stage 7 -- count label-[type]->label
            executeStages( new RelationshipCountsStage( config, nodeLabelsCache, relationshipStore,
                    neoStore.getLabelRepository().getHighId(),
                    neoStore.getRelationshipTypeRepository().getHighId(), countsUpdater, AUTO ) );

            // We're done, do some final logging about it
            long totalTimeMillis = currentTimeMillis() - startTime;
            executionMonitor.done( totalTimeMillis, storeUpdateMonitor.toString() );
            log.info( "Import completed, took " + Format.duration( totalTimeMillis ) + ". " + storeUpdateMonitor );
            hasBadEntries = badCollector.badEntries() > 0;
            if ( hasBadEntries )
            {
                log.warn( "There were " + badCollector.badEntries() + " bad entries which were skipped " +
                             "and logged into " + badFile.getAbsolutePath() );
            }
        }
        catch ( Throwable t )
        {
            log.error( "Error during import", t );
            throw Exceptions.launderedException( IOException.class, t );
        }
        finally
        {
            if ( nodeRelationshipCache != null )
            {
                nodeRelationshipCache.close();
            }
            if ( nodeLabelsCache != null )
            {
                nodeLabelsCache.close();
            }
            if ( !hasBadEntries )
            {
                fileSystem.deleteFile( badFile );
            }
        }
    }

    private long totalMemoryUsageOf( MemoryStatsVisitor.Visitable... users )
    {
        GatheringMemoryStatsVisitor total = new GatheringMemoryStatsVisitor();
        for ( MemoryStatsVisitor.Visitable user : users )
        {
            user.acceptMemoryStatsVisitor( total );
        }
        return total.getHeapUsage() + total.getOffHeapUsage();
    }

    private void importRelationships( NodeRelationshipCache nodeRelationshipCache,
            CountingStoreUpdateMonitor storeUpdateMonitor, BatchingNeoStores neoStore,
            IoMonitor writeMonitor, IdMapper idMapper, InputIterable<InputRelationship> relationships,
            InputCache inputCache, Object[] allRelationshipTypes, Object[] minorityRelationshipTypes )
    {
        // Imports the relationships from the Input. This isn't a straight forward as importing nodes,
        // since keeping track of and updating heads of relationship chains in scenarios where most nodes
        // are dense and there are many relationship types scales poorly w/ regards to cache memory usage
        // also as a side-effect time required to update this cache.
        //
        // The approach is instead to do multiple iterations where each iteration imports relationships
        // of a single type. For each iteration Node --> Relationship and Relationship --> Relationship
        // stages _for dense nodes only_ are run so that the cache can be reused to hold relationship chain heads
        // of the next type in the next iteration. All relationships will be imported this way and then
        // finally there will be one Node --> Relationship and Relationship --> Relationship stage linking
        // all sparse relationship chains together.

        Set<Object> minorityRelationshipTypeSet = asSet( minorityRelationshipTypes );
        PerTypeRelationshipSplitter perTypeIterator = new PerTypeRelationshipSplitter(
                relationships.iterator(),
                allRelationshipTypes,
                type -> minorityRelationshipTypeSet.contains( type ),
                neoStore.getRelationshipTypeRepository(),
                inputCache );

        long nextRelationshipId = 0;
        for ( int i = 0; perTypeIterator.hasNext(); i++ )
        {
            // Stage 3a -- relationships, properties
            nodeRelationshipCache.setForwardScan( true );
            Object currentType = perTypeIterator.currentType();
            int currentTypeId = neoStore.getRelationshipTypeRepository().getOrCreateId( currentType );

            InputIterator<InputRelationship> perType = perTypeIterator.next();
            String topic = " [:" + currentType + "] (" +
                    (i+1) + "/" + allRelationshipTypes.length + ")";
            final RelationshipStage relationshipStage = new RelationshipStage( topic, config, writeMonitor,
                    perType, idMapper, neoStore, nodeRelationshipCache, storeUpdateMonitor, nextRelationshipId );
            executeStages( relationshipStage );

            // Stage 4a -- set node nextRel fields for dense nodes
            executeStages( new NodeFirstRelationshipStage( topic, config, neoStore.getNodeStore(),
                    neoStore.getTemporaryRelationshipGroupStore(), nodeRelationshipCache, true/*dense*/,
                    currentTypeId ) );

            // Stage 5a -- link relationship chains together for dense nodes
            nodeRelationshipCache.setForwardScan( false );
            executeStages( new RelationshipLinkbackStage( topic, config, neoStore.getRelationshipStore(),
                    nodeRelationshipCache, nextRelationshipId,
                    relationshipStage.getNextRelationshipId(), true/*dense*/ ) );
            nextRelationshipId = relationshipStage.getNextRelationshipId();
            nodeRelationshipCache.clearChangedChunks( true/*dense*/ ); // cheap higher level clearing
        }

        String topic = " Sparse";
        nodeRelationshipCache.setForwardScan( true );
        // Stage 4b -- set node nextRel fields for sparse nodes
        executeStages( new NodeFirstRelationshipStage( topic, config, neoStore.getNodeStore(),
                neoStore.getTemporaryRelationshipGroupStore(), nodeRelationshipCache, false/*sparse*/, -1 ) );

        // Stage 5b -- link relationship chains together for sparse nodes
        nodeRelationshipCache.setForwardScan( false );
        executeStages( new RelationshipLinkbackStage( topic, config, neoStore.getRelationshipStore(),
                nodeRelationshipCache, 0, nextRelationshipId, false/*sparse*/ ) );

        if ( minorityRelationshipTypes.length > 0 )
        {
            // Do some batch insertion style random-access insertions for super small minority types
            executeStages( new BatchInsertRelationshipsStage( config, idMapper,
                    perTypeIterator.getMinorityRelationships(), neoStore, nextRelationshipId ) );
        }
    }

    private void executeStages( Stage... stages )
    {
        superviseExecution( executionMonitor, config, stages );
    }
}
