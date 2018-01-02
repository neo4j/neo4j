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
package org.neo4j.unsafe.impl.batchimport;

import java.io.File;
import java.io.IOException;

import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Format;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;
import org.neo4j.unsafe.impl.batchimport.cache.NodeLabelsCache;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputCache;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.DynamicProcessorAssigner;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO;
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
            AdditionalInitialIds additionalInitialIds, Config dbConfig )
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
        try ( BatchingNeoStores neoStore =
                      new BatchingNeoStores( fileSystem, storeDir, config, logService, additionalInitialIds, dbConfig );
              CountsAccessor.Updater countsUpdater = neoStore.getCountsStore().reset(
                    neoStore.getLastCommittedTransactionId() );
              InputCache inputCache = new InputCache( fileSystem, storeDir ) )
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

            // Stage 1 -- nodes, properties, labels
            NodeStage nodeStage = new NodeStage( config, writeMonitor,
                    nodes, idMapper, idGenerator, neoStore, inputCache, neoStore.getLabelScanStore(),
                    storeUpdateMonitor, memoryUsageStats );

            // Stage 2 -- calculate dense node threshold
            CalculateDenseNodesStage calculateDenseNodesStage = new CalculateDenseNodesStage( config, relationships,
                    nodeRelationshipCache, idMapper, badCollector, inputCache );

            // Execute stages 1 and 2 in parallel or sequentially?
            if ( idMapper.needsPreparation() )
            {   // The id mapper of choice needs preparation in order to get ids from it,
                // So we need to execute the node stage first as it fills the id mapper and prepares it in the end,
                // before executing any stage that needs ids from the id mapper, for example calc dense node stage.
                executeStages( nodeStage );
                executeStages( new IdMapperPreparationStage( config, idMapper, nodes, inputCache,
                        badCollector, memoryUsageStats ) );
                executeStages( calculateDenseNodesStage );
            }
            else
            {   // The id mapper of choice doesn't need any preparation, so we can go ahead and execute
                // the node and calc dense node stages in parallel.
                executeStages( nodeStage, calculateDenseNodesStage );
            }
            nodeRelationshipCache.fixateNodes();

            // Stage 3 -- relationships, properties
            final RelationshipStage relationshipStage = new RelationshipStage( config, writeMonitor,
                    relationships.supportsMultiplePasses() ? relationships : inputCache.relationships(),
                    idMapper, neoStore, nodeRelationshipCache, input.specificRelationshipIds(), storeUpdateMonitor );
            executeStages( relationshipStage );
            nodeRelationshipCache.fixateGroups();

            // Stage 4 -- set node nextRel fields
            executeStages( new NodeFirstRelationshipStage( config, neoStore.getNodeStore(),
                    neoStore.getRelationshipGroupStore(), nodeRelationshipCache, badCollector,
                    neoStore.getLabelScanStore() ) );
            // Stage 5 -- link relationship chains together
            nodeRelationshipCache.clearRelationships();
            executeStages( new RelationshipLinkbackStage( config, neoStore.getRelationshipStore(),
                    nodeRelationshipCache ) );

            // Release this potentially really big piece of cached data
            nodeRelationshipCache.close();
            nodeRelationshipCache = null;

            // Stage 6 -- count nodes per label and labels per node
            nodeLabelsCache = new NodeLabelsCache( AUTO, neoStore.getLabelRepository().getHighId() );
            memoryUsageStats = new MemoryUsageStatsProvider( nodeLabelsCache );
            executeStages( new NodeCountsStage( config, nodeLabelsCache, neoStore.getNodeStore(),
                    neoStore.getLabelRepository().getHighId(), countsUpdater, memoryUsageStats ) );
            // Stage 7 -- count label-[type]->label
            executeStages( new RelationshipCountsStage( config, nodeLabelsCache, neoStore.getRelationshipStore(),
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

    private void executeStages( Stage... stages )
    {
        superviseExecution( executionMonitor, config, stages );
    }
}
