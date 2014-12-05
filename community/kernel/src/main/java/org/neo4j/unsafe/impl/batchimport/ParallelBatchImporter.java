/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.IOException;

import org.neo4j.function.Function;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Format;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.unsafe.impl.batchimport.cache.LongArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.NodeLabelsCache;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipLink;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipLinkImpl;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.DynamicProcessorAssigner;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisor;
import org.neo4j.unsafe.impl.batchimport.staging.IteratorBatcherStep;
import org.neo4j.unsafe.impl.batchimport.staging.MultiExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.staging.StageExecution;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStore;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.WriterFactory;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.WriterFactories.parallel;

/**
 * {@link BatchImporter} which tries to exercise as much of the available resources to gain performance.
 * Or rather ensure that the slowest resource (usually I/O) is fully saturated and that enough work is
 * being performed to keep that slowest resource saturated all the time.
 *
 * Overall goals: split up processing cost by parallelizing. Keep CPUs busy, keep I/O busy and writing sequentially.
 * I/O is only allowed to be read to and written from sequentially, any random access drastically reduces performance.
 * Goes through multiple stages where each stage has one or more steps executing in parallel, passing
 * batches between these steps through each stage, i.e. passing batches downstream.
 */
public class ParallelBatchImporter implements BatchImporter
{
    private final String storeDir;
    private final FileSystemAbstraction fileSystem;
    private final Configuration config;
    private final IoMonitor writeMonitor;
    private final ExecutionSupervisor executionPoller;
    private final Logging logging;
    private final StringLogger logger;
    private final Monitors monitors;
    private final WriterFactory writerFactory;
    private final AdditionalInitialIds highTokenIds;

    /**
     * Advanced usage of the parallel batch importer, for special and very specific cases. Please use
     * a constructor with fewer arguments instead.
     */
    public ParallelBatchImporter( String storeDir, FileSystemAbstraction fileSystem, Configuration config,
            Logging logging, ExecutionMonitor executionMonitor, Function<Configuration,WriterFactory> writerFactory,
            AdditionalInitialIds highTokenIds )
    {
        this.storeDir = storeDir;
        this.fileSystem = fileSystem;
        this.config = config;
        this.logging = logging;
        this.highTokenIds = highTokenIds;
        this.logger = logging.getMessagesLog( getClass() );
        this.executionPoller = new ExecutionSupervisor( Clock.SYSTEM_CLOCK, new MultiExecutionMonitor(
                executionMonitor, new DynamicProcessorAssigner( config, config.maxNumberOfProcessors() ) ) );
        this.monitors = new Monitors();
        this.writeMonitor = new IoMonitor();
        this.writerFactory = writerFactory.apply( config );
    }

    public ParallelBatchImporter( String storeDir, Configuration config, Logging logging,
            ExecutionMonitor executionMonitor )
    {
        this( storeDir, new DefaultFileSystemAbstraction(), config, logging, executionMonitor, parallel(), EMPTY );
    }

    @Override
    public void doImport( Input input ) throws IOException
    {
        logger.info( "Import starting" );

        long startTime = currentTimeMillis();
        try ( BatchingNeoStore neoStore = new BatchingNeoStore( fileSystem, storeDir, config,
                writeMonitor, logging, monitors, writerFactory, highTokenIds ) )
        {
            // Some temporary caches and indexes in the import
            IdMapper idMapper = input.idMapper();
            IdGenerator idGenerator = input.idGenerator();
            NodeRelationshipLink nodeRelationshipLink =
                    new NodeRelationshipLinkImpl( LongArrayFactory.AUTO, config.denseNodeThreshold() );
            final ResourceIterable<InputNode> nodes = input.nodes();
            final ResourceIterable<InputRelationship> relationships = input.relationships();

            // Stage 1 -- nodes, properties, labels
            final NodeStage nodeStage = new NodeStage( nodes, idMapper, idGenerator, neoStore );

            // Stage 2 -- calculate dense node threshold
            final CalculateDenseNodesStage calculateDenseNodesStage =
                    new CalculateDenseNodesStage( relationships, nodeRelationshipLink, idMapper );

            // Execute stages 1 and 2 in parallel or sequentially?
            if ( idMapper.needsPreparation() )
            {   // The id mapper of choice needs preparation in order to get ids from it,
                // So we need to execute the node stage first as it fills the id mapper and prepares it in the end,
                // before executing any stage that needs ids from the id mapper, for example calc dense node stage.
                executeStages( nodeStage );
                executeStages( calculateDenseNodesStage );
            }
            else
            {   // The id mapper of choice doesn't need any preparation, so we can go ahead and execute
                // the node and calc dense node stages in parallel.
                executeStages( nodeStage, calculateDenseNodesStage );
            }

            // Stage 3 -- relationships, properties
            final RelationshipStage relationshipStage =
                    new RelationshipStage( relationships, idMapper, neoStore, nodeRelationshipLink );

            // execute stage 3
            executeStages( relationshipStage );

            // Switch to reverse updating mode
            writerFactory.awaitEverythingWritten();
            neoStore.switchToUpdateMode();
            // Release IdMapper references since they are no longer needed, and so can be collected
            idMapper = null;
            idGenerator = null;

            // Stage 4 -- set node nextRel fields
            final NodeFirstRelationshipStage nodeFirstRelationshipStage =
                    new NodeFirstRelationshipStage( neoStore, nodeRelationshipLink );

            // execute stage 4
            executeStages( nodeFirstRelationshipStage );

            nodeRelationshipLink.clearRelationships();

            // Stage 5 -- link relationship chains together
            final RelationshipLinkbackStage relationshipLinkbackStage =
                    new RelationshipLinkbackStage( neoStore, nodeRelationshipLink );

            // execute stage 5
            executeStages( relationshipLinkbackStage );

            // Counts stages. The reason we're doing this as separate stages is that they require
            // as much, and different, memory as the node/relationship encoding stages
            // TODO OK so opportunity here: if we spot that there's at least as much memory available
            // as our current node --> relationship cache has allocated we can execute these count stages
            // in parallel with the link-back stages, or rather piggy-back on that processing directly.

            // Release this potentially really big piece of cached data
            nodeRelationshipLink = null;

            // Stage 6 -- count nodes per label and labels per node
            NodeLabelsCache countsCache = new NodeLabelsCache( LongArrayFactory.AUTO,
                    neoStore.getLabelRepository().getHighId() );
            final NodeCountsStage nodeCountsStage = new NodeCountsStage( neoStore, countsCache );
            executeStages( nodeCountsStage );

            // Stage 7 -- count label-[type]->label
            final RelationshipCountsStage relationshipCountsStage = new RelationshipCountsStage( neoStore, countsCache );
            executeStages( relationshipCountsStage );

            long totalTimeMillis = currentTimeMillis() - startTime;
            executionPoller.done( totalTimeMillis );
            logger.info( "Import completed, took " + Format.duration( totalTimeMillis ) );
        }
        catch ( Throwable t )
        {
            logger.error( "Error during import", t );
            throw Exceptions.launderedException( IOException.class, t );
        }
        finally
        {
            writerFactory.shutdown();
        }
    }

    private synchronized void executeStages( Stage... stages )
    {
        try
        {
            StageExecution[] executions = new StageExecution[stages.length];
            for ( int i = 0; i < stages.length; i++ )
            {
                executions[i] = stages[i].execute();
            }
            executionPoller.supervise( executions );
        }
        finally
        {
            for ( Stage stage : stages )
            {
                stage.close();
            }
        }
    }

    public class NodeStage extends Stage
    {
        public NodeStage( ResourceIterable<InputNode> nodes, IdMapper idMapper, IdGenerator idGenerator,
                          BatchingNeoStore neoStore )
        {
            super( "Nodes", config );
            add( new IteratorBatcherStep<>( control(), "INPUT", config.batchSize(), config.movingAverageSize(),
                    nodes.iterator() ) );

            NodeStore nodeStore = neoStore.getNodeStore();
            PropertyStore propertyStore = neoStore.getPropertyStore();
            Iterable<Object> allIds = new IterableWrapper<Object, InputNode>( nodes )
            {
                @Override
                protected Object underlyingObjectToObject( InputNode object )
                {
                    return object.id();
                }
            };
            add( new NodeEncoderStep( control(), config, idMapper, idGenerator,
                    neoStore.getLabelRepository(), nodeStore, allIds ) );
            add( new PropertyEncoderStep<>( control(), config, 1, neoStore.getPropertyKeyRepository(), propertyStore ) );
            add( new EntityStoreUpdaterStep<>( control(), "WRITER", config, nodeStore, propertyStore,
                    writeMonitor, writerFactory ) );
        }
    }

    public class CalculateDenseNodesStage extends Stage
    {
        public CalculateDenseNodesStage( ResourceIterable<InputRelationship> relationships,
                NodeRelationshipLink nodeRelationshipLink, IdMapper idMapper )
        {
            super( "Calculate dense nodes", config );
            add( new IteratorBatcherStep<>( control(), "INPUT", config.batchSize(), config.movingAverageSize(),
                    relationships.iterator() ) );

            add( new RelationshipPreparationStep( control(), config, idMapper ) );
            add( new CalculateDenseNodesStep( control(), config, nodeRelationshipLink ) );
        }
    }

    public class RelationshipStage extends Stage
    {
        public RelationshipStage( ResourceIterable<InputRelationship> relationships, IdMapper idMapper,
                BatchingNeoStore neoStore, NodeRelationshipLink nodeRelationshipLink )
        {
            super( "Relationships", config );
            add( new IteratorBatcherStep<>( control(), "INPUT", config.batchSize(), config.movingAverageSize(),
                    relationships.iterator() ) );

            RelationshipStore relationshipStore = neoStore.getRelationshipStore();
            PropertyStore propertyStore = neoStore.getPropertyStore();
            add( new RelationshipPreparationStep( control(), config, idMapper ) );
            add( new RelationshipEncoderStep( control(), config, idMapper,
                    neoStore.getRelationshipTypeRepository(), relationshipStore, nodeRelationshipLink ) );
            add( new PropertyEncoderStep<>( control(), config, 1, neoStore.getPropertyKeyRepository(), propertyStore ) );
            add( new EntityStoreUpdaterStep<>( control(), "WRITER", config,
                    relationshipStore, propertyStore, writeMonitor, writerFactory ) );
        }
    }

    public class NodeFirstRelationshipStage extends Stage
    {
        public NodeFirstRelationshipStage( BatchingNeoStore neoStore, NodeRelationshipLink nodeRelationshipLink )
        {
            super( "Node first rel", config );
            add( new NodeFirstRelationshipStep( control(), config,
                    neoStore.getNodeStore(), neoStore.getRelationshipGroupStore(), nodeRelationshipLink ) );
        }
    }

    public class RelationshipLinkbackStage extends Stage
    {
        public RelationshipLinkbackStage( BatchingNeoStore neoStore, NodeRelationshipLink nodeRelationshipLink )
        {
            super( "Relationship back link", config );
            add( new RelationshipLinkbackStep( control(), config,
                    neoStore.getRelationshipStore(), nodeRelationshipLink ) );
        }
    }

    public class NodeCountsStage extends Stage
    {
        public NodeCountsStage( BatchingNeoStore neoStore, NodeLabelsCache cache )
        {
            super( "Node counts", config );
            add( new NodeCountsStep( control(), config, neoStore.getNodeStore(), cache,
                    neoStore.getLabelRepository().getHighId(), neoStore.getCountsStore() ) );
        }
    }

    public class RelationshipCountsStage extends Stage
    {
        public RelationshipCountsStage( BatchingNeoStore neoStore, NodeLabelsCache cache )
        {
            super( "Relationship counts", config );
            add( new RelationshipCountsStep( control(), config, neoStore.getRelationshipStore(), cache,
                    neoStore.getLabelRepository().getHighId(), neoStore.getRelationshipTypeRepository().getHighId(),
                    neoStore.getCountsStore() ) );
        }
    }
}
