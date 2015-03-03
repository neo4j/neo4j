/**
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
package org.neo4j.unsafe.impl.batchimport;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.neo4j.function.Function;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Format;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.unsafe.impl.batchimport.cache.NodeLabelsCache;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipLink;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipLinkImpl;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.InputIteratorBatcherStep;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStore;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.WriterFactory;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Utils.idsOf;
import static org.neo4j.unsafe.impl.batchimport.WriterFactories.parallel;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors.superviseDynamicExecution;

/**
 * {@link BatchImporter} which tries to exercise as much of the available resources to gain performance.
 * Or rather ensure that the slowest resource (usually I/O) is fully saturated and that enough work is
 * being performed to keep that slowest resource saturated all the time.
 * <p/>
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
    private final Logging logging;
    private final StringLogger logger;
    private final ExecutionMonitor executionMonitor;
    private final Monitors monitors;
    private final WriterFactory writerFactory;
    private final AdditionalInitialIds additionalInitialIds;

    /**
     * Advanced usage of the parallel batch importer, for special and very specific cases. Please use
     * a constructor with fewer arguments instead.
     */
    public ParallelBatchImporter( String storeDir, FileSystemAbstraction fileSystem, Configuration config,
            Logging logging, ExecutionMonitor executionMonitor, Function<Configuration,WriterFactory> writerFactory,
            AdditionalInitialIds additionalInitialIds )
    {
        this.storeDir = storeDir;
        this.fileSystem = fileSystem;
        this.config = config;
        this.logging = logging;
        this.executionMonitor = executionMonitor;
        this.additionalInitialIds = additionalInitialIds;
        this.logger = logging.getMessagesLog( getClass() );
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

        // Things that we need to close later. The reason they're not in the try-with-resource statement
        // is that we need to close, and set to null, at specific points preferably. So use good ol' finally block.
        NodeRelationshipLink nodeRelationshipLink = null;
        NodeLabelsCache nodeLabelsCache = null;
        long startTime = currentTimeMillis();
        File badRelationshipsFile = new File( config.badFileName() );
        boolean hasBadRelationships = false;
        try ( BatchingNeoStore neoStore = new BatchingNeoStore( fileSystem, storeDir, config,
                writeMonitor, logging, monitors, writerFactory, additionalInitialIds );
              OutputStream badRelationshipsOutput = new BufferedOutputStream(
                      fileSystem.openAsOutputStream( badRelationshipsFile, false ) );
              Collector<InputRelationship> badRelationships =
                      input.badRelationshipsCollector( badRelationshipsOutput );
              CountsAccessor.Updater countsUpdater = neoStore.getCountsStore().reset() )
        {
            // Some temporary caches and indexes in the import
            IdMapper idMapper = input.idMapper();
            IdGenerator idGenerator = input.idGenerator();
            nodeRelationshipLink = new NodeRelationshipLinkImpl( AUTO, config.denseNodeThreshold() );
            final InputIterable<InputNode> nodes = input.nodes();
            final InputIterable<InputRelationship> relationships = input.relationships();

            // Stage 1 -- nodes, properties, labels
            final NodeStage nodeStage = new NodeStage( nodes, idMapper, idGenerator, neoStore );

            // Stage 2 -- calculate dense node threshold
            final CalculateDenseNodesStage calculateDenseNodesStage = new CalculateDenseNodesStage( relationships,
                    nodeRelationshipLink, idMapper, badRelationships );

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
            final RelationshipStage relationshipStage = new RelationshipStage( relationships, idMapper,
                    neoStore, nodeRelationshipLink, input.specificRelationshipIds() );
            executeStages( relationshipStage );

            // Prepare for updating
            neoStore.flush();
            writerFactory.awaitEverythingWritten();
            nodeLabelsCache = new NodeLabelsCache( AUTO, neoStore.getLabelRepository().getHighId() );

            // Stage 4 -- set node nextRel fields
            executeStages( new NodeFirstRelationshipStage( config, neoStore.getNodeStore(),
                    neoStore.getRelationshipGroupStore(), nodeRelationshipLink ) );
            // Stage 5 -- link relationship chains together
            nodeRelationshipLink.clearRelationships();
            executeStages( new RelationshipLinkbackStage( config, neoStore.getRelationshipStore(),
                    nodeRelationshipLink ) );

            // Release this potentially really big piece of cached data
            nodeRelationshipLink.close();
            nodeRelationshipLink = null;

            // Stage 6 -- count nodes per label and labels per node
            executeStages( new NodeCountsStage( config, nodeLabelsCache, neoStore.getNodeStore(),
                    neoStore.getLabelRepository().getHighId(), countsUpdater ) );
            // Stage 7 -- count label-[type]->label
            executeStages( new RelationshipCountsStage( config, nodeLabelsCache, neoStore.getRelationshipStore(),
                    neoStore.getLabelRepository().getHighId(),
                    neoStore.getRelationshipTypeRepository().getHighId(), countsUpdater ) );

            // We're done, do some final logging about it
            long totalTimeMillis = currentTimeMillis() - startTime;
            executionMonitor.done( totalTimeMillis );
            logger.info( "Import completed, took " + Format.duration( totalTimeMillis ) );
            hasBadRelationships = badRelationships.badEntries() > 0;
            if ( hasBadRelationships )
            {
                logger.warn( "There were " + badRelationships.badEntries() + " bad relationships which were skipped " +
                             "and logged into " + badRelationshipsFile.getAbsolutePath() );
            }
        }
        catch ( Throwable t )
        {
            logger.error( "Error during import", t );
            throw Exceptions.launderedException( IOException.class, t );
        }
        finally
        {
            writerFactory.shutdown();
            if ( nodeRelationshipLink != null )
            {
                nodeRelationshipLink.close();
            }
            if ( nodeLabelsCache != null )
            {
                nodeLabelsCache.close();
            }
            if ( !hasBadRelationships )
            {
                fileSystem.deleteFile( badRelationshipsFile );
            }
        }
    }

    private void executeStages( Stage... stages )
    {
        superviseDynamicExecution( executionMonitor, config, stages );
    }

    public class NodeStage extends Stage
    {
        public NodeStage( InputIterable<InputNode> nodes, IdMapper idMapper, IdGenerator idGenerator,
                BatchingNeoStore neoStore )
        {
            super( "Nodes", config, idGenerator.dependsOnInput() );
            add( new InputIteratorBatcherStep<>( control(), config.batchSize(), config.movingAverageSize(),
                    nodes.iterator(), InputNode.class ) );

            NodeStore nodeStore = neoStore.getNodeStore();
            PropertyStore propertyStore = neoStore.getPropertyStore();
            add( new PropertyEncoderStep<>( control(), config, 1, neoStore.getPropertyKeyRepository(),
                    propertyStore ) );
            add( new NodeEncoderStep( control(), config, idMapper, idGenerator,
                    neoStore.getLabelRepository(), nodeStore, idsOf( nodes ) ) );
            add( new EntityStoreUpdaterStep<>( control(), config, nodeStore, propertyStore,
                    writeMonitor, writerFactory ) );
        }
    }

    public class CalculateDenseNodesStage extends Stage
    {
        public CalculateDenseNodesStage( InputIterable<InputRelationship> relationships,
                NodeRelationshipLink nodeRelationshipLink, IdMapper idMapper,
                Collector<InputRelationship> badRelationshipsCollector )
        {
            super( "Calculate dense nodes", config, false );
            add( new InputIteratorBatcherStep<>( control(), config.batchSize(), config.movingAverageSize(),
                    relationships.iterator(), InputRelationship.class ) );

            add( new RelationshipPreparationStep( control(), config, idMapper ) );
            add( new CalculateDenseNodesStep( control(), config, nodeRelationshipLink, badRelationshipsCollector ) );
        }
    }

    public class RelationshipStage extends Stage
    {
        public RelationshipStage( InputIterable<InputRelationship> relationships, IdMapper idMapper,
                BatchingNeoStore neoStore, NodeRelationshipLink nodeRelationshipLink, boolean specificIds )
        {
            super( "Relationships", config, false );
            add( new InputIteratorBatcherStep<>( control(), config.batchSize(), config.movingAverageSize(),
                    relationships.iterator(), InputRelationship.class ) );

            RelationshipStore relationshipStore = neoStore.getRelationshipStore();
            PropertyStore propertyStore = neoStore.getPropertyStore();
            add( new RelationshipPreparationStep( control(), config, idMapper ) );
            add( new PropertyEncoderStep<>( control(), config, 1, neoStore.getPropertyKeyRepository(),
                    propertyStore ) );
            add( new RelationshipEncoderStep( control(), config,
                    neoStore.getRelationshipTypeRepository(), relationshipStore, nodeRelationshipLink, specificIds ) );
            add( new EntityStoreUpdaterStep<>( control(), config,
                    relationshipStore, propertyStore, writeMonitor, writerFactory ) );
        }
    }
}
