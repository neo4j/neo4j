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
import java.util.Iterator;

import org.neo4j.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.unsafe.impl.batchimport.cache.LongArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipLink;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipLinkImpl;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapping;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.IteratorBatcherStep;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.staging.StageExecution;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStore;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.WriterFactory;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;
import org.neo4j.unsafe.impl.batchimport.store.io.IoQueue;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.SYNCHRONOUS;

/**
 * Overall goals: split up processing cost by parallelizing. Keep CPUs busy, keep I/O busy and writing sequentially.
 * Goes through multiple stages where each stage has one or more steps executing in parallel, passing
 * batches downstream.
 */
public class ParallelBatchImporter implements BatchImporter
{
    private final String storeDir;
    private final FileSystemAbstraction fileSystem;
    private final Configuration config;
    private final IoMonitor writeMonitor;
    private final ExecutionMonitor executionMonitor;
    private final Logging logging;
    private final StringLogger logger;
    private final LifeSupport life = new LifeSupport();
    private final Monitors monitors;
    private final WriterFactory writerFactory;

    ParallelBatchImporter( String storeDir, FileSystemAbstraction fileSystem, Configuration config,
            Logging logging, ExecutionMonitor executionMonitor, WriterFactory writerFactory )
    {
        this.storeDir = storeDir;
        this.fileSystem = fileSystem;
        this.config = config;
        this.logging = logging;
        this.logger = logging.getMessagesLog( getClass() );
        this.executionMonitor = executionMonitor;
        this.monitors = new Monitors();
        this.writeMonitor = new IoMonitor();
        this.writerFactory = writerFactory;

        life.start();
    }

    public ParallelBatchImporter( String storeDir, FileSystemAbstraction fileSystem,
                                  Configuration config, Logging logging, ExecutionMonitor executionMonitor )
    {
        this( storeDir, fileSystem, config, logging, executionMonitor,
                new IoQueue( config.numberOfIoThreads(), SYNCHRONOUS ) );
    }

    @Override
    public void doImport( Iterable<InputNode> nodes, Iterable<InputRelationship> relationships,
                          IdMapping idMapping ) throws IOException
    {
        logger.info( "Import starting" );

        long startTime = currentTimeMillis();
        try ( BatchingNeoStore neoStore = new BatchingNeoStore( fileSystem, storeDir, config,
                writeMonitor, logging, monitors, writerFactory ) )
        {
            // Some temporary caches and indexes in the import
            IdMapper idMapper = idMapping.idMapper();
            IdGenerator idGenerator = idMapping.idGenerator();
            NodeRelationshipLink nodeRelationshipLink = new NodeRelationshipLinkImpl(
                    LongArrayFactory.AUTO, config.denseNodeThreshold() );

            // Stage 1 -- nodes, properties, labels
            NodeStage nodeStage = new NodeStage( nodes.iterator(), idMapper, idGenerator, neoStore );

            // Stage 2 -- calculate dense node threshold
            CalculateDenseNodesStage calculateDenseNodesStage = new CalculateDenseNodesStage(
                    relationships.iterator(), nodeRelationshipLink, idMapper );

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
                // the node and calc dende node stages in parallel.
                executeStages( nodeStage, calculateDenseNodesStage );
            }

            // Stage 3 -- relationships, properties
            executeStages( new RelationshipStage( relationships.iterator(), idMapper,
                    neoStore, nodeRelationshipLink ) );

            // Switch to reverse updating mode
            writerFactory.awaitEverythingWritten();
            neoStore.switchToUpdateMode();

            // Stage 4 -- set node nextRel fields
            executeStages( new NodeFirstRelationshipStage( neoStore, nodeRelationshipLink ) );

            // Stage 5 -- link relationship chains together
            nodeRelationshipLink.clearRelationships();
            executeStages( new RelationshipLinkbackStage( neoStore, nodeRelationshipLink ) );

            executionMonitor.done( currentTimeMillis() - startTime );
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

        // TODO add import starts to this log message
        logger.info( "Import completed" );
    }

    private synchronized void executeStages( Stage... stages ) throws Exception
    {
        StageExecution[] executions = new StageExecution[stages.length];
        for ( int i = 0; i < stages.length; i++ )
        {
            executions[i] = stages[i].execute();
        }

        executionMonitor.monitor( executions );
    }

    @Override
    public void shutdown()
    {
        logger.debug( "Importer shutting down" );
        life.shutdown();
        logger.info( "Importer shut down." );
    }

    public class NodeStage extends Stage
    {
        public NodeStage( Iterator<InputNode> input, IdMapper idMapper, IdGenerator idGenerator,
                          BatchingNeoStore neoStore )
        {
            super( logging, "Nodes", config );
            input( new IteratorBatcherStep<>( control(), "INPUT", config.batchSize(), input ) );

            NodeStore nodeStore = neoStore.getNodeStore();
            PropertyStore propertyStore = neoStore.getPropertyStore();
            add( new NodeEncoderStep( control(), "ENCODER", config.workAheadSize(), 1, idMapper, idGenerator,
                    neoStore.getPropertyKeyRepository(), neoStore.getLabelRepository(),
                    nodeStore, propertyStore ) );
            add( new EntityStoreUpdaterStep<>( control(), "WRITER", nodeStore, propertyStore, writeMonitor ) );
        }
    }

    public class CalculateDenseNodesStage extends Stage
    {
        public CalculateDenseNodesStage( Iterator<InputRelationship> input, NodeRelationshipLink nodeRelationshipLink,
                IdMapper idMapper )
        {
            super( logging, "Calculate dense nodes", config );
            input( new IteratorBatcherStep<>( control(), "INPUT", config.batchSize(), input ) );

            add( new CalculateDenseNodesStep( control(), config.workAheadSize(), nodeRelationshipLink,
                    idMapper, logger ) );
        }
    }

    public class RelationshipStage extends Stage
    {
        public RelationshipStage( Iterator<InputRelationship> input, IdMapper idMapper, BatchingNeoStore neoStore,
                                  NodeRelationshipLink nodeRelationshipLink )
        {
            super( logging, "Relationships", config );
            input( new IteratorBatcherStep<>( control(), "INPUT", config.batchSize(), input ) );

            RelationshipStore relationshipStore = neoStore.getRelationshipStore();
            PropertyStore propertyStore = neoStore.getPropertyStore();
            add( new RelationshipEncoderStep( control(), "ENCODER", config.workAheadSize(), 1, idMapper,
                    neoStore.getPropertyKeyRepository(), neoStore.getRelationshipTypeRepository(),
                    relationshipStore, propertyStore, nodeRelationshipLink ) );
            add( new EntityStoreUpdaterStep<>( control(), "WRITER", relationshipStore, propertyStore, writeMonitor ) );
        }
    }

    public class NodeFirstRelationshipStage extends Stage
    {
        public NodeFirstRelationshipStage( BatchingNeoStore neoStore, NodeRelationshipLink nodeRelationshipLink )
        {
            super( logging, "Node first rel", config );
            input( new NodeFirstRelationshipStep( control(), config.batchSize(),
                    neoStore.getNodeStore(), neoStore.getRelationshipGroupStore(), nodeRelationshipLink ) );
        }
    }

    public class RelationshipLinkbackStage extends Stage
    {
        public RelationshipLinkbackStage( BatchingNeoStore neoStore, NodeRelationshipLink nodeRelationshipLink )
        {
            super( logging, "Relationship back link", config );
            input( new RelationshipLinkbackStep( control(), config.batchSize(),
                    neoStore.getRelationshipStore(), nodeRelationshipLink ) );
        }
    }
}
