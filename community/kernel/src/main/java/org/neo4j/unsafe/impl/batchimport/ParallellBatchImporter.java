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
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.unsafe.impl.batchimport.cache.LongArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.NodeIdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.NodeIdMapping;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipLink;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipLinkImpl;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.ProducerStep;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStore;
import org.neo4j.unsafe.impl.batchimport.store.IoMonitor;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_dir;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.logging.DefaultLogging.createDefaultLogging;

/**
 * Overall goals: split up processing cost by parallelizing. Keep CPUs busy, keep I/O busy and writing sequentially.
 * Goes through multiple stages where each stage has one or more steps executing in parallel, passing
 * batches downstream.
 */
public class ParallellBatchImporter implements BatchImporter
{
    private final String storeDir;
    private final FileSystemAbstraction fileSystem;
    private final Configuration config;
    private final IoMonitor writeMonitor;
    private final ExecutionMonitor executionMonitor;
    private final Logging logging;
    private final ConsoleLogger logger;
    private final LifeSupport life = new LifeSupport();
    private final Monitors monitors;

    public ParallellBatchImporter( String storeDir, FileSystemAbstraction fileSystem, Configuration config,
            Iterable<KernelExtensionFactory<?>> kernelExtensions, ExecutionMonitor executionMonitor )
    {
        this.storeDir = storeDir;
        this.fileSystem = fileSystem;
        this.config = config;
        this.logging = life.add( createDefaultLogging( stringMap( store_dir.name(), storeDir ) ) );
        this.logger = logging.getConsoleLog( getClass() );
        this.executionMonitor = executionMonitor;
        this.monitors = new Monitors();
        this.writeMonitor = new IoMonitor();

        life.start();
    }

    @Override
    public void doImport( Iterable<InputNode> nodes, Iterable<InputRelationship> relationships,
            NodeIdMapping nodeIdMapping ) throws IOException
    {
        // TODO log about import starting

        try ( BatchingNeoStore neoStore = new BatchingNeoStore( fileSystem, storeDir, config,
                logging, monitors, writeMonitor ) )
        {
            NodeIdMapper nodeIdMapper = nodeIdMapping.mapper( neoStore.getNodeStore() );

            // Stage 1 -- nodes, properties, labels
            executeStage( new NodeStage( nodeIdMapper.wrapNodes( nodes.iterator() ), neoStore ) );

            // Stage 2 -- calculate dense node threshold
            NodeRelationshipLink nodeRelationshipLink = new NodeRelationshipLinkImpl(
                    LongArrayFactory.AUTO, neoStore.getNodeStore().getHighId(), config.denseNodeThreshold() );
            executeStage( new CalculateDenseNodesStage( relationships.iterator(), neoStore,
                    nodeRelationshipLink ) );

            // Stage 3 -- relationships, properties
            executeStage( new RelationshipStage( relationships, neoStore, nodeRelationshipLink ) );

            // Switch to reverse updating mode
            neoStore.switchNodeAndRelationshipStoresToReverseUpdatingMode();

            // Stage 4 -- set node nextRel fields
            executeStage( new NodeFirstRelationshipStage( neoStore, nodeRelationshipLink ) );

            // Stage 5 -- link relationship chains together
            nodeRelationshipLink.clearRelationships();
            executeStage( new RelationshipLinkbackStage( neoStore, nodeRelationshipLink ) );

            executionMonitor.done();

            logger.log( "Import completed [TODO import stats]" );
        }
        catch ( Throwable t )
        {
            logger.error( "Error during import", t );
            throw Exceptions.launderedException( IOException.class, t );
        }
    }

    private synchronized void executeStage( Stage<?> stage ) throws Exception
    {
        executionMonitor.monitor( stage.execute() );
    }

    @Override
    public void shutdown()
    {
        logger.log( "Shutting down" );
        life.shutdown();
        logger.log( "Successfully shut down [TODO store stats?]" );
    }

    public class NodeStage extends Stage<Iterator<InputNode>>
    {
        public NodeStage( Iterator<InputNode> input, BatchingNeoStore neoStore )
        {
            super( logging, "Nodes", config );
            input( new ProducerStep<InputNode>( control(), "INPUT", config.batchSize() ), input );

            NodeStore nodeStore = neoStore.getNodeStore();
            PropertyStore propertyStore = neoStore.getPropertyStore();
            add( new NodeEncoderStep( control(), "ENCODER", config.workAheadSize(), 1,
                    neoStore.getPropertyKeyRepository(), neoStore.getLabelRepository(), nodeStore, propertyStore ) );
            add( new EntityStoreUpdaterStep<>( control(), "WRITER", nodeStore, propertyStore, writeMonitor ) );
        }
    }

    public class CalculateDenseNodesStage extends Stage<Iterator<InputRelationship>>
    {
        public CalculateDenseNodesStage( Iterator<InputRelationship> input,
                BatchingNeoStore neoStore, NodeRelationshipLink nodeRelationshipLink )
        {
            super( logging, "Calculate dense nodes", config );
            input( new ProducerStep<InputRelationship>( control(), "INPUT", config.batchSize() ), input );

            add( new CalculateDenseNodesStep( control(), config.workAheadSize(), nodeRelationshipLink ) );
        }
    }

    public class RelationshipStage extends Stage<Iterator<InputRelationship>>
    {
        public RelationshipStage( Iterable<InputRelationship> input, BatchingNeoStore neoStore,
                NodeRelationshipLink nodeRelationshipLink )
        {
            super( logging, "Relationships", config );
            input( new ProducerStep<InputRelationship>( control(), "INPUT", config.batchSize() ), input.iterator() );

            RelationshipStore relationshipStore = neoStore.getRelationshipStore();
            PropertyStore propertyStore = neoStore.getPropertyStore();
            add( new RelationshipEncoderStep( control(), "ENCODER", config.workAheadSize(), 1,
                    neoStore.getPropertyKeyRepository(), neoStore.getRelationshipTypeRepository(),
                    relationshipStore, propertyStore, nodeRelationshipLink ) );
            add( new EntityStoreUpdaterStep<>( control(), "WRITER", relationshipStore, propertyStore, writeMonitor ) );
        }
    }

    public class NodeFirstRelationshipStage extends Stage<Void>
    {
        public NodeFirstRelationshipStage( BatchingNeoStore neoStore, NodeRelationshipLink nodeRelationshipLink )
        {
            super( logging, "Node first rel", config );
            input( new NodeFirstRelationshipStep( control(), config.batchSize(),
                    neoStore.getNodeStore(), neoStore.getRelationshipGroupStore(), nodeRelationshipLink ), null );
        }
    }

    public class RelationshipLinkbackStage extends Stage<Void>
    {
        public RelationshipLinkbackStage( BatchingNeoStore neoStore, NodeRelationshipLink nodeRelationshipLink )
        {
            super( logging, "Relationship back link", config );
            input( new RelationshipLinkbackStep( control(), config.batchSize(),
                    neoStore.getRelationshipStore(), nodeRelationshipLink ), null );
        }
    }
}
