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
package org.neo4j.unsafe.impl.batchimport;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;

import static java.lang.String.format;

import static org.neo4j.unsafe.impl.batchimport.staging.SpectrumExecutionMonitor.fitInProgress;

/**
 * Imports data from {@link Input} into a store. Unlinked entity data and property data is imported here.
 * Linking records, except properties, with each other is not done in here.
 *
 * Main design goal here is low garbage generation and having as much as possible able to withstand multiple
 * threads passing through. So each import consists of instantiating an input source reader, optimal number
 * of threads and letting each thread:
 * <ol>
 * <li>Get {@link InputChunk chunk} of data and for every entity in it:</li>
 * <li>Parse its data, filling current record with data using {@link InputEntityVisitor} callback from parsing</li>
 * <li>Write record(s)</li>
 * <li>Repeat until no more chunks from input.</li>
 * </ol>
 */
public class DataImporter
{
    public static class Monitor
    {
        private final AtomicLong nodes = new AtomicLong();
        private final AtomicLong relationships = new AtomicLong();
        private final AtomicLong properties = new AtomicLong();

        public void nodesImported( long nodes )
        {
            this.nodes.addAndGet( nodes );
        }

        public void relationshipsImported( long relationships )
        {
            this.relationships.addAndGet( relationships );
        }

        public void propertiesImported( long properties )
        {
            this.properties.addAndGet( properties );
        }

        @Override
        public String toString()
        {
            return format( "Imported:%n  %d nodes%n  %d relationships%n  %d properties",
                    nodes.get(), relationships.get(), properties.get() );
        }
    }

    private static long importData( String title, int numRunners, InputIterator data,
            Supplier<EntityImporter> visitors )
            throws InterruptedException
    {
        System.out.println( "Importing " + title );
        AtomicLong roughEntityCountProgress = new AtomicLong();
        ExecutorService pool = Executors.newFixedThreadPool( numRunners );
        StageControl control = new PullBasedStageControl();
        for ( int i = 0; i < numRunners; i++ )
        {
            pool.submit( new ExhaustingEntityImporterRunnable(
                    control, data, visitors.get(), roughEntityCountProgress ) );
        }
        pool.shutdown();

        long entitiesLastReport = 0;
        int interval = 2;
        while ( !pool.awaitTermination( interval, TimeUnit.SECONDS ) )
        {
            long entities = roughEntityCountProgress.get();
            long entitiesDiff = (entities - entitiesLastReport) / interval;
            System.out.print( "\r" + fitInProgress( entities ) + " ∆" + fitInProgress( entitiesDiff ) + "/s" );
            entitiesLastReport = entities;
        }
        control.assertHealthy();
        return roughEntityCountProgress.get();
    }

    public static void importNodes( int numRunners, Input input, BatchingNeoStores stores, IdMapper idMapper,
            NodeRelationshipCache nodeRelationshipCache, Monitor monitor )
            throws InterruptedException
    {
        importData( "Nodes", numRunners, input.nodes(), () ->
            new NodeImporter( stores.getNeoStores(),
                    stores.getPropertyKeyRepository(), stores.getLabelRepository(), idMapper, monitor ) );
        nodeRelationshipCache.setHighNodeId( stores.getNodeStore().getHighId() );
    }

    public static RelationshipTypeDistribution importRelationships( int numRunners, Input input,
            BatchingNeoStores stores, IdMapper idMapper, NodeRelationshipCache nodeRelationshipCache,
            Collector badCollector, Monitor monitor ) throws InterruptedException
    {
        RelationshipTypeDistribution typeDistribution = new RelationshipTypeDistribution();
        importData( "Relationships", numRunners, input.relationships(), () ->
                new RelationshipImporter( stores.getNeoStores(),
                        stores.getPropertyKeyRepository(), stores.getRelationshipTypeRepository(), idMapper,
                        nodeRelationshipCache, typeDistribution, monitor, badCollector ) );
        return typeDistribution;
    }
}
