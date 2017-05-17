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
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;

import static org.neo4j.unsafe.impl.batchimport.staging.SpectrumExecutionMonitor.fitInProgress;

public class DataImporter
{
    private static void importData( String title, int numRunners, InputIterator data,
            Supplier<EntityImporter> visitors )
            throws InterruptedException
    {
        System.out.println( "Importing " + title );
        AtomicLong entitiesCallback = new AtomicLong();
        ExecutorService pool = Executors.newFixedThreadPool( numRunners );
        for ( int i = 0; i < numRunners; i++ )
        {
            pool.submit( new ExhaustingEntityImporterRunnable( data, visitors.get(), entitiesCallback ) );
        }
        pool.shutdown();

        long entitiesLastReport = 0;
        int interval = 2;
        while ( !pool.awaitTermination( interval, TimeUnit.SECONDS ) )
        {
            long entities = entitiesCallback.get();
            long entitiesDiff = (entities - entitiesLastReport) / interval;
            System.out.print( "\r" + fitInProgress( entities ) + " ∆" + fitInProgress( entitiesDiff ) + "/s" );
            entitiesLastReport = entities;
        }
    }

    public static void importNodes( int numRunners, Input input, BatchingNeoStores stores, IdMapper idMapper,
            NodeRelationshipCache nodeRelationshipCache )
            throws InterruptedException
    {
        importData( "Nodes", numRunners, input.nodes(), () ->
            new NodeImporter( stores.getNeoStores(),
                    stores.getPropertyKeyRepository(), stores.getLabelRepository(), idMapper ) );
        nodeRelationshipCache.setHighNodeId( stores.getNodeStore().getHighId() );
    }

    public static RelationshipTypeDistribution importRelationships( int numRunners, Input input,
            BatchingNeoStores stores, IdMapper idMapper, NodeRelationshipCache nodeRelationshipCache )
                    throws InterruptedException
    {
        RelationshipTypeDistribution typeDistribution = new RelationshipTypeDistribution();
        importData( "Relationships", numRunners, input.relationships(), () ->
                new RelationshipImporter( stores.getNeoStores(),
                        stores.getPropertyKeyRepository(), stores.getRelationshipTypeRepository(), idMapper,
                        nodeRelationshipCache, typeDistribution ) );
        return typeDistribution;
    }
}
