/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.unsafe.impl.batchimport.DataStatistics.RelationshipTypeCount;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.configuration.Config.defaults;
import static org.neo4j.kernel.impl.logging.NullLogService.getInstance;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores.batchingNeoStoresWithExternalPageCache;

public class ImportLogicTest
{
    @Rule
    public final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule();

    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void closeImporterWithoutDiagnosticState() throws IOException
    {
        ExecutionMonitor monitor = mock( ExecutionMonitor.class );
        try ( BatchingNeoStores stores = batchingNeoStoresWithExternalPageCache( storage.fileSystem(), storage.pageCache(), NULL,
                storage.directory().directory(), defaultFormat(), DEFAULT, getInstance(), EMPTY, defaults() ) )
        {
            //noinspection EmptyTryBlock
            try ( ImportLogic logic = new ImportLogic( storage.directory().directory(), storage.fileSystem(), stores, DEFAULT, getInstance(), monitor,
                    defaultFormat(), NO_MONITOR ) )
            {
                // nothing to run in this import
                logic.success();
            }
        }

        verify( monitor ).done( eq( true ), anyLong(), contains( "Data statistics is not available." ) );
    }

    @Test
    public void shouldSplitUpRelationshipTypesInBatches()
    {
        // GIVEN
        int denseNodeThreshold = 5;
        int numberOfNodes = 100;
        int numberOfTypes = 10;
        NodeRelationshipCache cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, denseNodeThreshold );
        cache.setNodeCount( numberOfNodes + 1 );
        Direction[] directions = Direction.values();
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            int count = random.nextInt( 1, denseNodeThreshold * 2 );
            cache.setCount( i, count, random.nextInt( numberOfTypes ), random.among( directions ) );
        }
        cache.countingCompleted();
        List<RelationshipTypeCount> types = new ArrayList<>();
        int numberOfRelationships = 0;
        for ( int i = 0; i < numberOfTypes; i++ )
        {
            int count = random.nextInt( 1, 100 );
            types.add( new RelationshipTypeCount( i, count ) );
            numberOfRelationships += count;
        }
        types.sort( ( t1, t2 ) -> Long.compare( t2.getCount(), t1.getCount() ) );
        DataStatistics typeDistribution =
                new DataStatistics( 0, 0, types.toArray( new RelationshipTypeCount[types.size()] ) );

        // WHEN enough memory for all types
        {
            long memory = cache.calculateMaxMemoryUsage( numberOfRelationships ) * numberOfTypes;
            int upToType = ImportLogic.nextSetOfTypesThatFitInMemory( typeDistribution, 0, memory, cache.getNumberOfDenseNodes() );

            // THEN
            assertEquals( types.size(), upToType );
        }

        // and WHEN less than enough memory for all types
        {
            long memory = cache.calculateMaxMemoryUsage( numberOfRelationships ) * numberOfTypes / 3;
            int startingFromType = 0;
            int rounds = 0;
            while ( startingFromType < types.size() )
            {
                rounds++;
                startingFromType = ImportLogic.nextSetOfTypesThatFitInMemory( typeDistribution, startingFromType, memory,
                        cache.getNumberOfDenseNodes() );
            }
            assertEquals( types.size(), startingFromType );
            assertThat( rounds, greaterThan( 1 ) );
        }
    }

    @Test
    public void shouldUseDataStatisticsCountsForPrintingFinalStats() throws IOException
    {
        // given
        ExecutionMonitor monitor = mock( ExecutionMonitor.class );
        try ( BatchingNeoStores stores = batchingNeoStoresWithExternalPageCache( storage.fileSystem(), storage.pageCache(), NULL,
                storage.directory().directory(), defaultFormat(), DEFAULT, getInstance(), EMPTY, defaults() ) )
        {
            // when
            RelationshipTypeCount[] relationshipTypeCounts = new RelationshipTypeCount[]
                    {
                            new RelationshipTypeCount( 0, 33 ),
                            new RelationshipTypeCount( 1, 66 )
                    };
            DataStatistics dataStatistics = new DataStatistics( 100123, 100456, relationshipTypeCounts );
            try ( ImportLogic logic = new ImportLogic( storage.directory().directory(), storage.fileSystem(), stores, DEFAULT, getInstance(), monitor,
                    defaultFormat(), NO_MONITOR ) )
            {
                logic.putState( dataStatistics );
                logic.success();
            }

            // then
            verify( monitor ).done( eq( true ), anyLong(), contains( dataStatistics.toString() ) );
        }
    }
}
