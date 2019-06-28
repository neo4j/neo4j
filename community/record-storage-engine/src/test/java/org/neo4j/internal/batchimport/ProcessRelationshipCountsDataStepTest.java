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
package org.neo4j.internal.batchimport;

import org.junit.jupiter.api.Test;

import org.neo4j.common.ProgressReporter;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.internal.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.internal.batchimport.cache.NodeLabelsCache;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.batchimport.staging.SimpleStageControl;
import org.neo4j.internal.batchimport.staging.StageControl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.ByteUnit.tebiBytes;

class ProcessRelationshipCountsDataStepTest
{
    @Test
    void shouldLetProcessorsBeZeroIfEnoughMemory()
    {
        // given
        ProcessRelationshipCountsDataStep step = instantiateStep( 10, 10, 10_000, 4, mebiBytes( 10 ) );

        // then
        assertEquals( 0, step.getMaxProcessors() );
    }

    @Test
    void shouldNotOverflowWhenTooMuchMemoryAvailable()
    {
        // given
        ProcessRelationshipCountsDataStep step = instantiateStep( 1, 1, 10_000, 64, tebiBytes( 10 ) );

        // then
        assertEquals( 0, step.getMaxProcessors() );
    }

    @Test
    void shouldLimitProcessorsIfScarceMemory()
    {
        // given labels/types amounting to ~360k, 2MiB max mem and 1MiB in use by node-label cache
        ProcessRelationshipCountsDataStep step = instantiateStep( 100, 220, mebiBytes( 1 ), 4, mebiBytes( 2 ) );

        // then
        assertEquals( 2, step.getMaxProcessors() );
    }

    @Test
    void shouldAtLeastHaveOneProcessorEvenIfLowMemory()
    {
        // given labels/types amounting to ~1.6MiB, 2MiB max mem and 1MiB in use by node-label cache
        ProcessRelationshipCountsDataStep step = instantiateStep( 1_000, 1_000, mebiBytes( 1 ), 4, mebiBytes( 2 ) );

        // then
        assertEquals( 1, step.getMaxProcessors() );
    }

    private ProcessRelationshipCountsDataStep instantiateStep( int highLabelId, int highRelationshipTypeId, long labelCacheSize, int maxProcessors,
            long maxMemory )
    {
        StageControl control = new SimpleStageControl();
        NodeLabelsCache cache = nodeLabelsCache( labelCacheSize );
        Configuration config = mock( Configuration.class );
        when( config.maxNumberOfProcessors() ).thenReturn( maxProcessors );
        when( config.maxMemoryUsage() ).thenReturn( maxMemory );
        return new ProcessRelationshipCountsDataStep( control, cache, config, highLabelId, highRelationshipTypeId,
            mock( CountsAccessor.Updater.class ), NumberArrayFactory.OFF_HEAP, ProgressReporter.SILENT );
    }

    private NodeLabelsCache nodeLabelsCache( long sizeInBytes )
    {
        NodeLabelsCache cache = mock( NodeLabelsCache.class );
        doAnswer( invocation ->
        {
            MemoryStatsVisitor visitor = invocation.getArgument( 0 );
            visitor.offHeapUsage( sizeInBytes );
            return null;
        } ).when( cache ).acceptMemoryStatsVisitor( any() );
        return cache;
    }
}
