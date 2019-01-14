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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.junit.Rule;
import org.junit.Test;

import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.csv.reader.Extractors;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.DataGeneratorInput;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.staging.HumanUnderstandableExecutionMonitor.ImportStage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.configuration.Config.defaults;
import static org.neo4j.kernel.impl.store.format.standard.Standard.LATEST_RECORD_FORMATS;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.input.DataGeneratorInput.bareboneNodeHeader;
import static org.neo4j.unsafe.impl.batchimport.input.DataGeneratorInput.bareboneRelationshipHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.IdType.INTEGER;
import static org.neo4j.unsafe.impl.batchimport.staging.HumanUnderstandableExecutionMonitor.NO_EXTERNAL_MONITOR;

public class HumanUnderstandableExecutionMonitorIT
{
    private static final long NODE_COUNT = 1_000;
    private static final long RELATIONSHIP_COUNT = 10_000;

    @Rule
    public final RandomRule random = new RandomRule();

    @Rule
    public final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule();

    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void shouldReportProgressOfNodeImport() throws Exception
    {
        // given
        CapturingMonitor progress = new CapturingMonitor();
        HumanUnderstandableExecutionMonitor monitor = new HumanUnderstandableExecutionMonitor( progress, NO_EXTERNAL_MONITOR );
        IdType idType = INTEGER;
        Input input = new DataGeneratorInput( NODE_COUNT, RELATIONSHIP_COUNT, idType, Collector.EMPTY, random.seed(),
                0, bareboneNodeHeader( idType, new Extractors( ';' ) ), bareboneRelationshipHeader( idType, new Extractors( ';' ) ),
                1, 1, 0, 0 );

        // when
        new ParallelBatchImporter( storage.directory().absolutePath(), storage.fileSystem(), storage.pageCache(), DEFAULT,
                NullLogService.getInstance(), monitor, EMPTY, defaults(), LATEST_RECORD_FORMATS, NO_MONITOR ).doImport( input );

        // then
        progress.assertAllProgressReachedEnd();
    }

    private static class CapturingMonitor implements HumanUnderstandableExecutionMonitor.Monitor
    {
        final EnumMap<ImportStage,AtomicInteger> progress = new EnumMap<>( ImportStage.class );

        @Override
        public void progress( ImportStage stage, int percent )
        {
            if ( percent > 100 )
            {
                fail( "Expected percentage to be 0..100% but was " + percent );
            }

            AtomicInteger stageProgress = progress.computeIfAbsent( stage, s -> new AtomicInteger() );
            int previous = stageProgress.getAndSet( percent );
            if ( previous > percent )
            {
                fail( "Progress should go forwards only, but went from " + previous + " to " + percent );
            }
        }

        void assertAllProgressReachedEnd()
        {
            assertEquals( ImportStage.values().length, progress.size() );
            progress.values().forEach( p -> assertEquals( 100, p.get() ) );
        }
    }
}
