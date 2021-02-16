/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.api.index;

import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " IndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending TokenIndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public class TokenIndexPopulatorCompatibility extends TokenIndexProviderCompatibilityTestSuite.Compatibility
{
    public TokenIndexPopulatorCompatibility( TokenIndexProviderCompatibilityTestSuite testSuite )
    {
        super( testSuite, testSuite.indexPrototype() );
    }

    @Test
    public void shouldStorePopulationFailedForRetrievalFromProviderLater() throws Exception
    {
        // GIVEN
        String failure = "The contrived failure";
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.defaults() );
        // WHEN (this will attempt to call close)
        withPopulator( indexProvider.getPopulator( descriptor, indexSamplingConfig, heapBufferFactory( 1024 ), INSTANCE, tokenNameLookup ),
                p -> p.markAsFailed( failure ), false );
        // THEN
        assertThat( indexProvider.getPopulationFailure( descriptor, NULL ) ).contains( failure );
    }

    @Test
    public void shouldReportInitialStateAsFailedIfPopulationFailed() throws Exception
    {
        // GIVEN
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.defaults() );
        withPopulator( indexProvider.getPopulator( descriptor, indexSamplingConfig, heapBufferFactory( 1024 ), INSTANCE, tokenNameLookup ), p ->
        {
            String failure = "The contrived failure";

            // WHEN
            p.markAsFailed( failure );
            p.close( false, NULL );

            // THEN
            assertEquals( FAILED, indexProvider.getInitialState( descriptor, NULL ) );
        }, false );
    }

    @Test
    public void shouldBeAbleToDropAClosedIndexPopulator()
    {
        // GIVEN
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.defaults() );
        final IndexPopulator p = indexProvider.getPopulator( descriptor, indexSamplingConfig, heapBufferFactory( 1024 ), INSTANCE, tokenNameLookup );
        p.close( false, NULL );

        // WHEN
        p.drop();

        // THEN - no exception should be thrown (it's been known to!)
    }
}
