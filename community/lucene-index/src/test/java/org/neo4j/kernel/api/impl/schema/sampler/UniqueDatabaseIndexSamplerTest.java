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
package org.neo4j.kernel.api.impl.schema.sampler;

import org.apache.lucene.search.IndexSearcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.storageengine.api.schema.IndexSample;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UniqueDatabaseIndexSamplerTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final IndexSearcher indexSearcher = mock( IndexSearcher.class, Mockito.RETURNS_DEEP_STUBS );
    private final TaskCoordinator taskControl = new TaskCoordinator( 0, TimeUnit.MILLISECONDS );

    @Test
    public void uniqueSamplingUseDocumentsNumber() throws IndexNotFoundKernelException
    {
        when( indexSearcher.getIndexReader().numDocs() ).thenReturn( 17 );

        UniqueLuceneIndexSampler sampler = new UniqueLuceneIndexSampler( indexSearcher, taskControl.newInstance() );
        IndexSample sample = sampler.sampleIndex();
        assertEquals( 17, sample.indexSize() );
    }

    @Test
    public void uniqueSamplingCancel() throws IndexNotFoundKernelException
    {
        when( indexSearcher.getIndexReader().numDocs() ).thenAnswer( invocation ->
        {
            taskControl.cancel();
            return 17;
        } );

        expectedException.expect( IndexNotFoundKernelException.class );
        expectedException.expectMessage( "Index dropped while sampling." );

        UniqueLuceneIndexSampler sampler = new UniqueLuceneIndexSampler( indexSearcher, taskControl.newInstance() );
        sampler.sampleIndex();
    }

}
