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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;

import org.junit.Test;

import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class CountingIndexUpdaterTest
{
    @Test
    public void shouldCloseUsingProvidedTransactionId() throws IOException, IndexEntryConflictException
    {
        // given
        IndexUpdater indexUpdater = mock( IndexUpdater.class );
        IndexCountVisitor indexCountVisitor = mock( IndexCountVisitor.class );
        CountingIndexUpdater countingIndexUpdater = new CountingIndexUpdater( 42l, indexUpdater, indexCountVisitor );

        // when
        countingIndexUpdater.close();

        // then
        verify( indexCountVisitor ).incrementIndexCount( eq( 42l ), eq( 0l ) );
        verify( indexUpdater ).close();

        verifyNoMoreInteractions( indexCountVisitor );
        verifyNoMoreInteractions( indexUpdater );
    }

    @Test
    public void shouldIncrementDeltaForAdds() throws IOException, IndexEntryConflictException
    {
        // given
        IndexUpdater indexUpdater = mock( IndexUpdater.class );
        IndexCountVisitor indexCountVisitor = mock( IndexCountVisitor.class );
        CountingIndexUpdater countingIndexUpdater = new CountingIndexUpdater( 42l, indexUpdater, indexCountVisitor );

        // when
        NodePropertyUpdate update = NodePropertyUpdate.add( 1, 2, "x", new long[]{3} );
        countingIndexUpdater.process( update );
        countingIndexUpdater.close();

        // then
        verify( indexUpdater ).process( update );
        verify( indexCountVisitor ).incrementIndexCount( eq( 42l ), eq( 1l ) );
        verify( indexUpdater ).close();

        verifyNoMoreInteractions( indexCountVisitor );
        verifyNoMoreInteractions( indexUpdater );
    }

    @Test
    public void shouldIncrementDeltaForRemoves() throws IOException, IndexEntryConflictException
    {
        // given
        IndexUpdater indexUpdater = mock( IndexUpdater.class );
        IndexCountVisitor indexCountVisitor = mock( IndexCountVisitor.class );
        CountingIndexUpdater countingIndexUpdater = new CountingIndexUpdater( 42l, indexUpdater, indexCountVisitor );

        // when
        NodePropertyUpdate update = NodePropertyUpdate.remove( 1, 2, "x", new long[]{3} );
        countingIndexUpdater.process( update );
        countingIndexUpdater.close();

        // then
        verify( indexUpdater ).process( update );
        verify( indexCountVisitor ).incrementIndexCount( eq( 42l ), eq( -1l ) );
        verify( indexUpdater ).close();

        verifyNoMoreInteractions( indexCountVisitor );
        verifyNoMoreInteractions( indexUpdater );
    }

    @Test
    public void shouldIncrementDeltaForMultipleAddsAndDeletes() throws IOException, IndexEntryConflictException
    {
        // given
        IndexUpdater indexUpdater = mock( IndexUpdater.class );
        IndexCountVisitor indexCountVisitor = mock( IndexCountVisitor.class );
        CountingIndexUpdater countingIndexUpdater = new CountingIndexUpdater( 42l, indexUpdater, indexCountVisitor );

        // when
        NodePropertyUpdate update1 = NodePropertyUpdate.add( 1, 2, "x", new long[]{3} );
        NodePropertyUpdate update2 = NodePropertyUpdate.add( 4, 2, "x", new long[]{3} );
        NodePropertyUpdate update3 = NodePropertyUpdate.remove( 4, 2, "x", new long[]{3} );
        countingIndexUpdater.process( update1 );
        countingIndexUpdater.process( update2 );
        countingIndexUpdater.process( update3 );
        countingIndexUpdater.close();

        // then
        verify( indexUpdater ).process( update1 );
        verify( indexUpdater ).process( update2 );
        verify( indexUpdater ).process( update3 );
        verify( indexCountVisitor ).incrementIndexCount( eq( 42l ), eq( 1l ) );
        verify( indexUpdater ).close();

        verifyNoMoreInteractions( indexCountVisitor );
        verifyNoMoreInteractions( indexUpdater );
    }
}
