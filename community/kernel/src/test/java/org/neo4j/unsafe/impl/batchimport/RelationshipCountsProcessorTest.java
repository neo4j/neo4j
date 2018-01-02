/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Test;
import org.mockito.ArgumentMatcher;

import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.unsafe.impl.batchimport.cache.NodeLabelsCache;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.longThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RelationshipCountsProcessorTest
{

    private final static int ANY = -1;
    private NodeLabelsCache nodeLabelCache = mock( NodeLabelsCache.class );
    private CountsAccessor.Updater countsUpdater = mock( CountsAccessor.Updater.class );

    @Test
    public void shouldHandleBigNumberOfLabelsAndRelationshipTypes() throws Exception
    {
        /*
         * This test ensures that the RelationshipCountsProcessor does not attempt to allocate a negative amount
         * of memory when trying to get an array to store the relationship counts. This could happen when the labels
         * and relationship types were enough in number to overflow an integer used to hold a product of those values.
         * Here we ask the Processor to do that calculation and ensure that the number passed to the NumberArrayFactory
         * is positive.
         */
        // Given
        /*
         * A large but not impossibly large number of labels and relationship types. These values are the simplest
         * i could find in a reasonable amount of time that would result in an overflow. Given that the calculation
         * involves squaring the labelCount, 22 bits are more than enough for an integer to overflow. However, the
         * actual issue involves adding a product of relTypeCount and some other things, which makes hard to predict
         * which values will make it go negative. These worked. Given that with these values the integer overflows
         * some times over, it certainly works with much smaller numbers, but they don't come out of a nice simple bit
         * shifting.
         */
        int relTypeCount = 1 << 8;
        int labelCount = 1 << 22;
        NumberArrayFactory numberArrayFactory = mock( NumberArrayFactory.class );

        // When
        new RelationshipCountsProcessor( nodeLabelCache, labelCount, relTypeCount, countsUpdater, numberArrayFactory );

        // Then
        verify( numberArrayFactory, times( 2 ) ).newLongArray( longThat( new IsNonNegativeLong() ), anyLong() );
    }

    @Test
    public void testRelationshipCountersUpdates()
    {
        int relationTypes = 2;
        int labels = 3;

        NodeLabelsCache.Client client = mock( NodeLabelsCache.Client.class );
        when( nodeLabelCache.newClient() ).thenReturn( client );
        when( nodeLabelCache.get( eq( client ), eq( 1l ), any( int[].class ) ) ).thenReturn( new int[]{0, 2} );
        when( nodeLabelCache.get( eq( client ), eq( 2l ), any( int[].class ) ) ).thenReturn( new int[]{1} );
        when( nodeLabelCache.get( eq( client ), eq( 3l ), any( int[].class ) ) ).thenReturn( new int[]{1, 2} );
        when( nodeLabelCache.get( eq( client ), eq( 4l ), any( int[].class ) ) ).thenReturn( new int[]{} );

        RelationshipCountsProcessor countsProcessor = new RelationshipCountsProcessor( nodeLabelCache, labels,
                relationTypes, countsUpdater, NumberArrayFactory.AUTO );

        countsProcessor.process( 1, 0, 3 );
        countsProcessor.process( 2, 1, 4 );


        countsProcessor.done();

        // wildcard counts
        verify( countsUpdater ).incrementRelationshipCount( ANY, ANY, ANY, 2l );
        verify( countsUpdater ).incrementRelationshipCount( ANY, 0, ANY, 1l );
        verify( countsUpdater ).incrementRelationshipCount( ANY, 1, ANY, 1l );

        // start labels counts
        verify( countsUpdater ).incrementRelationshipCount( 0, 0, ANY, 1l );
        verify( countsUpdater ).incrementRelationshipCount( 2, 0, ANY, 1l );

        // end labels counts
        verify( countsUpdater ).incrementRelationshipCount( ANY, 0, 1, 1l );
        verify( countsUpdater ).incrementRelationshipCount( ANY, 0, 2, 1l );
    }

    private class IsNonNegativeLong extends ArgumentMatcher<Long>
    {
        public boolean matches( Object argument )
        {
            return argument != null && ((Long) argument) >= 0;
        }
    }
}
