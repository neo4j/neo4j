/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.longThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.unsafe.impl.batchimport.cache.NodeLabelsCache;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;

public class RelationshipCountsProcessorTest
{
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
        new RelationshipCountsProcessor( mock( NodeLabelsCache.class ), labelCount, relTypeCount,
                mock( CountsAccessor.Updater.class ), numberArrayFactory );

        // Then
        verify( numberArrayFactory, times( 1 ) ).newLongArray( longThat( new IsNonNegativeLong() ), anyLong() );
    }

    private class IsNonNegativeLong extends ArgumentMatcher<Long>
    {
        public boolean matches( Object theLong )
        {
            return theLong != null && ((Long) theLong) >= 0;
        }
    }
}
