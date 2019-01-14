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

import java.util.Iterator;

import org.neo4j.test.Race;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.unsafe.impl.batchimport.DataStatistics.Client;
import org.neo4j.unsafe.impl.batchimport.DataStatistics.RelationshipTypeCount;

import static org.junit.Assert.assertEquals;

public class DataStatisticsTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldSumCounts() throws Throwable
    {
        // given
        DataStatistics stats = new DataStatistics( 1, 2, new RelationshipTypeCount[0] );
        Race race = new Race();
        int types = 10;
        long[] expected = new long[types];
        int threads = Runtime.getRuntime().availableProcessors();
        for ( int i = 0; i < threads; i++ )
        {
            long[] local = new long[types];
            for ( int j = 0; j < types; j++ )
            {
                local[j] = random.nextInt( 1_000, 2_000 );
                expected[j] += local[j];
            }
            race.addContestant( () ->
            {
                try ( DataStatistics.Client client = stats.newClient() )
                {
                    for ( int typeId = 0; typeId < types; typeId++ )
                    {
                        while ( local[typeId]-- > 0 )
                        {
                            client.increment( typeId );
                        }
                    }
                }
            } );
        }

        // when
        race.go();

        // then
        stats.forEach( count -> assertEquals( expected[count.getTypeId()], count.getCount() ) );
    }

    @Test
    public void shouldGrowArrayProperly()
    {
        // given
        DataStatistics stats = new DataStatistics( 1, 1, new RelationshipTypeCount[0] );

        // when
        int typeId = 1_000;
        try ( Client client = stats.newClient() )
        {
            client.increment( typeId );
        }

        // then
        RelationshipTypeCount count = typeCount( stats.iterator(), typeId );
        assertEquals( 1, count.getCount() );
        assertEquals( typeId, count.getTypeId() );
    }

    private RelationshipTypeCount typeCount( Iterator<RelationshipTypeCount> iterator, int typeId )
    {
        while ( iterator.hasNext() )
        {
            RelationshipTypeCount count = iterator.next();
            if ( count.getTypeId() == typeId )
            {
                return count;
            }
        }
        throw new IllegalStateException( "Couldn't find " + typeId );
    }
}
