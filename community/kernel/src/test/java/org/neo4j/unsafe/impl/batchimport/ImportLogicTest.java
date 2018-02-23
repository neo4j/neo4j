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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Resource;

import org.neo4j.graphdb.Direction;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.unsafe.impl.batchimport.DataStatistics.RelationshipTypeCount;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( RandomExtension.class )
public class ImportLogicTest
{
    @Resource
    public RandomRule random;

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
}
