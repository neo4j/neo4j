/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.statistics;

import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.statistics.RollingAverage;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.neo4j.kernel.impl.api.statistics.HeuristicsTestSupport.generateStore;

public class StatisticsCollectorTest
{

    @Test
    public void shouldGatherLabelDistribution() throws Throwable
    {
        // Given
        double equalityTolerance = 0.2d;
        StatisticsCollectedData data = new StatisticsCollectedData( new RollingAverage.Parameters( RollingAverage
                .Parameters.DEFAULT_WINDOW_SIZE, equalityTolerance ) );
        StatisticsCollector collector = new StatisticsCollector( generateStore(), data );

        // When
        collector.run();
        collector.run();

        // Then
        assertThat( collector.collectedData().labelDistribution( 0 ), closeTo( 0.2, equalityTolerance ) );
        assertThat( collector.collectedData().labelDistribution( 1 ), closeTo( 0.8, equalityTolerance ) );
    }


    @Test
    public void shouldGatherRelationshipTypeAndDirectionDistribution() throws Exception
    {
        // Given
        double equalityTolerance = 0.2d;
        StatisticsCollectedData data = new StatisticsCollectedData( new RollingAverage.Parameters( RollingAverage
                .Parameters.DEFAULT_WINDOW_SIZE, equalityTolerance ) );
        StatisticsCollector collector = new StatisticsCollector( generateStore(), data );

        // When
        collector.run();
        collector.run();

        // Then
        assertThat( collector.collectedData().relationshipTypeDistribution( 0 ), closeTo( 0.4, equalityTolerance ) );
        assertThat( collector.collectedData().relationshipTypeDistribution( 1 ), closeTo( 0.5, equalityTolerance ) );
    }

    @Test
    public void shouldGatherRelationshipDegreeByLabelDistribution() throws Exception
    {
        // Given
        StatisticsCollectedData data = new StatisticsCollectedData();
        StatisticsCollector collector = new StatisticsCollector( generateStore(), data );

        // When
        collector.run();
        collector.run();

        // Then
        assertThat( collector.collectedData().degree( 1, 0, Direction.INCOMING ), closeTo( 44.0, 10.0 ) );
        assertThat( collector.collectedData().degree( 1, 0, Direction.OUTGOING ), closeTo( 4.4, 1.0 ) );
    }

    @Test
    public void shouldGatherLiveNodes() throws Throwable
    {
        // Given
        StatisticsCollectedData data = new StatisticsCollectedData();
        StatisticsCollector collector = new StatisticsCollector( generateStore( 0.6 ), data );

        // When
        collector.run();
        collector.run();
        collector.run();
        collector.run();
        collector.run();
        collector.run();

        // Then
        assertThat( collector.collectedData().liveNodesRatio(), closeTo( 0.6, 0.1 ) );
    }

    @Test
    public void shouldGatherMaxNodes() throws Throwable
    {
        // Given
        StatisticsCollectedData data = new StatisticsCollectedData();
        StatisticsCollector collector = new StatisticsCollector( generateStore(), data );

        // When
        collector.run();
        collector.run();

        // Then
        assertThat( collector.collectedData().maxAddressableNodes(), equalTo( 1000L ) );
    }
}
