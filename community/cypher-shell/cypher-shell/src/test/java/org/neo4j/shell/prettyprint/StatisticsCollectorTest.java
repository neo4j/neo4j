/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.shell.prettyprint;

import org.junit.Test;

import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.SummaryCounters;
import org.neo4j.shell.cli.Format;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatisticsCollectorTest
{

    @Test
    public void returnEmptyStringForPlainFormatting() throws Exception
    {
        // given
        ResultSummary result = mock( ResultSummary.class );

        // when
        String actual = new StatisticsCollector( Format.PLAIN ).collect( result );

        // then
        assertThat( actual, is( "" ) );
    }

    @Test
    public void returnStatisticsForDefaultFormatting() throws Exception
    {
        // given
        ResultSummary resultSummary = mock( ResultSummary.class );
        SummaryCounters summaryCounters = mock( SummaryCounters.class );

        when( resultSummary.counters() ).thenReturn( summaryCounters );
        when( summaryCounters.labelsAdded() ).thenReturn( 1 );
        when( summaryCounters.nodesCreated() ).thenReturn( 10 );

        // when
        String actual = new StatisticsCollector( Format.VERBOSE ).collect( resultSummary );

        // then
        assertThat( actual, is( "Added 10 nodes, Added 1 labels" ) );
    }
}
