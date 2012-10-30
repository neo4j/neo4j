/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bench.cases;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.neo4j.bench.domain.CaseResult;

public class TestSimpleBenchmark
{

    @Test
    public void shouldReportRuntime() throws Exception
    {
        // Given
        BenchmarkCase bench = new SimpleBenchmark()
        {
            @Override
            public long runOperations()
            {
                return 1000;
            }
        };

        // When
        CaseResult result = bench.run();

        // Then
        assertThat( result, not(nullValue()));
        assertThat( result.getMetrics().size(), is(1) );
        assertThat( result.getMetrics().get(0).getValue(), not(nullValue()));
    }



}
