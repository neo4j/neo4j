/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.performance.cases;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

import java.util.Collection;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.performance.domain.benchmark.Benchmark;
import org.neo4j.performance.domain.benchmark.BenchmarkLoader;
import org.neo4j.performance.domain.benchmark.SimpleBenchmark;

public class TestBenchmarkCaseLoader
{

    public static class FindMeBenchCase extends SimpleBenchmark
    {
        @Override
        public void runOperation()
        {

        }
    }

    @Test
    @Ignore
    public void shouldFindBenchCasesInPackage() throws Exception
    {
        // Given
        BenchmarkLoader loader = new BenchmarkLoader();

        // When
        Collection<Benchmark> cases = loader.loadBenchmarksIn( getClass().getPackage().getName() );

        // Then
        assertThat( cases.size(), is( 1 ));
        assertThat( cases.iterator().next(), is( FindMeBenchCase.class ) );
    }



}
