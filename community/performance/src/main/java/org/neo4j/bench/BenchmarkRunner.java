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
package org.neo4j.bench;

import java.util.Date;

import org.neo4j.bench.cases.Benchmark;
import org.neo4j.bench.cases.BenchmarkLoader;
import org.neo4j.bench.domain.CaseResult;
import org.neo4j.bench.domain.RunResult;

public class BenchmarkRunner
{

    private final BenchmarkLoader caseLoader;
    private final String testedVersion;
    private final String buildURL;

    /**
     * Convienience method for running a single benchmark.
     * @param benchmark
     */
    public static void runAndDumpReport(Benchmark benchmark)
    {
        CaseResult result = new BenchmarkRunner().runCase( benchmark );
        System.out.println(result.createReport( "" ));
    }

    public BenchmarkRunner()
    {
        this("Unknown version", "N/A");
    }

    public BenchmarkRunner(String testedVersion, String buildURL)
    {
        this.testedVersion = testedVersion;
        this.buildURL = buildURL;
        caseLoader = new BenchmarkLoader();
    }

    public CaseResult runCase(Benchmark benchmark)
    {
        benchmark.setUp();
        try
        {
            return benchmark.run();
        } finally {
            benchmark.tearDown();
        }
    }

    public RunResult runAllInPackage(String rootPackage)
    {
        RunResult result = new RunResult( testedVersion, new Date( ), buildURL );
        for ( Benchmark benchmarkCase : caseLoader.loadBenchmarksIn( rootPackage ) )
        {
            result.addResult( runCase( benchmarkCase ) );
        }

        return result;
    }

}
