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
package org.neo4j.performance.domain;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;
import org.neo4j.performance.domain.benchmark.BenchmarkResult;

/**
 * Result of a full run of one or more {@link org.neo4j.performance.domain.benchmark.Benchmark}es.
 */
public class RunResult
{

    @JsonProperty private Date timestamp;
    @JsonProperty private String testedVersion;
    @JsonProperty private List<BenchmarkResult> results = new ArrayList<BenchmarkResult>(  );
    @JsonProperty private String buildUrl;

    private RunResult(){}

    public RunResult( String testedVersion, Date timestamp, String buildUrl )
    {
        this.testedVersion = testedVersion;
        this.timestamp = timestamp;
        this.buildUrl = buildUrl;
    }

    public Date getTimestamp()
    {
        return timestamp;
    }

    public String getTestedVersion()
    {
        return testedVersion;
    }

    public List<BenchmarkResult> getResults()
    {
        return results;
    }

    public String getBuildUrl()
    {
        return buildUrl != null ? buildUrl : "Unknown build url";
    }

    public void addResult(BenchmarkResult result)
    {
        results.add( result );
    }

    public BenchmarkResult getCase( String caseName )
    {
        for(BenchmarkResult result : results)
        {
            if(result.getCaseName().equals( caseName ))
            {
                return result;
            }
        }

        return null;
    }

    public boolean containsMetric( String caseName, String metricName )
    {
        BenchmarkResult benchmarkResult = getCase(caseName);
        return benchmarkResult != null && benchmarkResult.containsMetric( metricName );
    }

    public BenchmarkResult.Metric getMetric( String caseName, String metricName )
    {
        BenchmarkResult benchmarkResult = getCase(caseName);
        if( benchmarkResult != null )
        {
            return benchmarkResult.getMetric( metricName );
        } else {
            return null;
        }
    }
}
