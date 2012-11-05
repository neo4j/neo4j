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
package org.neo4j.bench.domain;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Result of a full run of one or more {@link org.neo4j.bench.cases.Benchmark}es.
 */
public class RunResult
{

    @JsonProperty private Date timestamp;
    @JsonProperty private String testedVersion;
    @JsonProperty private List<CaseResult> results = new ArrayList<CaseResult>(  );
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

    public List<CaseResult> getResults()
    {
        return results;
    }

    public String getBuildUrl()
    {
        return buildUrl != null ? buildUrl : "Unknown build url";
    }

    public void addResult(CaseResult result)
    {
        results.add( result );
    }

    public CaseResult getCase( String caseName )
    {
        for(CaseResult result : results)
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
        CaseResult caseResult = getCase(caseName);
        return caseResult != null && caseResult.containsMetric( metricName );
    }

    public CaseResult.Metric getMetric( String caseName, String metricName )
    {
        CaseResult caseResult = getCase(caseName);
        if(caseResult != null )
        {
            return caseResult.getMetric( metricName );
        } else {
            return null;
        }
    }
}
