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
import java.util.Arrays;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;
import org.neo4j.bench.domain.filter.RunResultFilter;

/**
 * A collection of {@link RunResult}s.
 */
public class RunResultSet
{

    @JsonProperty private List<RunResult> results;

    public RunResultSet()
    {
        this.results = new ArrayList<RunResult>(  );
    }

    public RunResultSet(RunResult ... results )
    {
        this( Arrays.asList(results) );
    }

    public RunResultSet(List<RunResult> results )
    {
        this.results = results;
    }

    public void add( RunResult runResult )
    {
        results.add( runResult );
    }

    public RunResultSet filter( RunResultFilter filter )
    {
        List<RunResult> filtered = new ArrayList<RunResult>(  );
        for(RunResult result : results)
        {
            if(filter.accept( result ))
            {
                filtered.add( result );
            }
        }

        return new RunResultSet(filtered);
    }

    public int size()
    {
        return results.size();
    }

    public Pair<CaseResult.Metric, RunResult> getHighestValueOf( String caseName, String metricName)
    {
        CaseResult.Metric highest = null;
        RunResult bestRun = null;

        for(RunResult result : results)
        {
            CaseResult.Metric current = result.getMetric(caseName, metricName);
            if(current != null && (highest == null || current.compareTo( highest) < 0))
            {
                highest = current;
                bestRun = result;
            }
        }

        return Pair.of( highest, bestRun );
    }
}
