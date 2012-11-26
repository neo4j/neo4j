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
package org.neo4j.performance.domain.benchmark.memory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.TreeSet;

import org.codehaus.jackson.annotate.JsonProperty;
import org.neo4j.performance.domain.Units;
import org.neo4j.performance.domain.benchmark.BenchmarkResult;

/**
 * A case results that wraps a memory profiling result, adding some helpful info to the
 * printed report.
 */
public class MemoryBenchmarkResult extends BenchmarkResult
{
    private final MemoryProfilingReport memoryReport;

    public MemoryBenchmarkResult( @JsonProperty("caseName") String caseName, MemoryProfilingReport memoryReport )
    {
        super( caseName,
                new BenchmarkResult.Metric( "memory usage",      memoryReport.getTotalBytesAllocated(),   Units.BYTE ),
                new BenchmarkResult.Metric( "objects allocated", memoryReport.getTotalObjectsAllocated(), Units.OBJECT ));
        this.memoryReport = memoryReport;
    }

    public void createReport( String prefix, OutputStreamWriter out ) throws IOException
    {
        super.createReport( prefix, out );


        out.append( prefix + "Object allocations:\n" );

        for ( MemoryProfilingReport.ObjectReport objectReport : sortedObjectReports() )
        {
            out.append( String.format( "%s  %12d bytes  %12d allocations: %s\n",
                    prefix, objectReport.getTotalBytes(),
                    objectReport.getTotalObjectsAllocated(), objectReport.getObjectName() ) );
        }

        out.append("Allocation sources:\n");
        for ( MemoryProfilingReport.ObjectReport objectReport : sortedObjectReports() )
        {
            out.append( prefix + "Allocations of '" + objectReport.getObjectName() + "'\n");
            for ( MemoryProfilingReport.AllocationReport allocationReport : sortedAllocationSources( objectReport ) )
            {
                out.append( prefix + "  " + allocationReport.getAllocations() + " allocations from:\n" );
                out.append( allocationReport.getStackTrace());
                out.append( "\n\n" );
            }

        }

        out.flush();
    }

    private Collection<MemoryProfilingReport.AllocationReport> sortedAllocationSources(
            MemoryProfilingReport.ObjectReport objectReport )
    {
        return new TreeSet<MemoryProfilingReport.AllocationReport>(objectReport.getAllocationSources().values());
    }

    private Collection<MemoryProfilingReport.ObjectReport> sortedObjectReports()
    {
        return new TreeSet<MemoryProfilingReport.ObjectReport>( memoryReport.getObjectReports().values() );
    }
}
