package org.neo4j.bench.cases.memory;

import java.util.Collection;
import java.util.TreeSet;

import org.codehaus.jackson.annotate.JsonProperty;
import org.neo4j.bench.domain.CaseResult;
import org.neo4j.bench.domain.Units;

/**
 * A case results that wraps a memory profiling result, adding some helpful info to the
 * printed report.
 */
public class MemoryCaseResult extends CaseResult
{
    private final MemoryProfilingReport memoryReport;

    public MemoryCaseResult( @JsonProperty("caseName") String caseName, MemoryProfilingReport memoryReport )
    {
        super( caseName,
                new CaseResult.Metric( "memory usage",      memoryReport.getTotalBytesAllocated(),   Units.BYTE ),
                new CaseResult.Metric( "objects allocated", memoryReport.getTotalObjectsAllocated(), Units.OBJECT ));
        this.memoryReport = memoryReport;
    }

    public String createReport(String prefix)
    {
        String baseReport = super.createReport( prefix );

        StringBuilder report = new StringBuilder( baseReport );

        report.append( prefix + "Object allocations:\n" );

        for ( MemoryProfilingReport.ObjectReport objectReport : sortedObjectReports() )
        {
            report.append( String.format( "%s  %12d bytes  %12d allocations: %s\n",
                    prefix, objectReport.getTotalBytes(),
                    objectReport.getTotalObjectsAllocated(), objectReport.getObjectName() ) );
        }

        System.out.println("Allocation sources:\n");
        for ( MemoryProfilingReport.ObjectReport objectReport : sortedObjectReports() )
        {
            report.append( prefix + "Allocations of '" + objectReport.getObjectName() + "'\n");
            for ( MemoryProfilingReport.AllocationReport allocationReport : sortedAllocationSources( objectReport ) )
            {
                report.append( prefix + "  " + allocationReport.getAllocations() + " allocations from:\n" );
                report.append( allocationReport.getStackTrace());
                report.append( "\n\n" );
            }

        }

        return report.toString();
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
