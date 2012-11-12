package org.neo4j.bench.cases.memory;

import java.io.IOException;
import java.rmi.Remote;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

public class MemoryProfilingReport implements Remote
{

    /**
     * Tracks how many times an object was allocated from a given source.
     */
    public static class AllocationReport implements Comparable<AllocationReport>
    {

        private String stackTrace;
        private long allocations = 0;
        private long allocatedBytes = 0;

        public AllocationReport()
        {

        }

        public AllocationReport( String stackTrace )
        {

            this.stackTrace = stackTrace;
        }

        public String getStackTrace()
        {
            return stackTrace;
        }

        public void setStackTrace( String stackTrace )
        {
            this.stackTrace = stackTrace;
        }

        public void recordAllocation( int count, long size )
        {
            allocatedBytes += size;
            allocations += 1;
        }

        public long getAllocatedBytes()
        {
            return allocatedBytes;
        }

        public void setAllocatedBytes( long allocatedBytes )
        {
            this.allocatedBytes = allocatedBytes;
        }

        public long getAllocations()
        {
            return allocations;
        }

        public void setAllocations( long allocations )
        {
            this.allocations = allocations;
        }

        @Override
        public int compareTo( AllocationReport other )
        {
            if(getAllocatedBytes() == other.getAllocatedBytes())
            {
                if(getAllocations() == other.getAllocations())
                {
                    return other.getStackTrace().compareTo( getStackTrace() );
                }

                return ((Long)other.getAllocations()).compareTo( getAllocations() );
            } else {
                return ((Long)other.getAllocatedBytes()).compareTo( getAllocatedBytes() );
            }
        }
    }

    /**
     * Tracks the number, source and size of allocations of some given object.
     */
    public static class ObjectReport implements Comparable<ObjectReport>
    {

        private String objectName;
        private long totalObjectsAllocated = 0;
        private long totalBytes = 0;

        private Map<String, AllocationReport> allocationSources = new HashMap<String, AllocationReport>();

        public ObjectReport()
        {

        }

        public ObjectReport( @JsonProperty String objectName )
        {
            this.objectName = objectName;
        }

        public void setObjectName(String name)
        {
            this.objectName = name;
        }

        public String getObjectName()
        {
            return objectName;
        }

        public void recordAllocation( int count, Object object, long size, StackTraceElement[] allocationStackTrace )
        {
            totalObjectsAllocated += 1;
            totalBytes += size;

            String allocation = stackTraceToString( allocationStackTrace );
            if(!allocationSources.containsKey( allocation ))
            {
                allocationSources.put( allocation, new AllocationReport(allocation ) );
            }

            allocationSources.get( allocation ).recordAllocation(count, size);
        }

        public long getTotalBytes()
        {
            return totalBytes;
        }

        public void setTotalBytes( long totalBytes )
        {
            this.totalBytes = totalBytes;
        }

        public long getTotalObjectsAllocated()
        {
            return totalObjectsAllocated;
        }

        public void setTotalObjectsAllocated( long totalObjectsAllocated )
        {
            this.totalObjectsAllocated = totalObjectsAllocated;
        }

        public Map<String, AllocationReport> getAllocationSources()
        {
            return allocationSources;
        }

        public void setAllocationSources( Map<String, AllocationReport> allocationSources )
        {
            this.allocationSources = allocationSources;
        }

        private String stackTraceToString(StackTraceElement [] stackTrace)
        {
            int k=stackTrace.length;
            if (k==0)
                return null;
            StringBuilder out=new StringBuilder();
            out.append(stackTrace[0]);
            for (int x=1;x<k;++x)
                out.append("\n").append(stackTrace[x]);
            return out.toString();
        }

        @Override
        public int compareTo( ObjectReport other )
        {
            Long otherTotalBytes = other.getTotalBytes();

            if(totalBytes == otherTotalBytes)
            {
                Long otherObjectsAllocated = other.getTotalObjectsAllocated();
                if(totalObjectsAllocated == otherObjectsAllocated)
                {
                    return other.getObjectName().compareTo( getObjectName() );
                }

                return otherObjectsAllocated.compareTo( totalObjectsAllocated );
            }
            return otherTotalBytes.compareTo( totalBytes );
        }
    }


    private long totalBytes = 0;
    private long totalObjectsAllocated = 0;
    private Map<String, ObjectReport> objectReports = new HashMap<String, ObjectReport>();


    public synchronized void recordSample( int count, String objectName, Object object, long size )
    {
        totalBytes += size;
        totalObjectsAllocated += 1; // TODO: Count contains the size of arrays, parhaps take into account here?

        if(!objectReports.containsKey( objectName ))
        {
            objectReports.put( objectName, new ObjectReport(objectName) );
        }

        objectReports.get( objectName ).recordAllocation( count, object, size, getAllocationStackTrace() );
    }


    private StackTraceElement[] getAllocationStackTrace()
    {
        // Stack trace while recording an allocation will look something like:
        //        at org.neo4j.bench.cases.memory.MemoryProfilingReport.recordSample(MemoryProfilingReport.java:84)
        //        at org.neo4j.bench.cases.memory.MemoryProfiler$ProfilingMemorySampler.sampleAllocation(MemoryProfiler.java:31)
        //        at com.google.monitoring.runtime.instrumentation.AllocationRecorder.recordAllocation(AllocationRecorder.java:203)
        //        at org.neo4j.bench.Examples$MemoryProfilingWithMainMethod.invoke(Examples.java:164)
        //        at org.neo4j.bench.cases.memory.MemoryProfiler$MemoryProfilingRunner.profileOperation(MemoryProfiler.java:69)
        //        at org.neo4j.bench.cases.memory.MemoryProfiler.main(MemoryProfiler.java:92)
        // So we cut away all elements until we get to where the AllocationRecorder gets invoked.

        StackTraceElement[] stackTrace = new Throwable().getStackTrace();
        return Arrays.copyOfRange( stackTrace, 4, stackTrace.length);
    }

    public long getTotalBytesAllocated()
    {
        return totalBytes;
    }

    public void setTotalBytesAllocated(long bytesAllocated)
    {
        this.totalBytes = bytesAllocated;
    }

    public long getTotalObjectsAllocated()
    {
        return totalObjectsAllocated;
    }

    public void setTotalObjectsAllocated( long totalObjectsAllocated )
    {
        this.totalObjectsAllocated = totalObjectsAllocated;
    }

    public Map<String, ObjectReport> getObjectReports()
    {
        return objectReports;
    }

    public String serialize()
    {
        ObjectMapper mapper = new ObjectMapper();
        try
        {
            return mapper.writeValueAsString( this );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public static MemoryProfilingReport deserialize( String serialized )
    {
        ObjectMapper mapper = new ObjectMapper();
        try
        {
            return mapper.readValue( serialized, MemoryProfilingReport.class );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
