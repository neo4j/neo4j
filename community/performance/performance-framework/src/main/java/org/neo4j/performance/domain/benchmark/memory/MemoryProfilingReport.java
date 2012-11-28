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
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.exc.UnrecognizedPropertyException;

public class MemoryProfilingReport
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

        private Map<Integer, AllocationReport> allocationSources = new HashMap<Integer, AllocationReport>();

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

            Integer allocation = stackTraceHash( allocationStackTrace );
            if(!allocationSources.containsKey( allocation ))
            {
                allocationSources.put( allocation, new AllocationReport( stackTraceToString( allocationStackTrace ) ) );
            }

            allocationSources.get( allocation ).recordAllocation(count, size);
        }

        private int stackTraceHash( StackTraceElement[] allocationStackTrace )
        {
            int hash = 0;
            for(StackTraceElement el : allocationStackTrace)
            {
                hash ^= el.hashCode();
            }
            return hash;
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

        public Map<Integer, AllocationReport> getAllocationSources()
        {
            return allocationSources;
        }

        public void setAllocationSources( Map<Integer, AllocationReport> allocationSources )
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

    public void serialize( PrintStream out )
    {
        ObjectMapper mapper = new ObjectMapper();
        try
        {
            mapper.writeValue( out, this );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public static MemoryProfilingReport deserialize( byte[] serialized )
    {
        ObjectMapper mapper = new ObjectMapper();
        try
        {
            try
            {
                return mapper.readValue( serialized, MemoryProfilingReport.class );
            } catch( UnrecognizedPropertyException e)
            {
                Map<String, Object> err = mapper.readValue( serialized, Map.class );

                throw new RuntimeException( "Memory profiling failed: " + err.get( "error" ));
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
