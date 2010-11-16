package org.neo4j.server.rrd;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import java.lang.management.ManagementFactory;

public class MemoryUsedSampleable implements Sampleable
{
    private ObjectName memoryName;
    private MBeanServer mbeanServer;

    public MemoryUsedSampleable() throws MalformedObjectNameException
    {
        memoryName = new ObjectName( "java.lang:type=Memory" );
        mbeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    public String getName()
    {
        return "memory_usage_percent";
    }

    public long getValue()
    {
        try
        {
            long used = (Long)( (CompositeDataSupport)mbeanServer.getAttribute( memoryName, "HeapMemoryUsage" ) ).get( "used" );
            long max = 100 * (Long)( (CompositeDataSupport)mbeanServer.getAttribute( memoryName, "HeapMemoryUsage" ) ).get( "max" );
            return used / max;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
