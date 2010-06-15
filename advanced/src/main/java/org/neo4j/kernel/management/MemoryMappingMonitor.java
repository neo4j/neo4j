package org.neo4j.kernel.management;

import java.util.Collection;
import java.util.Iterator;

import org.neo4j.kernel.impl.nioneo.store.WindowPoolStats;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;

abstract class MemoryMappingMonitor extends Neo4jJmx
{
    static class MemoryMapping extends MemoryMappingMonitor implements
            MemoryMappingMBean
    {
        MemoryMapping( int instanceId, NeoStoreXaDataSource datasource )
        {
            super( instanceId, datasource );
        }
    }

    static class MXBeanImplementation extends MemoryMappingMonitor implements
            MemoryMappingMXBean
    {
        MXBeanImplementation( int instanceId, NeoStoreXaDataSource datasource )
        {
            super( instanceId, datasource );
        }
    }

    private final NeoStoreXaDataSource datasource;

    private MemoryMappingMonitor( int instanceId,
            NeoStoreXaDataSource datasource )
    {
        super( instanceId );
        this.datasource = datasource;
    }

    public WindowPoolInfo[] getMemoryPools()
    {
        Collection<WindowPoolStats> stats = datasource.getWindowPoolStats();
        WindowPoolInfo[] pools = new WindowPoolInfo[stats.size()];
        Iterator<WindowPoolStats> iter = stats.iterator();
        for ( int index = 0; iter.hasNext(); index++ )
        {
            pools[index] = new WindowPoolInfo( iter.next() );
        }
        return pools;
    }

    public String[] getPoolNames()
    {
        Collection<WindowPoolStats> stats = datasource.getWindowPoolStats();
        String[] pools = new String[stats.size()];
        Iterator<WindowPoolStats> iter = stats.iterator();
        for ( int index = 0; iter.hasNext(); index++ )
        {
            pools[index] = iter.next().getName();
        }
        return pools;
    }
}
