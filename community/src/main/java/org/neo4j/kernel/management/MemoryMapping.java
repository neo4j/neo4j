package org.neo4j.kernel.management;

import java.util.Collection;
import java.util.Iterator;

import org.neo4j.kernel.impl.nioneo.store.WindowPoolStats;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;

class MemoryMapping extends Neo4jJmx implements MemoryMappingMBean
{
    static class AsMxBean extends Neo4jJmx implements MemoryMappingMXBean
    {
        private final NeoStoreXaDataSource datasource;

        AsMxBean( int instanceId, NeoStoreXaDataSource datasource )
        {
            super( instanceId );
            this.datasource = datasource;
        }

        public WindowPoolInfo[] getMemoryPools()
        {
            return getMemoryPoolsImpl( datasource );
        }
    }

    private final NeoStoreXaDataSource datasource;

    MemoryMapping( int instanceId, NeoStoreXaDataSource datasource )
    {
        super( instanceId );
        this.datasource = datasource;
    }

    public WindowPoolInfo[] getMemoryPools()
    {
        return getMemoryPoolsImpl( datasource );
    }

    public static WindowPoolInfo[] getMemoryPoolsImpl( NeoStoreXaDataSource datasource )
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
}
