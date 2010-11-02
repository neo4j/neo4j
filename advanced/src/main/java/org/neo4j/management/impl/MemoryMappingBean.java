package org.neo4j.management.impl;

import java.util.Collection;
import java.util.Iterator;

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension.KernelData;
import org.neo4j.kernel.impl.nioneo.store.WindowPoolStats;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.management.MemoryMapping;
import org.neo4j.management.WindowPoolInfo;

@Service.Implementation( ManagementBeanProvider.class )
public final class MemoryMappingBean extends ManagementBeanProvider
{
    public MemoryMappingBean()
    {
        super( MemoryMapping.class );
    }

    @Override
    protected Neo4jMBean createMBean( KernelData kernel ) throws NotCompliantMBeanException
    {
        return new MemoryMappingImpl( this, kernel );
    }

    @Override
    protected Neo4jMBean createMXBean( KernelData kernel ) throws NotCompliantMBeanException
    {
        return new MemoryMappingImpl( this, kernel, true );
    }

    @Description( "The status of Neo4j memory mapping" )
    private static class MemoryMappingImpl extends Neo4jMBean implements MemoryMapping
    {
        private final NeoStoreXaDataSource datasource;

        MemoryMappingImpl( ManagementBeanProvider provider, KernelData kernel )
                throws NotCompliantMBeanException
        {
            super( provider, kernel );
            this.datasource = KernelBean.getNeoDataSource( kernel );
        }

        MemoryMappingImpl( ManagementBeanProvider provider, KernelData kernel, boolean isMxBean )
        {
            super( provider, kernel, isMxBean );
            XaDataSourceManager mgr = kernel.getConfig().getTxModule().getXaDataSourceManager();
            this.datasource = (NeoStoreXaDataSource) mgr.getXaDataSource( "nioneodb" );
        }

        @Description( "Get information about each pool of memory mapped regions from store files with "
                      + "memory mapping enabled" )
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
                pools[index] = createWindowPoolInfo( iter.next() );
            }
            return pools;
        }

        private static WindowPoolInfo createWindowPoolInfo( WindowPoolStats stats )
        {
            return new WindowPoolInfo( stats.getName(), stats.getMemAvail(), stats.getMemUsed(),
                    stats.getWindowCount(), stats.getWindowSize(), stats.getHitCount(),
                    stats.getMissCount(), stats.getOomCount() );
        }
    }
}
