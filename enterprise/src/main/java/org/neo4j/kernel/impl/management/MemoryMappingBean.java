package org.neo4j.kernel.impl.management;

import java.util.Collection;
import java.util.Iterator;

import javax.management.NotCompliantMBeanException;

import org.neo4j.kernel.impl.nioneo.store.WindowPoolStats;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.management.MemoryMapping;
import org.neo4j.kernel.management.WindowPoolInfo;

@Description( "The status of Neo4j memory mapping" )
class MemoryMappingBean extends Neo4jMBean implements MemoryMapping
{
    static MemoryMappingBean create( final String instanceId, final NeoStoreXaDataSource datasource )
    {
        return createMX( new MXFactory<MemoryMappingBean>()
        {
            @Override
            MemoryMappingBean createMXBean()
            {
                return new MemoryMappingBean( instanceId, datasource, true );
            }

            @Override
            MemoryMappingBean createStandardMBean() throws NotCompliantMBeanException
            {
                return new MemoryMappingBean( instanceId, datasource );
            }
        } );
    }

    private final NeoStoreXaDataSource datasource;

    private MemoryMappingBean( String instanceId, NeoStoreXaDataSource datasource )
            throws NotCompliantMBeanException
    {
        super( instanceId, MemoryMapping.class );
        this.datasource = datasource;
    }

    private MemoryMappingBean( String instanceId, NeoStoreXaDataSource datasource, boolean isMXBean )
    {
        super( instanceId, MemoryMapping.class, isMXBean );
        this.datasource = datasource;
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
