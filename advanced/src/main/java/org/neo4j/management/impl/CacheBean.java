package org.neo4j.management.impl;

import java.security.AccessControlException;

import javax.management.MBeanOperationInfo;
import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension.KernelData;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.management.Cache;

@Service.Implementation( ManagementBeanProvider.class )
public class CacheBean extends ManagementBeanProvider
{
    public CacheBean()
    {
        super( Cache.class );
    }

    @Override
    protected Neo4jMBean createMBean( KernelData kernel ) throws NotCompliantMBeanException
    {
        return new CacheManager( this, kernel );
    }

    @Description( "Information about the caching in Neo4j" )
    private class CacheManager extends Neo4jMBean implements Cache
    {
        CacheManager( ManagementBeanProvider provider, KernelData kernel )
                throws NotCompliantMBeanException
        {
            super( provider, kernel );
            this.nodeManager = kernel.getConfig().getGraphDbModule().getNodeManager();
        }

        private final NodeManager nodeManager;

        @Description( "The type of cache used by Neo4j" )
        public String getCacheType()
        {
            return nodeManager.getCacheType().getDescription();
        }

        @Description( "The number of Nodes currently in cache" )
        public int getNodeCacheSize()
        {
            return nodeManager.getNodeCacheSize();
        }

        @Description( "The number of Relationships currently in cache" )
        public int getRelationshipCacheSize()
        {
            return nodeManager.getRelationshipCacheSize();
        }

        @Description( value = "Clears the Neo4j caches", impact = MBeanOperationInfo.ACTION )
        public void clear()
        {
            if ( true )
                throw new AccessControlException( "Clearing cache through JMX not permitted." );
            nodeManager.clearCache();
        }
    }
}
