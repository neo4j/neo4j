package org.neo4j.kernel.impl.management;

import java.security.AccessControlException;

import javax.management.MBeanOperationInfo;
import javax.management.NotCompliantMBeanException;

import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.management.Cache;

@Description( "Information about the caching in Neo4j" )
class CacheBean extends Neo4jMBean implements Cache
{
    private final NodeManager nodeManager;

    CacheBean( String instanceId, NodeManager nodeManager ) throws NotCompliantMBeanException
    {
        super( instanceId, Cache.class );
        this.nodeManager = nodeManager;
    }

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
