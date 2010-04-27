package org.neo4j.kernel.impl.manage;

import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.manage.CacheMXBean;

public class CacheMonitor extends Neo4jMonitor implements CacheMXBean
{
    private final NodeManager nodeManager;

    public CacheMonitor( int instanceId, NodeManager nodeManager )
    {
        super( instanceId );
        this.nodeManager = nodeManager;
    }

    @Override
    public String getCacheType()
    {
        return nodeManager.isUsingSoftReferenceCache() ? "soft reference cache"
                : "lru cache";
    }

    @Override
    public int getNodeCacheSize()
    {
        return nodeManager.getNodeCacheSize();
    }

    @Override
    public int getRelationshipCacheSize()
    {
        return nodeManager.getRelationshipCacheSize();
    }
}
