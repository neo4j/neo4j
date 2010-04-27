package org.neo4j.kernel.manage;

import org.neo4j.kernel.impl.core.NodeManager;

class Cache extends Neo4jJmx implements CacheMBean
{
    private final NodeManager nodeManager;

    Cache( int instanceId, NodeManager nodeManager )
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
