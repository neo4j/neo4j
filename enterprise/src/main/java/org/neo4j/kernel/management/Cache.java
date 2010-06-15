package org.neo4j.kernel.management;

import org.neo4j.kernel.impl.core.NodeManager;

class Cache extends Neo4jJmx implements CacheMBean
{
    private final NodeManager nodeManager;

    Cache( int instanceId, NodeManager nodeManager )
    {
        super( instanceId );
        this.nodeManager = nodeManager;
    }

    public String getCacheType()
    {
        return nodeManager.isUsingSoftReferenceCache() ? "soft reference cache"
                : "lru cache";
    }

    public int getNodeCacheSize()
    {
        return nodeManager.getNodeCacheSize();
    }

    public int getRelationshipCacheSize()
    {
        return nodeManager.getRelationshipCacheSize();
    }
}
