package org.neo4j.kernel.manage;

public interface CacheMXBean
{
    String NAME = "Cache";

    String getCacheType();

    int getNodeCacheSize();

    int getRelationshipCacheSize();
}
