package org.neo4j.kernel.manage;

public interface CacheMBean
{
    final String NAME = "Cache";

    String getCacheType();

    int getNodeCacheSize();

    int getRelationshipCacheSize();
}
