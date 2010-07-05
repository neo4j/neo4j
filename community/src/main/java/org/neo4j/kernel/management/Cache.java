package org.neo4j.kernel.management;

public interface Cache
{
    final String NAME = "Cache";

    String getCacheType();

    int getNodeCacheSize();

    int getRelationshipCacheSize();

    void clear();
}
