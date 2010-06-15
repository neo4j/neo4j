package org.neo4j.kernel.management;

public interface CacheMBean
{
    final String NAME = "Cache";

    String getCacheType();

    int getNodeCacheSize();

    int getRelationshipCacheSize();
}
