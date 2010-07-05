package org.neo4j.kernel.management;

public interface StoreFile
{
    final String NAME = "Store file sizes";

    long getLogicalLogSize();

    long getTotalStoreSize();

    long getNodeStoreSize();

    long getRelationshipStoreSize();

    long getPropertyStoreSize();

    long getStringStoreSize();

    long getArrayStoreSize();
}
