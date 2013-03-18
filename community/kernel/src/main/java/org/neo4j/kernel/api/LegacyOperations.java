package org.neo4j.kernel.api;

public interface LegacyOperations
{
    boolean hasLegacyNodeIndex(String indexName);
    boolean hasLegacyRelationshipIndex(String indexName);
}
