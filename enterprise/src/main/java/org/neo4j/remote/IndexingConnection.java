package org.neo4j.remote;

public interface IndexingConnection
{
    RemoteResponse<IterableSpecification<NodeSpecification>> getIndexNodes(
        int transactionId, int serviceId, String key, Object value );

    RemoteResponse<Void> indexNode( int transactionId, int serviceId,
        long nodeId, String key, Object value );

    RemoteResponse<Void> removeIndexNode( int transactionId, int serviceId,
        long nodeId, String key, Object value );
}
