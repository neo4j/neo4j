package org.neo4j.bolt.v4.messaging;

import org.neo4j.bolt.runtime.BoltResult;

public interface ResultConsumer
{
    void consume( BoltResult t ) throws Exception;
    boolean hasMore();
}
