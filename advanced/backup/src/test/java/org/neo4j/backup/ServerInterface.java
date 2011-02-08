package org.neo4j.backup;

public interface ServerInterface
{
    void shutdown();
    
    void awaitStarted();
}
