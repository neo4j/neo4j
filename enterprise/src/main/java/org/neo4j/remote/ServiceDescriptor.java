package org.neo4j.remote;

public interface ServiceDescriptor<T>
{
    String getIdentifier();

    T getService();
}
