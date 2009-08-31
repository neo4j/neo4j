package org.neo4j.impl.nioneo.store;


public interface IdGenerator
{
    long nextId();
    void setHighId( long id );
    long getHighId();
    void freeId( long id );
    void close();
    String getFileName();
    long getNumberOfIdsInUse();
}