package org.neo4j.com;

public interface RequestType<M>
{
    MasterCaller getMasterCaller();
    
    ObjectSerializer getObjectSerializer();
    
    byte id();
}
