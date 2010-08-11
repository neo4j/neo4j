package org.neo4j.kernel.impl.ha;

public interface ResponseReceiver
{
    SlaveContext getSlaveContext();
    
    <T> T receive( Response<T> response );
}
