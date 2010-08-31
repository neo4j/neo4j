package org.neo4j.kernel.impl.ha;

public interface ResponseReceiver
{
    SlaveContext getSlaveContext( int eventIdentifier );
    
    <T> T receive( Response<T> response );
    
    void somethingIsWrong( Exception e );
}
