package org.neo4j.kernel.impl.ha;

public interface ResponseReceiver
{
    <T> T receive( Response<T> response );
}
